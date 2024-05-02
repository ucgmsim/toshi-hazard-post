import itertools
import json
import logging
import multiprocessing
import time
import urllib.request
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Tuple, Union

from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.util import decompress_string
from nzshm_model.source_logic_tree.slt_config import from_config

from toshi_hazard_post.hazard_aggregation.aggregation import AggTaskArgs, process_location_list
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.locations import get_locations
from toshi_hazard_post.logic_tree.branch_combinator import get_logic_tree
from toshi_hazard_post.toshi_api_support import get_deagg_config, get_imtl, toshi_api

from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)

# TODO: remove if not needed
DeaggTaskArgs = namedtuple(
    "DeaggTaskArgs", "gtid lt_config source_branches_truncate hazard_model_id aggs deagg_dimensions stride skip_save"
)


@dataclass
class DeaggProcessArgs:
    """Class for passing arguments to distributed tasks"""

    lt_config_id: str
    lt_config: Union[str, Path]
    source_branches_truncate: int
    hazard_model_id: str
    aggs: List[str]
    deagg_dimensions: List[str]
    stride: int
    skip_save: bool
    hazard_gts: List[str]
    locations: List[Tuple[float, float]]
    deagg_agg_targets: List[str]
    poes: List[float]
    imts: List[str]
    vs30s: List[int]
    deagg_hazard_model_target: str
    inv_time: int
    num_workers: int


class DeAggregationWorkerMP(multiprocessing.Process):
    """A worker that batches and saves records to DynamoDB. ported from THS."""

    def __init__(self, task_queue: multiprocessing.JoinableQueue, result_queue: multiprocessing.Queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        log.info("worker %s running." % self.name)
        proc_name = self.name

        while True:
            nt = self.task_queue.get()
            if nt is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)
                break

            try:
                process_single_deagg(
                    nt.gtid,
                    nt.lt_config,
                    nt.source_branches_truncate,
                    nt.hazard_model_id,
                    nt.aggs,
                    nt.deagg_dimensions,
                    nt.stride,
                    nt.skip_save,
                )
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)
                self.result_queue.put(str(nt.gtid))
            except KeyError as e:
                log.warn(f'missing file in archive for GT ID {nt.gtid}')
                log.warn(e)
                self.task_queue.task_done()
                self.result_queue.put(f'FAILED {str(nt.gtid)}')
            except Exception as e:
                log.error(f'unknown exception occured: {e}')
                self.task_queue.task_done()
                self.result_queue.put(f'FAILED {str(nt.gtid)}')


@dataclass
class DeaggConfig:
    """class for specifying a deaggregation to lookup in GT index"""

    hazard_model_id: str
    location: str
    inv_time: int
    agg: str
    poe: float
    imt: str
    vs30: int


def get_index_from_s3() -> Dict[str, Any]:
    INDEX_URL = "https://nzshm22-static-reports.s3.ap-southeast-2.amazonaws.com/gt-index/gt-index.json"
    index_request = urllib.request.Request(INDEX_URL)
    index_str = urllib.request.urlopen(index_request)
    index_comp = index_str.read().decode("utf-8")
    return json.loads(decompress_string(index_comp))
    # index_str = urllib.request.urlopen(index_request)
    # return json.loads(index_str.read().decode("utf-8"))


def coded_location(loc: Tuple[float, float]) -> CodedLocation:
    return CodedLocation(*loc, 0.001).code


def requested_configs(
    locations: List[Tuple[float, float]],
    deagg_agg_targets: List[str],
    poes: List[float],
    imts: List[str],
    vs30s: List[int],
    deagg_hazard_model_target: str,
    inv_time: int,
    iter_method: str = '',
) -> Generator[DeaggConfig, None, None]:

    iterator: Iterable[Any] = []
    if not iter_method or iter_method.lower() == 'product':
        iterator = itertools.product(
            map(coded_location, locations),
            deagg_agg_targets,
            poes,
            imts,
            vs30s,
        )
    elif iter_method.lower() == 'zip':
        iterator = zip(
            map(coded_location, locations),
            deagg_agg_targets,
            poes,
            imts,
            vs30s,
        )
    else:
        raise ValueError('iter_method must be empty, "product", or "zip", %s given.' % iter_method)

    for location, agg, poe, imt, vs30 in iterator:
        yield DeaggConfig(
            hazard_model_id=deagg_hazard_model_target,
            location=location,
            inv_time=inv_time,
            agg=agg,
            poe=poe,
            imt=imt,
            vs30=vs30,
        )


def get_deagg_gtids(
    hazard_gts: List[str],
    lt_config: Path,
    locations: List[Tuple[float, float]],
    deagg_agg_targets: List[str],
    poes: List[float],
    imts: List[str],
    vs30s: List[int],
    deagg_hazard_model_target: str,
    inv_time: int,
    iter_method: str = '',
) -> List[str]:
    def extract_deagg_config(deagg_entry):
        deagg_task_config = json.loads(
            deagg_entry['arguments']['disagg_config'].replace("'", '"').replace('None', 'null')
        )

        return DeaggConfig(
            hazard_model_id=deagg_entry['arguments']['hazard_model_id'],
            location=deagg_task_config['location'],
            inv_time=deagg_task_config['inv_time'],
            agg=deagg_entry['arguments']['hazard_agg_target'],
            poe=deagg_task_config['poe'],
            imt=deagg_task_config['imt'],
            vs30=deagg_task_config['vs30'],
        )

    def num_success(gt):
        return gt['num_success']

    if hazard_gts:
        return hazard_gts
    else:
        gtids = []
        index = get_index_from_s3()
        slt = from_config(lt_config)
        nbranches = sum([len(fslt.branches) for fslt in slt.fault_system_lts])
        for deagg in requested_configs(
            locations,
            deagg_agg_targets,
            poes,
            imts,
            vs30s,
            deagg_hazard_model_target,
            inv_time,
            iter_method,
        ):
            gtids_tmp = []
            for gt_id, entry in index.items():
                if entry['subtask_type'] == 'OpenquakeHazardTask' and entry['hazard_subtask_type'] == 'DISAGG':
                    if deagg == extract_deagg_config(entry) and num_success(entry) == nbranches:
                        gtids_tmp.append(gt_id)
            if not gtids_tmp:
                raise Exception("no general task found for deagg {}".format(deagg))
            if len(gtids_tmp) > 1:
                raise Exception("more than one general task {} found for {}".format(gtids_tmp, deagg))
            gtids += gtids_tmp

    return gtids


def process_deaggregation(config: AggregationConfig) -> None:
    """Gather task information and launch local deaggregation processing.

    Parameters
    ----------
    config : AggregationConfig
        the config
    """

    locations = get_locations(config)

    args = DeaggProcessArgs(
        lt_config_id='',
        lt_config=config.lt_config,
        source_branches_truncate=config.source_branches_truncate,
        hazard_model_id=config.hazard_model_id,
        aggs=config.aggs,
        deagg_dimensions=config.deagg_dimensions,
        stride=config.stride,
        skip_save=config.skip_save,
        hazard_gts=config.hazard_gts,
        locations=locations,
        deagg_agg_targets=config.deagg_agg_targets,
        poes=config.poes,
        imts=config.imts,
        vs30s=config.vs30s,
        deagg_hazard_model_target=config.deagg_hazard_model_target,
        inv_time=config.inv_time,
        num_workers=NUM_WORKERS,
    )

    if config.run_serial:
        process_deaggregation_local_serial(args)
    else:
        process_deaggregation_local(args)


def process_deaggregation_local(args: DeaggProcessArgs, iter_method: str = '') -> List[str]:
    """Aggregate the Deaggregations in parallel."""

    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    num_workers = args.num_workers
    print('Creating %d workers' % num_workers)
    workers = [DeAggregationWorkerMP(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0

    gtids = get_deagg_gtids(
        args.hazard_gts,
        Path(args.lt_config),
        args.locations,
        args.deagg_agg_targets,
        args.poes,
        args.imts,
        args.vs30s,
        args.deagg_hazard_model_target,
        args.inv_time,
        iter_method,
    )

    for gtid in gtids:
        t = DeaggTaskArgs(
            gtid,
            args.lt_config,
            args.source_branches_truncate,
            args.hazard_model_id,
            args.aggs,
            args.deagg_dimensions,
            args.stride,
            args.skip_save,
        )

        task_queue.put(t)
        sleep_time = 10
        log.info(f'sleeping {sleep_time} seconds before queuing next task')
        time.sleep(sleep_time)

        num_jobs += 1

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    task_queue.join()

    results = []
    while num_jobs:
        result = result_queue.get()
        results.append(result)
        num_jobs -= 1

    toc = time.perf_counter()
    print(f'time to run deaggregations: {toc-tic:.0f} seconds')
    return results


def process_deaggregation_local_serial(args: DeaggProcessArgs) -> List[str]:
    """Aggregate the Deaggregations in serail. For debugging."""

    gtids = get_deagg_gtids(
        args.hazard_gts,
        Path(args.lt_config),
        args.locations,
        args.deagg_agg_targets,
        args.poes,
        args.imts,
        args.vs30s,
        args.deagg_hazard_model_target,
        args.inv_time,
    )
    results = []
    for gtid in gtids:
        process_single_deagg(
            gtid,
            args.lt_config,
            args.source_branches_truncate,
            args.hazard_model_id,
            args.aggs,
            args.deagg_dimensions,
            args.stride,
            args.skip_save,
        )
        results.append(gtid)

    return results


def process_single_deagg(
    gtid: str,
    lt_config: Union[str, Path],
    source_branches_truncate: int,
    hazard_model_id: str,
    aggs: List[str],
    deagg_dimensions: List[str],
    stride: int,
    skip_save: bool,
) -> None:

    log.info(
        f'gtid: {gtid}, lt_config: {lt_config}, source_branches_truncate: {source_branches_truncate}, '
        f'hazard_model_id {hazard_model_id}, aggs: {aggs}, deagg_dimensions: {deagg_dimensions}, '
        f'stride: {stride}, skip_save: {skip_save}'
    )

    # TODO: running 2 toshiAPI quieries on each GT ID, could we remove the redundancy?finishedfinished
    log.info('start get_disagg_gt()')
    gtdata = toshi_api.get_disagg_gt(gtid)
    log.info('finish get_disagg_gt()')
    imtl = get_imtl(gtdata)
    deagg_config = get_deagg_config(gtdata)

    location = deagg_config.location.split('~')
    loc = (float(location[0]), float(location[1]))
    resolution = 0.001
    coded_location = CodedLocation(*loc, resolution)

    log.info('start building logic tree ')
    # TODO: check that we get the correct logic tree when some tasks are missing
    tic = time.perf_counter()
    logic_tree = get_logic_tree(
        lt_config,
        [gtid],
        deagg_config.vs30,
        gmm_correlations=[],
        truncate=source_branches_truncate,
    )
    toc = time.perf_counter()
    log.info('finished building logic tree ')
    log.info(f'time to build logic tree {toc-tic} seconds')

    # TODO: need some "levels" for deaggs (deagg bins), this can come when we pull deagg data from THS
    levels: List[float] = []

    t = AggTaskArgs(
        hazard_model_id=hazard_model_id,
        grid_loc=coded_location.downsample(0.1).code,
        locs=[coded_location.downsample(0.001).code],
        logic_tree=logic_tree,
        aggs=aggs,  # TODO: I think this only works w/ one agg (len==1)
        imts=[deagg_config.imt],
        levels=levels,
        vs30=deagg_config.vs30,
        deagg=(deagg_dimensions, deagg_config.deagg_agg_target),
        poe=deagg_config.poe,
        deagg_imtl=imtl,
        save_rlz=False,
        stride=stride,
        skip_save=skip_save,
    )

    process_location_list(t)
