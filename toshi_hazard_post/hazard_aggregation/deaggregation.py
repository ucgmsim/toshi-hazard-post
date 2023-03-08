import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_post.hazard_aggregation.aggregation import AggTaskArgs, process_location_list
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.logic_tree.branch_combinator import get_logic_tree
from toshi_hazard_post.toshi_api_support import get_deagg_config, get_imtl, toshi_api

from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)

# TODO: remove if not needed
DeaggTaskArgs = namedtuple("DeaggTaskArgs", "gtid config")


@dataclass
class DistributedDeaggTaskArguments:
    """Class for passing arguments to distributed tasks"""

    gtid: str
    source_branches_truncate: int
    hazard_model_id: str
    aggs: List[str]
    deagg_dimensions: List[str]
    stride: int
    skip_save: bool
    lt_config_id: str


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

            process_single_deagg(
                nt.gtid,
                nt.config.lt_config,
                nt.config.source_branches_truncate,
                nt.config.hazard_model_id,
                nt.config.aggs,
                nt.config.deagg_dimensions,
                nt.config.stride,
                nt.config.skip_save,
            )
            self.task_queue.task_done()
            log.info('%s task done.' % self.name)
            self.result_queue.put(str(nt.gtid))


def process_deaggregation(config: AggregationConfig) -> List[str]:
    """Aggregate the Deaggregations in parallel."""

    # TODO: deagg should get the hazard_id from  the oq runs (not sure it's there atm) so that user can't overwrite
    # with the wrong id
    if config.run_serial:
        results = process_deaggregation_serial(config)
        return results

    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    num_workers = NUM_WORKERS
    print('Creating %d workers' % num_workers)
    workers = [DeAggregationWorkerMP(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0

    for gtid in config.hazard_gts:
        t = DeaggTaskArgs(gtid, config)

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


def process_deaggregation_serial(config: AggregationConfig) -> List[str]:
    """Aggregate the Deaggregations in serail. For debugging."""

    results = []
    for gtid in config.hazard_gts:
        process_single_deagg(
            gtid,
            config.lt_config,
            config.source_branches_truncate,
            config.hazard_model_id,
            config.aggs,
            config.deagg_dimensions,
            config.stride,
            config.skip_save,
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

    # TODO: running 2 toshiAPI quieries on each GT ID, could we remove the redundancy?
    gtdata = toshi_api.get_disagg_gt(gtid)
    imtl = get_imtl(gtdata)
    deagg_config = get_deagg_config(gtdata)

    location = deagg_config.location.split('~')
    loc = (float(location[0]), float(location[1]))
    resolution = 0.001
    coded_location = CodedLocation(*loc, resolution)

    # TODO: check that we get the correct logic tree when some tasks are missing
    logic_tree = get_logic_tree(
        lt_config,
        [gtid],
        deagg_config.vs30,
        gmm_correlations=[],
        truncate=source_branches_truncate,
    )
    log.info('finished building logic tree ')

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
        deagg=deagg_dimensions,
        poe=deagg_config.poe,
        deagg_imtl=imtl,
        save_rlz=False,
        stride=stride,
        skip_save=skip_save,
    )

    process_location_list(t)
