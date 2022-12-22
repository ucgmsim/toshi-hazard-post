"""Hazard aggregation task dispatch."""
import cProfile
import json
import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import List, Iterable, Dict, Union, Any

import numpy as np
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model

# from toshi_hazard_store.branch_combinator.branch_combinator import (
#     get_weighted_branches,
#     grouped_ltbs,
#     merge_ltbs_fromLT,
# )
from toshi_hazard_post.branch_combinator import build_source_branches, merge_ltbs_fromLT
from toshi_hazard_post.data_functions import (
    get_imts,
    get_levels,
    load_realization_values,
    load_realization_values_deagg,
)
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.locations import get_locations
from toshi_hazard_post.util.file_utils import save_deaggs

from .aggregate_rlzs import build_branches, calculate_aggs, get_branch_weights, get_len_rate
from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)
pr = cProfile.Profile()


AggTaskArgs = namedtuple(
    "AggTaskArgs",
    "hazard_model_id grid_loc locs toshi_ids source_branches aggs imts levels vs30 deagg poe deagg_imtl save_rlz",
)


@dataclass
class DistributedAggregationTaskArguments:
    """Class for passing arguments to Distributed Tasks."""

    hazard_model_id: str
    source_branches_id: str
    toshi_ids: List[str]
    locations: List[CodedLocation]
    aggs: List[str]
    imts: List[str]
    levels: List[float]
    vs30s: List[int]


class AggregationWorkerMP(multiprocessing.Process):
    """A worker that batches aggregation processing."""

    def __init__(
        self,
        task_queue: multiprocessing.JoinableQueue,
        result_queue: multiprocessing.Queue
    ):
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

            process_location_list(nt)
            self.task_queue.task_done()
            log.info('%s task done.' % self.name)
            self.result_queue.put(str(nt.grid_loc))


def process_location_list(task_args: AggTaskArgs) -> None:
    """For each imt and location, get the weighed aggregate statiscits of the hazard curve (or flattened disagg matrix)
    realizations. This is done over 100 elements of the hazard curve at a time to reduce phyisical memory usage
    which allows for multiple calculations at once when the hazard curve is long (e.g. large disaggregations).

    REFACTOR.
    """

    locs = task_args.locs
    toshi_ids = task_args.toshi_ids
    source_branches = task_args.source_branches
    aggs = task_args.aggs
    imts = task_args.imts
    levels = task_args.levels
    vs30 = task_args.vs30
    deagg_dimensions = task_args.deagg
    save_rlz = task_args.save_rlz

    if deagg_dimensions:
        poe = task_args.poe
        imtl = task_args.deagg_imtl

    # print(locs)
    if deagg_dimensions:
        log.info('performing deaggregation')
    log.info('get values for %s locations and %s hazard_solutions' % (len(locs), len(toshi_ids)))
    log.debug('locs: %s' % (locs))
    log.debug('aggs: %s' % (aggs))
    log.debug('imts: %s' % (imts))
    log.debug('toshi_ids[:3]: %s' % (toshi_ids[:3]))

    print('locs: %s' % (locs))
    print('aggs: %s' % (aggs))
    print('imts: %s' % (imts))
    print('toshi_ids[:3]: %s' % (toshi_ids[:3]))

    # log.debug('source_branches: %s' % (source_branches))

    tic_fn = time.perf_counter()
    if deagg_dimensions:
        values, bins = load_realization_values_deagg(toshi_ids, locs, [vs30], deagg_dimensions)
    else:
        values = load_realization_values(toshi_ids, locs, [vs30])

    if not values:
        log.info('missing values: %s' % (values))
        return
    
    weights = get_branch_weights(source_branches)
    for imt in imts:
        log.info('process_location_list() working on imt: %s' % imt)

        tic_imt = time.perf_counter()
        for loc in locs:
            log.info(f'process_location_list() working on loc {loc}')
            lat, lon = loc.split('~')
            resolution = 0.001
            location = CodedLocation(float(lat), float(lon), resolution)
            log.debug('build_branches imt: %s, loc: %s, vs30: %s' % (imt, loc, vs30))

            ncols = get_len_rate(values)
            hazard = np.empty((ncols, len(aggs)))
            stride = 100  # TODO: optimise stride length for avail. physical mem., number of threads, ...?
            for start_ind in range(0, ncols, stride):
                end_ind = start_ind + stride
                if end_ind > ncols:
                    end_ind = ncols

                tic = time.perf_counter()
                branch_probs = build_branches(source_branches, values, imt, loc, vs30, start_ind, end_ind)
                hazard[start_ind:end_ind, :] = calculate_aggs(branch_probs, aggs, weights)
                log.info(f'time to calculate hazard for one level {time.perf_counter() - tic} seconds')

                # TODO: replace with write to THS, this only works if the len(levels) < stride
                if save_rlz:
                    save_dir = '/work/chrisdc/NZSHM-WORKING/PROD/branch_rlz/SRWG/'
                    branches_filepath = save_dir + f'branches_{imt}-{loc}-{vs30}'
                    weights_filepath = save_dir + f'weights_{imt}-{loc}-{vs30}'
                    source_branches_filepath = save_dir + f'source_branches_{imt}-{loc}-{vs30}.json'
                    np.save(branches_filepath, branch_probs)
                    np.save(weights_filepath, weights)
                    with open(source_branches_filepath, 'w') as jsonfile:
                        json.dump(source_branches, jsonfile)

            if deagg_dimensions:
                save_deaggs(
                    hazard, bins, loc, imt, imtl, poe, vs30, task_args.hazard_model_id, deagg_dimensions
                )  # TODO: need more information about deagg to save (e.g. poe, inv_time)
            else:
                with model.HazardAggregation.batch_write() as batch:
                    for aggind, agg in enumerate(aggs):
                        hazard_vals = []
                        for j, level in enumerate(levels):
                            hazard_vals.append((level, hazard[j, aggind]))  # tuple lvl, val

                        if not hazard_vals:
                            log.debug('no hazard_vals for agg %s imt %s' % (agg, imt))
                            continue
                        else:
                            log.debug('hazard_vals :%s' % hazard_vals)

                        hag = model.HazardAggregation(
                            values=[model.LevelValuePairAttribute(lvl=lvl, val=val) for lvl, val in hazard_vals],
                            vs30=vs30,
                            imt=imt,
                            agg=agg,
                            hazard_model_id=task_args.hazard_model_id,
                        ).set_location(location)
                        batch.save(hag)

        toc_imt = time.perf_counter()
        log.info('imt: %s took %.3f secs' % (imt, (toc_imt - tic_imt)))

    toc_fn = time.perf_counter()
    log.info('process_location_list took %.3f secs' % (toc_fn - tic_fn))


def process_aggregation_local_serial(
    hazard_model_id: str,
    toshi_ids: Dict[Union[str, int], Any],
    source_branches: Dict[Union[str, int], Any],
    coded_locations: Iterable[CodedLocation],
    levels: Iterable[float],
    config: AggregationConfig,
    num_workers: int,
    save_rlz: bool = False,
) -> None:
    """Run task serially. This is only needed if running the debugger"""

    toshi_ids = {int(k): v for k, v in toshi_ids.items()}
    source_branches = {int(k): v for k, v in source_branches.items()}

    for coded_loc in coded_locations:
        for vs30 in config.vs30s:
            t = AggTaskArgs(
                hazard_model_id,
                coded_loc.downsample(0.1).code,
                [coded_loc.downsample(0.001).code],
                toshi_ids[vs30],
                source_branches[vs30],
                config.aggs,
                config.imts,
                levels,
                vs30,
                False,
                None,
                None,
                save_rlz,
            )

            # process_location_list(t, config.deagg_poes[0])
            process_location_list(t)


def process_aggregation_local(
    hazard_model_id: str,
    toshi_ids: Dict[Union[str, int], Any],
    source_branches: Dict[Union[str, int], Any],
    coded_locations: Iterable[CodedLocation],
    levels: Iterable[float],
    config: AggregationConfig,
    num_workers: int,
    save_rlz: bool = False,
) -> List[str]:
    """Place aggregation jobs into a multiprocessing queue.

    Parameters
    ----------
    hazard_model_id : str
        id of toshi-hazard-store model to write to
    tohsi_ids : List[str]
        Toshi IDs of Openquake Hazard Solutions
    source_branches : List[?]
        list of source model branches
    coded_locations : List[CodedLocation]
        locations at which to calculate hazard
    levels : List[float]
        shaking levels of hazard curve
    config : AggregationConfig
        the config
    num_workers : int
        number of multiprocessing tasks to run simultaneously
    save_rlz : bool
        flag, save realizations to disk

    Returns
    -------
    results : List[str]
        locations processed
    """
    # num_workers = 1
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print('Creating %d workers' % num_workers)
    workers = [AggregationWorkerMP(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0

    toshi_ids = {int(k): v for k, v in toshi_ids.items()}
    source_branches = {int(k): v for k, v in source_branches.items()}

    for coded_loc in coded_locations:
        for vs30 in config.vs30s:
            t = AggTaskArgs(
                hazard_model_id,
                coded_loc.downsample(0.1).code,
                [coded_loc.downsample(0.001).code],
                toshi_ids[vs30],
                source_branches[vs30],
                config.aggs,
                config.imts,
                levels,
                vs30,
                False,
                None,
                None,
                save_rlz,
            )

            task_queue.put(t)
            # log.info('sleeping 10 seconds before queuing next task')
            # time.sleep(10)
            num_jobs += 1

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    task_queue.join()

    results: List[str] = []
    while num_jobs:
        result = result_queue.get()
        results.append(result)
        num_jobs -= 1

    toc = time.perf_counter()
    print(f'time to run aggregations: {toc-tic:.0f} seconds')
    return results


def process_aggregation(config: AggregationConfig) -> None:
    """Gather task information and launch local aggregation processing.

    Parameters
    ----------
    config : AggregationConfig
        the config
    """
    serial = True

    omit: List[str] = []

    gtdata = config.hazard_solutions

    toshi_ids = {}
    for vs30 in config.vs30s:
        toshi_ids[vs30] = [
            b.hazard_solution_id
            for b in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=gtdata, omit=omit)
            if b.vs30 == vs30
        ]

    source_branches = {}
    for vs30 in config.vs30s:
        source_branches[vs30] = build_source_branches(
            config.logic_tree_permutations,
            gtdata,
            config.src_correlations,
            config.gmm_correlations,
            vs30,
            omit,
            toshi_ids[vs30],
            truncate=config.source_branches_truncate,
        )
    log.info('finished building logic tree ')

    locations = get_locations(config)
    resolution = 0.001
    coded_locations = [CodedLocation(*loc, resolution) for loc in locations]

    example_loc_code = coded_locations[0].downsample(0.001).code

    levels = get_levels(
        source_branches[config.vs30s[0]], [example_loc_code], config.vs30s[0]
    )  # TODO: get seperate levels for every IMT
    avail_imts = get_imts(source_branches[config.vs30s[0]], config.vs30s[0])  # TODO: equiv check for deaggs
    for imt in config.imts:
        assert imt in avail_imts

    if not serial:
        process_aggregation_local(
            config.hazard_model_id,
            toshi_ids,
            source_branches,
            coded_locations,
            levels,
            config,
            NUM_WORKERS,
            save_rlz=config.save_rlz,
        )  # TODO: use source_branches dict
    else:
        process_aggregation_local_serial(
            config.hazard_model_id,
            toshi_ids,
            source_branches,
            coded_locations,
            levels,
            config,
            NUM_WORKERS,
            save_rlz=config.save_rlz,
        )  # TODO: use source_branches dict
