"""Hazard aggregation task dispatch."""
import cProfile
import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

import numpy as np
import numpy.typing as npt
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model

from toshi_hazard_post.calculators import rate_to_prob
from toshi_hazard_post.data_functions import (
    get_imts,
    get_levels,
    get_site_vs30,
    load_realization_values,
    load_realization_values_deagg,
)
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.locations import get_locations

# from toshi_hazard_store.branch_combinator.branch_combinator import (
#     get_weighted_branches,
#     grouped_ltbs,
#     merge_ltbs_fromLT,
# )
from toshi_hazard_post.logic_tree.branch_combinator import get_logic_tree
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree
from toshi_hazard_post.util.file_utils import save_realizations

from .aggregate_rlzs import build_branches, calculate_aggs, get_branch_weights
from .aggregation_config import AggregationConfig

INV_TIME = 1.0

log = logging.getLogger(__name__)
pr = cProfile.Profile()


AggTaskArgs = namedtuple(
    "AggTaskArgs",
    """hazard_model_id grid_loc locs logic_tree aggs imts levels vs30 deagg poe deagg_imtl save_rlz
    stride skip_save""",
)


@dataclass
class DistributedAggregationTaskArguments:
    """Class for passing arguments to Distributed Tasks."""

    hazard_model_id: str
    logic_trees_id: str
    locations: List[CodedLocation]
    levels: List[float]
    vs30s: List[int]
    aggs: List[str]
    imts: List[str]
    stride: int


class AggregationWorkerMP(multiprocessing.Process):
    """A worker that batches aggregation processing."""

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
                process_location_list(nt)
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)
                self.result_queue.put(str(nt.grid_loc))
            except Exception as e:
                log.error(f'unknown exception occured: {e}')
                self.task_queue.task_done()
                self.result_queue.put(f'FAILED {str(nt.gtid)}')


def process_location_list(task_args: AggTaskArgs) -> None:
    """For each imt and location, get the weighed aggregate statiscits of the hazard curve (or flattened disagg matrix)
    realizations. This is done over STRIDE elements (default 100) of the hazard curve at a time to reduce phyisical
    memory usage which allows for multiple calculations at once when the hazard curve is long (e.g. large
    disaggregations).

    REFACTOR.
    """

    locs = task_args.locs
    logic_tree = task_args.logic_tree
    aggs = task_args.aggs
    imts = task_args.imts
    levels = task_args.levels
    vs30 = task_args.vs30
    deagg_dimensions = task_args.deagg
    save_rlz = task_args.save_rlz
    stride = task_args.stride if task_args.stride else 100
    skip_save = task_args.skip_save
    toshi_ids = logic_tree.hazard_ids

    if deagg_dimensions:
        poe = task_args.poe
        imtl = task_args.deagg_imtl

    if deagg_dimensions:
        log.info('performing deaggregation')
    log.info('get values for %s locations and %s hazard_solutions' % (len(locs), len(toshi_ids)))
    log.debug('locs: %s' % (locs))
    log.debug('aggs: %s' % (aggs))
    log.debug('imts: %s' % (imts))
    log.debug('toshi_ids[:3]: %s' % (toshi_ids[:3]))

    tic_fn = time.perf_counter()
    if deagg_dimensions:
        values, bins = load_realization_values_deagg(toshi_ids, locs, [vs30], deagg_dimensions[0])
    else:
        values = load_realization_values(toshi_ids, locs, [vs30])

    if not values:
        log.info('missing values: %s' % (values))
        return

    weights = get_branch_weights(logic_tree)
    for imt in imts:
        log.info('working on imt: %s' % imt)

        tic_imt = time.perf_counter()
        for loc in locs:
            log.info(f'working on loc {loc}')
            lat, lon = loc.split('~')
            resolution = 0.001
            location = CodedLocation(float(lat), float(lon), resolution)

            site_vs30 = get_site_vs30(toshi_ids, loc) if vs30 == 0 else 0

            # ncols = get_len_rate(values)
            ncols = values.len_rate
            hazard = np.empty((ncols, len(aggs)))
            for start_ind in range(0, ncols, stride):
                end_ind = start_ind + stride
                if end_ind > ncols:
                    end_ind = ncols

                tic = time.perf_counter()
                branch_probs = build_branches(logic_tree, values, imt, loc, start_ind, end_ind)
                hazard[start_ind:end_ind, :] = calculate_aggs(branch_probs, aggs, weights)
                log.info(f'time to calculate hazard for one stride {time.perf_counter() - tic} seconds')

                if save_rlz:
                    save_realizations(imt, loc, vs30, branch_probs, weights, logic_tree)

            if task_args.skip_save:
                continue

            if not skip_save:
                if deagg_dimensions:
                    # save_deaggs(
                    #     hazard, bins, loc, imt, imtl, poe, vs30, task_args.hazard_model_id, deagg_dimensions
                    # )  # TODO: need more information about deagg to save (e.g. poe, inv_time)
                    save_disaggregation(
                        aggs[0], task_args.hazard_model_id, location, imt, vs30, poe, imtl, rate_to_prob(hazard, INV_TIME), bins, deagg_dimensions[1]
                    )
                else:
                    save_aggregation(
                        aggs,
                        levels,
                        rate_to_prob(hazard, INV_TIME),
                        imt,
                        vs30,
                        site_vs30,
                        task_args.hazard_model_id,
                        location,
                    )

        toc_imt = time.perf_counter()
        log.info('imt: %s took %.3f secs' % (imt, (toc_imt - tic_imt)))

    toc_fn = time.perf_counter()
    log.info('process_location_list took %.3f secs' % (toc_fn - tic_fn))


def save_disaggregation(
    agg: str,
    hazard_model_id: str,
    location: CodedLocation,
    imt: str,
    vs30: int,
    poe: float,  # fraction in 50 years
    imtl: float,
    deagg_array: npt.NDArray,
    bins: Dict[str, Any],
    deagg_agg_target: str,
) -> None:
    """
    Only handles a single aggregate statistic, assumed to be mean for both the hazard curve and the disagg.
    Assumes probability is fraction in 50 years
    """

    shape = [len(v) for v in bins.values()]
    deagg_array = deagg_array.reshape(shape)

    bins_array = np.array(list(bins.values()), dtype=object)

    hazard_agg = model.AggregationEnum(deagg_agg_target)
    disagg_agg = model.AggregationEnum(agg)
    probability = model.ProbabilityEnum[f'_{int(poe*100)}_PCT_IN_50YRS']
    with model.DisaggAggregationExceedance.batch_write() as batch:
        dae = model.DisaggAggregationExceedance.new_model(
            hazard_model_id,
            location,
            vs30,
            imt,
            hazard_agg,
            disagg_agg,
            probability,
            imtl,
            deagg_array,
            bins_array,
        )
        batch.save(dae)


def save_aggregation(
    aggs: List[str],
    levels: Iterable[float],
    hazard: npt.NDArray,
    imt: str,
    vs30: int,
    site_vs30: float,
    hazard_model_id: str,
    location: str,
) -> None:
    """Save aggregated curves to THS.

    Parameters
    ----------
    aggs
        list of aggregation statistics
    levels
        hazard (shaking) levels
    hazard
        hazard curves (probabilities)
    imt
        intensity measure type
    vs30
        site condition
    site_vs30
        sites specific vs30 for models that don't use the same value for every location. 0 if not used.
    hazard_model_id
        THS ID
    location
        location code
    """

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
                hazard_model_id=hazard_model_id,
            ).set_location(location)
            if site_vs30:
                hag.site_vs30 = site_vs30
            batch.save(hag)


def process_aggregation_local_serial(
    hazard_model_id: str,
    logic_trees: Dict[int, HazardLogicTree],
    coded_locations: Iterable[CodedLocation],
    levels: Iterable[float],
    vs30s: Iterable[int],
    aggs: Iterable[str],
    imts: Iterable[str],
    stride: int,
    num_workers: int,
    save_rlz: bool = False,
    skip_save: bool = False,
) -> None:
    """Run task serially. This is only needed if running the debugger"""

    # toshi_ids = {int(k): v for k, v in toshi_ids.items()}
    # source_branches = {int(k): v for k, v in source_branches.items()}

    for coded_loc in coded_locations:
        for vs30 in vs30s:
            t = AggTaskArgs(
                hazard_model_id=hazard_model_id,
                grid_loc=coded_loc.downsample(0.1).code,
                locs=[coded_loc.downsample(0.001).code],
                logic_tree=logic_trees[vs30],
                aggs=aggs,
                imts=imts,
                levels=levels,
                vs30=vs30,
                deagg=False,
                poe=None,
                deagg_imtl=None,
                save_rlz=save_rlz,
                stride=stride,
                skip_save=skip_save,
            )

            # process_location_list(t, config.deagg_poes[0])
            process_location_list(t)


def process_aggregation_local(
    hazard_model_id: str,
    logic_trees: Dict[int, HazardLogicTree],
    coded_locations: Iterable[CodedLocation],
    levels: Iterable[float],
    vs30s: Iterable[int],
    aggs: Iterable[str],
    imts: Iterable[str],
    stride: int,
    num_workers: int,
    save_rlz: bool = False,
    skip_save: bool = False,
) -> List[str]:
    """Place aggregation jobs into a multiprocessing queue.

    Parameters
    ----------
    hazard_model_id : str
        id of toshi-hazard-store model to write to
    tohsi_ids : List[str]
        Toshi IDs of Openquake Hazard Solutions
    logic_trees
        dict of HazardLogicTree
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

    for coded_loc in coded_locations:
        for vs30 in vs30s:
            t = AggTaskArgs(
                hazard_model_id=hazard_model_id,
                grid_loc=coded_loc.downsample(0.1).code,
                locs=[coded_loc.downsample(0.001).code],
                logic_tree=logic_trees[vs30],
                aggs=aggs,
                imts=imts,
                levels=levels,
                vs30=vs30,
                deagg=False,
                poe=None,
                deagg_imtl=None,
                save_rlz=save_rlz,
                stride=stride,
                skip_save=skip_save,
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

    logic_trees = {}
    for vs30 in config.vs30s:
        logic_trees[vs30] = get_logic_tree(
            config.lt_config,
            config.hazard_gts,
            vs30,
            gmm_correlations=[],  # TODO: for now no gmm correlations, need a good method for specifying in the config
            truncate=config.source_branches_truncate,
        )
    log.info('finished building logic trees')

    locations = get_locations(config)
    resolution = 0.001
    coded_locations = [CodedLocation(*loc, resolution) for loc in locations]

    example_loc_code = coded_locations[0].downsample(0.001).code

    levels = get_levels(
        logic_trees[config.vs30s[0]], [example_loc_code], config.vs30s[0]
    )  # TODO: get seperate levels for every IMT
    avail_imts = get_imts(logic_trees[config.vs30s[0]], config.vs30s[0])  # TODO: equiv check for deaggs
    for imt in config.imts:
        assert imt in avail_imts

    if config.run_serial:
        process_aggregation_local_serial(
            config.hazard_model_id,
            logic_trees,
            coded_locations,
            levels,
            config.vs30s,
            config.aggs,
            config.imts,
            config.stride,
            NUM_WORKERS,
            config.save_rlz,
            config.skip_save,
        )  # TODO: use source_branches dict
    else:
        process_aggregation_local(
            config.hazard_model_id,
            logic_trees,
            coded_locations,
            levels,
            config.vs30s,
            config.aggs,
            config.imts,
            config.stride,
            NUM_WORKERS,
            config.save_rlz,
            config.skip_save,
        )  # TODO: use source_branches dict
