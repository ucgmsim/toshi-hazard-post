"""Hazard aggregation task dispatch."""
import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import List

from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model

# from toshi_hazard_store.branch_combinator.branch_combinator import (
#     get_weighted_branches,
#     grouped_ltbs,
#     merge_ltbs_fromLT,
# )
from toshi_hazard_post.branch_combinator import get_weighted_branches, grouped_ltbs, merge_ltbs_fromLT
from toshi_hazard_post.hazard_aggregation.locations import get_locations
from toshi_hazard_post.local_config import NUM_WORKERS

from .aggregate_rlzs import (
    build_branches,
    build_rlz_table,
    calculate_aggs,
    get_imts,
    get_levels,
    load_realization_values,
)
from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)

AggTaskArgs = namedtuple("AggTaskArgs", "hazard_model_id grid_loc locs toshi_ids source_branches aggs imts levels vs30")


@dataclass
class DistributedAggregationTaskArguments:
    """Class for pass arguments to Distributed Tasks."""

    hazard_model_id: str
    source_branches_id: str
    toshi_ids: List[str]
    locations: List[CodedLocation]
    aggs: List[str]
    imts: List[str]
    levels: List[float]
    vs30s: List[int]


def build_source_branches(
    logic_tree_permutations, gtdata, src_correlations, gmm_correlations, vs30, omit, truncate=None
):
    """ported from THS. aggregate_rlzs_mp"""
    grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), vs30)

    source_branches = get_weighted_branches(grouped, src_correlations)

    if truncate:
        # for testing only
        source_branches = source_branches[:truncate]

    for i in range(len(source_branches)):
        rlz_combs, weight_combs = build_rlz_table(
            source_branches[i], vs30, gmm_correlations
        )  # TODO: add correlations to GMCM LT
        source_branches[i]['rlz_combs'] = rlz_combs
        source_branches[i]['weight_combs'] = weight_combs

    return source_branches


def process_location_list(task_args):
    """The math happens inside here... REFACTOR ME. ported from THS."""
    locs = task_args.locs
    toshi_ids = task_args.toshi_ids
    source_branches = task_args.source_branches
    aggs = task_args.aggs
    imts = task_args.imts
    levels = task_args.levels
    vs30 = task_args.vs30

    # print(locs)
    log.info('get values for %s locations and %s hazard_solutions' % (len(locs), len(toshi_ids)))
    log.debug('aggs: %s' % (aggs))
    log.debug('imts: %s' % (imts))
    log.debug('toshi_ids[:3]: %s' % (toshi_ids[:3]))

    # log.debug('source_branches: %s' % (source_branches))

    tic_fn = time.perf_counter()
    values = load_realization_values(toshi_ids, locs, [vs30])

    if not values:
        log.info('missing values: %s' % (values))
        return

    for imt in imts:
        log.info('process_location_list() working on imt: %s' % imt)

        tic_imt = time.perf_counter()
        for loc in locs:
            lat, lon = loc.split('~')
            resolution = 0.001
            location = CodedLocation(float(lat), float(lon), resolution)
            log.debug('build_branches imt: %s, loc: %s, vs30: %s' % (imt, loc, vs30))

            # tic1 = time.perf_counter()
            # TODO: make these two functions more readable
            weights, branch_probs = build_branches(source_branches, values, imt, loc, vs30)
            hazard = calculate_aggs(branch_probs, aggs, weights)
            # toc1 = time.perf_counter()
            # print(f'time to calculate_aggs {toc1-tic1} seconds')

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


class AggregationWorkerMP(multiprocessing.Process):
    """A worker that batches and saves records to DynamoDB. ported from THS."""

    def __init__(self, task_queue, result_queue):
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


def process_local(hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, num_workers):
    """Run task locally using Multiprocessing. ported from THS."""
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
            )

            task_queue.put(t)
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
    print(f'time to run aggregations: {toc-tic:.0f} seconds')
    return results


def process_aggregation(config: AggregationConfig):
    """Configure the tasks."""
    omit: List[str] = []

    toshi_ids = {}
    for vs30 in config.vs30s:
        toshi_ids[vs30] = [
            b.hazard_solution_id
            for b in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=omit)
            if b.vs30 == vs30
        ]

    source_branches = {}
    for vs30 in config.vs30s:
        source_branches[vs30] = build_source_branches(
            config.logic_tree_permutations,
            config.hazard_solutions,
            config.src_correlations,
            config.gmm_correlations,
            vs30,
            omit,
            truncate=config.source_branches_truncate,
        )

    locations = get_locations(config)

    resolution = 0.001
    coded_locations = [CodedLocation(*loc, resolution) for loc in locations]

    example_loc_code = coded_locations[0].downsample(0.001).code

    levels = get_levels(
        source_branches[config.vs30s[0]], [example_loc_code], config.vs30s[0]
    )  # TODO: get seperate levels for every IMT
    avail_imts = get_imts(source_branches[config.vs30s[0]], config.vs30s[0])
    for imt in config.imts:
        assert imt in avail_imts

    process_local(
        config.hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, NUM_WORKERS
    )  # TODO: use source_branches dict
