"""Hazard aggregation task dispatch."""
import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Iterator, List

import pandas as pd
from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model
from toshi_hazard_store.branch_combinator.branch_combinator import (
    get_weighted_branches,
    grouped_ltbs,
    merge_ltbs_fromLT,
)

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


def models_from_dataframe(
    location: CodedLocation, data: pd.DataFrame, args: AggTaskArgs
) -> Iterator[model.HazardAggregation]:
    """Generate for HazardAggregation models from dataframe."""
    for agg in args.aggs:
        values = []
        df_agg = data[data['agg'] == agg]
        for imt, val in enumerate(args.imts):
            values.append(
                model.IMTValuesAttribute(
                    imt=val,
                    lvls=df_agg.level.tolist(),
                    vals=df_agg.hazard.tolist(),
                )
            )
        # print(values[0])
        yield model.HazardAggregation(
            values=values,
            vs30=args.vs30,
            agg=agg,
            hazard_model_id=args.hazard_model_id,
        ).set_location(location)


def build_source_branches(logic_tree_permutations, gtdata, vs30, omit, truncate=None):
    """ported from THS. aggregate_rlzs_mp"""
    grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit))
    source_branches = get_weighted_branches(grouped)

    if truncate:
        # for testing only
        source_branches = source_branches[:truncate]

    for i in range(len(source_branches)):
        rlz_combs, weight_combs = build_rlz_table(source_branches[i], vs30)
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

    print(locs)
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

    columns = ['lat', 'lon', 'imt', 'agg', 'level', 'hazard']
    index = range(len(locs) * len(imts) * len(aggs) * len(levels))
    binned_hazard_curves = pd.DataFrame(columns=columns, index=index)

    nlocs = len(locs)
    naggs = len(aggs)
    nlevels = len(levels)

    cnt = 0
    start_imt = 0
    for imt in imts:

        log.info('process_location_list() working on imt: %s' % imt)
        tic_imt = time.perf_counter()
        start_loc = start_imt
        stop_imt = start_imt + nlocs * naggs * nlevels
        binned_hazard_curves.loc[start_imt:stop_imt, 'imt'] = imt
        start_imt = stop_imt

        for loc in locs:
            lat, lon = loc.split('~')
            start_agg = start_loc
            stop_loc = start_loc + naggs * nlevels
            binned_hazard_curves.loc[start_loc:stop_loc, 'lat'] = lat
            binned_hazard_curves.loc[start_loc:stop_loc, 'lon'] = lon
            start_loc = stop_loc

            log.debug('build_branches imt: %s, loc: %s, vs30: %s' % (imt, loc, vs30))

            # tic1 = time.perf_counter()

            # TODO: make these two functions more readable
            weights, branch_probs = build_branches(source_branches, values, imt, loc, vs30)
            hazard = calculate_aggs(branch_probs, aggs, weights)
            # toc1 = time.perf_counter()
            # print(f'time to calculate_aggs {toc1-tic1} seconds')

            for aggind, agg in enumerate(aggs):
                stop_agg = start_agg + nlevels
                binned_hazard_curves.loc[start_agg:stop_agg, 'agg'] = str(agg)
                start_agg = stop_agg

                for j, level in enumerate(levels):
                    binned_hazard_curves.loc[cnt, 'level':'hazard'] = pd.Series(
                        {'level': level, 'hazard': hazard[j, aggind]}
                    )
                    cnt += 1

        toc_imt = time.perf_counter()
        log.info('imt: %s took %.3f secs' % (imt, (toc_imt - tic_imt)))

    # TODO maybe this filtering isn't needed if we always have just one location, but do we? ....
    tic_db = time.perf_counter()
    for loc in locs:
        lat, lon = loc.split('~')
        log.debug(binned_hazard_curves)
        df_bhc = binned_hazard_curves
        loc_df = df_bhc[(df_bhc['lat'] == lat) & (df_bhc['lon'] == lon)]
        log.debug(loc_df)

        coded_loc = CodedLocation(float(lat), float(lon))
        log.debug(coded_loc)
        save_location_results(coded_loc, loc_df, task_args)

    toc_db = time.perf_counter()
    log.info('process_location_list save to db took %.3f secs' % (toc_db - tic_db))
    toc_fn = time.perf_counter()
    log.info('process_location_list took %.3f secs' % (toc_fn - tic_fn))
    return binned_hazard_curves


def save_location_results(coded_loc, binned_hazard_df, task_args):
    """Save the results."""
    with model.HazardAggregation.batch_write() as batch:
        for hag in models_from_dataframe(coded_loc, binned_hazard_df, task_args):
            batch.save(hag)


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
                toshi_ids,
                source_branches,
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
    toshi_ids = [
        b.hazard_solution_id
        for b in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=omit)
    ]
    source_branches = build_source_branches(
        config.logic_tree_permutations, config.hazard_solutions, config.vs30s[0], omit, truncate=5
    )

    locations = (
        load_grid(config.locations)
        if not config.location_limit
        else load_grid(config.locations)[: config.location_limit]
    )
    coded_locations = [CodedLocation(*loc) for loc in locations]

    example_loc_code = coded_locations[0].downsample(0.001).code
    levels = get_levels(source_branches, [example_loc_code], config.vs30s[0])  # TODO: get seperate levels for every IMT
    avail_imts = get_imts(source_branches, config.vs30s[0])
    for imt in config.imts:
        assert imt in avail_imts

    process_local(config.hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, NUM_WORKERS)
