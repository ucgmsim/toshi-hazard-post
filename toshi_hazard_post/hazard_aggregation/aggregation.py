"""Hazard aggregation task dispatch."""
import logging
import multiprocessing
import time
from typing import List

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store.aggregate_rlzs import concat_df_files, get_imts, get_levels
from toshi_hazard_store.aggregate_rlzs_mp import AggHardWorker, AggTaskArgs, build_source_branches
from toshi_hazard_store.branch_combinator.branch_combinator import merge_ltbs_fromLT

from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)


def process_local(toshi_ids, source_branches, coded_locations, levels, config, output_prefix):
    """Run task locally using Multiprocessing."""
    num_workers = 1
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print('Creating %d workers' % num_workers)
    workers = [AggHardWorker(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0

    for coded_loc in coded_locations:
        for vs30 in config.vs30s:
            t = AggTaskArgs(
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

    # Start printing results
    print('Results:')
    df_file_names = []
    while num_jobs:
        result = result_queue.get()
        df_file_names.append(result)
        print(str(result))
        num_jobs -= 1

    toc = time.perf_counter()
    print(f'time to run aggregations: {toc-tic:.0f} seconds')

    file_name = '_'.join((output_prefix, 'all_aggregates.json'))
    hazard_curves = concat_df_files(df_file_names)
    hazard_curves.to_json(file_name)

    return hazard_curves, source_branches


def process_aggregation(config: AggregationConfig, output_prefix=''):
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

    process_local(toshi_ids, source_branches, coded_locations, levels, config, output_prefix)
