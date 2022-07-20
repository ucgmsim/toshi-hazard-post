"""Hazard aggregation task config and dispatch."""
# import json
import multiprocessing
import time

from toshi_hazard_store.aggregate_rlzs import concat_df_files, get_imts, get_levels
from toshi_hazard_store.aggregate_rlzs_mp import AggHardWorker, AggTaskArgs, build_source_branches
from toshi_hazard_store.branch_combinator.branch_combinator import merge_ltbs_fromLT
from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import data as gtdata
from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import logic_tree_permutations
from toshi_hazard_store.locations import locations_nzpt2_chunked

# from collections import namedtuple
# from dis import dis
# from operator import inv
# from pathlib import Path


def process_agg(vs30, location_generator, aggs, imts=None, output_prefix='', num_workers=12, location_range=None):
    """Configure the tasks."""
    # source_branches = load_source_branches()
    # omit = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDEy']  # this is the failed/cloned job in first GT_37
    omit = []
    toshi_ids = [b.hazard_solution_id for b in merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit)]
    source_branches = build_source_branches(logic_tree_permutations, gtdata, vs30, omit, truncate=5)

    binned_locs = location_generator(range=location_range)
    levels = get_levels(source_branches, list(binned_locs.values())[0], vs30)  # TODO: get seperate levels for every IMT

    if not imts:
        imts = get_imts(source_branches, vs30)

    ###########
    #
    # MULTIPROC
    #
    ###########
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print('Creating %d workers' % num_workers)
    workers = [AggHardWorker(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0
    for key, locs in location_generator(range=location_range).items():
        for loc in locs[:1]:
            t = AggTaskArgs(key, [loc], toshi_ids, source_branches, aggs, imts, levels, vs30)
            task_queue.put(t)
            num_jobs += 1
        break

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

    # hazard_curves = pd.concat([hazard_curves, binned_hazard_curves])
    # print(hazard_curves)


if __name__ == "__main__":

    nproc = 1

    classical = True

    vs30 = 400
    aggs = [
        'mean',
        0.005,
        0.01,
        0.025,
        0.05,
    ]  # 0.1, 0.2, 0.5, 0.8, 0.9, 0.95, 0.975, 0.99, 0.995]

    imts = ['PGA', 'SA(0.2)', 'SA(0.3)']  # , 'SA(0.5)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)']
    # imts = ['PGA', 'SA(0.5)', 'SA(1.5)', 'SA(3.0)']
    # imts = None

    # location_generator = locations_nzpt2_and_nz34_chunked
    # location_generator = locations_nz34_chunked
    location_generator = locations_nzpt2_chunked

    loc_keyrange = (0, 29)  # CDC
    # loc_keyrange = (30,45) # CBC (there are 43, but just in case I miss counted)
    output_prefix = f'CLOUD_LT_BY_LOC_{loc_keyrange[0]}_{loc_keyrange[1]}'

    hazard_curves, source_branches = process_agg(
        vs30,
        location_generator,
        aggs,
        imts,
        output_prefix=output_prefix,
        num_workers=nproc,
        location_range=loc_keyrange,
    )
