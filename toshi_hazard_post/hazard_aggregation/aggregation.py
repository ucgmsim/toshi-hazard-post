"""Hazard aggregation task dispatch."""
import cProfile
import logging
import multiprocessing
import time
from collections import namedtuple
from dataclasses import dataclass
from tokenize import group
from typing import List
import json
import numpy as np
from pathlib import Path

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
from toshi_hazard_post.util.file_utils import save_deaggs

from .aggregate_rlzs import (
    build_branches,
    build_rlz_table,
    calculate_aggs,
    get_imts,
    get_levels,
    load_realization_values,
    load_realization_values_deagg,
    preload_meta,
)
from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)
pr = cProfile.Profile()


AggTaskArgs = namedtuple(
    "AggTaskArgs", "hazard_model_id grid_loc locs toshi_ids source_branches aggs imts levels vs30 deagg poe"
)
DeaggTaskArgs = namedtuple("DeaggTaskArgs", "gtdatafile, logic_tree_permutations src_correlations gmm_correlations source_branches_truncate agg hazard_model_id")


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
    logic_tree_permutations, gtdata, src_correlations, gmm_correlations, vs30, omit, toshi_ids, truncate=None
):
    """ported from THS. aggregate_rlzs_mp"""
    # pr.enable()

    grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), vs30)

    source_branches = get_weighted_branches(grouped, src_correlations)

    if truncate:
        # for testing only
        source_branches = source_branches[:truncate]

    metadata = preload_meta(toshi_ids, vs30)

    for i in range(len(source_branches)):
        rlz_combs, weight_combs, rlz_sets = build_rlz_table(
            source_branches[i], metadata, gmm_correlations
        )  # TODO: add correlations to GMCM LT
        source_branches[i]['rlz_combs'] = rlz_combs
        source_branches[i]['weight_combs'] = weight_combs
        source_branches[i]['rlz_sets'] = rlz_sets

    # pr.disable()
    # pr.print_stats(sort='time')

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
    deagg = task_args.deagg

    if deagg:
        poe = task_args.poe

    # print(locs)
    if deagg:
        log.info('performing deaggregation')
    log.info('get values for %s locations and %s hazard_solutions' % (len(locs), len(toshi_ids)))
    log.debug('aggs: %s' % (aggs))
    log.debug('imts: %s' % (imts))
    log.debug('toshi_ids[:3]: %s' % (toshi_ids[:3]))

    # log.debug('source_branches: %s' % (source_branches))

    tic_fn = time.perf_counter()
    if deagg:
        values = load_realization_values_deagg(toshi_ids, locs, [vs30])
    else:
        values = load_realization_values(toshi_ids, locs, [vs30])

    if not values:
        log.info('missing values: %s' % (values))
        return

    for imt in imts:
        log.info('process_location_list() working on imt: %s' % imt)

        tic_imt = time.perf_counter()
        for loc in locs:
            log.info(f'process_location_list() working on loc {loc}')
            lat, lon = loc.split('~')
            resolution = 0.001
            location = CodedLocation(float(lat), float(lon), resolution)
            log.debug('build_branches imt: %s, loc: %s, vs30: %s' % (imt, loc, vs30))

            # tic1 = time.perf_counter()
            # TODO: make these two functions more readable
            weights, branch_probs = build_branches(source_branches, values, imt, loc, vs30)
            hazard = calculate_aggs(branch_probs, aggs, weights)
            
            # TODO: remove me!
            # save_dir = '/work/chrisdc/NZSHM-WORKING/PROD/branch_rlz/'
            # branches_filepath = save_dir + f'branches_{imt}-{loc}-{vs30}'
            # weights_filepath = save_dir + f'weights_{imt}-{loc}-{vs30}'
            # np.save(branches_filepath, branch_probs)
            # np.save(weights_filepath, weights)

            # toc1 = time.perf_counter()
            # print(f'time to calculate_aggs {toc1-tic1} seconds')
            
            if deagg:
                save_deaggs(
                    hazard, loc, imt, poe
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


class DeAggregationWorkerMP(multiprocessing.Process):
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

            process_single_deagg(nt)
            self.task_queue.task_done()
            log.info('%s task done.' % self.name)
            self.result_queue.put(str(nt.gtdatafile))


def process_local_serial(
    hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, num_workers, deagg=False
):
    """run task serially. This is temporoary to help debug deaggs"""

    toshi_ids = {int(k): v for k, v in toshi_ids.items()}
    source_branches = {int(k): v for k, v in source_branches.items()}
    deagg_poe = config.deagg_poes[0] if deagg else None # TODO: poes is a list, but when processing we only want one value, bit of a hack to use same entry in the config for both

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
                deagg,
                deagg_poe,
            )

            # process_location_list(t, config.deagg_poes[0])
            process_location_list(t)



def process_local(
    hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, num_workers, deagg=False
):
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
    deagg_poe = config.deagg_poes[0] if deagg else None # TODO: poes is a list, but when processing we only want one value, bit of a hack to use same entry in the config for both
    
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
                deagg,
                deagg_poe,
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


def process_aggregation(config: AggregationConfig, deagg=False):
    """Configure the tasks."""
    omit: List[str] = []

    if deagg:
        config.validate_deagg()
        config.validate_deagg_file()
        gtdata = config.deagg_solutions
    else:
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

    if deagg:  # TODO: need some "levels" for deaggs (deagg bins), this can come when we pull deagg data from THS
        levels = []
    else:
        levels = get_levels(
            source_branches[config.vs30s[0]], [example_loc_code], config.vs30s[0]
        )  # TODO: get seperate levels for every IMT
        avail_imts = get_imts(source_branches[config.vs30s[0]], config.vs30s[0])  # TODO: equiv check for deaggs
        for imt in config.imts:
            assert imt in avail_imts

    process_local(
        config.hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, NUM_WORKERS, deagg=deagg
    )  # TODO: use source_branches dict

    # process_local_serial(
    #     config.hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, NUM_WORKERS, deagg=deagg
    # )  # TODO: use source_branches dict



def process_deaggregation(config: AggregationConfig):
    """ Aggregate the Deaggregations in parallel."""

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

    for gtdatafile in config.deagg_gtdatafiles:
        t = DeaggTaskArgs(
            gtdatafile,
            config.logic_tree_permutations,
            config.src_correlations,
            config.gmm_correlations,
            config.source_branches_truncate,
            config.aggs[0], #TODO: assert len(config.aggs) == 1 on load config
            config.hazard_model_id,
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
    print(f'time to run deaggregations: {toc-tic:.0f} seconds')
    return results
        

def get_deagg_config(gtdatafile):

    DeaggConfig = namedtuple("DeaggConfig", "vs30 imt location poe inv_time")
    with open(gtdatafile,'r') as jsonfile:
        data = json.load(jsonfile)
    node = data['deagg_solutions']['data']['node1']['children']['edges'][0]
    args = node['node']['child']['arguments']
    for arg in args:
        if arg['k'] == "disagg_config":
            disagg_args =  json.loads(arg['v'].replace("'", '"'))
        
    location = disagg_args['location']
    vs30 = int(disagg_args['vs30'])
    imt = disagg_args['imt']
    poe = float(disagg_args['poe'])
    inv_time = int(disagg_args['inv_time'])
    return DeaggConfig(vs30, imt, location, poe, inv_time)


def get_gtdata(gtdatafile):

    return json.load(Path(gtdatafile).open('r'))['deagg_solutions']

def process_single_deagg(task_args: DeaggTaskArgs):

    gtdatafile = task_args.gtdatafile
    
    deagg_config = get_deagg_config(gtdatafile)
    gtdata = get_gtdata(gtdatafile)

    location = deagg_config.location.split('~')
    loc = (float(location[0]), float(location[1]))
    resolution = 0.001
    coded_location = CodedLocation(*loc, resolution)

    omit: List[str] = []

    toshi_ids = [
        b.hazard_solution_id
        for b in merge_ltbs_fromLT(task_args.logic_tree_permutations, gtdata=gtdata, omit=omit)
        if b.vs30 == deagg_config.vs30
    ]

    source_branches = build_source_branches(
        task_args.logic_tree_permutations,
        gtdata,
        task_args.src_correlations,
        task_args.gmm_correlations,
        deagg_config.vs30,
        omit,
        toshi_ids,
        truncate=task_args.source_branches_truncate,
    )
    log.info('finished building logic tree ')

    levels = [] # TODO: need some "levels" for deaggs (deagg bins), this can come when we pull deagg data from THS
    
    t = AggTaskArgs(
    task_args.hazard_model_id,
    coded_location.downsample(0.1).code,
    [coded_location.downsample(0.001).code],
    toshi_ids,
    source_branches,
    [task_args.agg],
    [deagg_config.imt],
    levels,
    deagg_config.vs30,
    True,
    deagg_config.poe,
    )

    process_location_list(t)
