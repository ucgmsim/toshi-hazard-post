import json
import logging
import math
import multiprocessing
import time
from collections import namedtuple
from typing import List

import numpy as np
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID
from toshi_hazard_store.query_v3 import get_hazard_curves

from toshi_hazard_post.branch_combinator import merge_ltbs_fromLT
from toshi_hazard_post.hazard_aggregation.aggregate_rlzs import compute_hazard_at_poe, compute_rate_at_iml
from toshi_hazard_post.hazard_aggregation.aggregation import build_source_branches
from toshi_hazard_post.hazard_aggregation.locations import get_locations
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.hazard_aggregation.aggregation_task import fetch_source_branches
from toshi_hazard_post.hazard_aggregation.aws_aggregation import save_source_branches

from .aggregate_rlzs import (
    build_branches,
    build_rlz_table,
    calculate_aggs,
    get_imts,
    get_levels,
    get_source_and_gsim,
    load_realization_values,
    prob_to_rate,
    rate_to_prob,
)
from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)

DeAggTaskArgs = namedtuple(
    "DeAggTaskArgs", "hazard_model_id grid_loc locs toshi_ids source_branches aggs imts levels vs30 poes inv_time nbranches metric"
)


def process_location_list_deagg(task_args):
    """The math happens inside here... REFACTOR ME. ported from THS."""
    
    hazard_model_id = task_args.hazard_model_id
    locs = task_args.locs
    toshi_ids = task_args.toshi_ids
    source_branches = task_args.source_branches
    aggs = task_args.aggs
    imts = task_args.imts
    levels = task_args.levels
    vs30 = task_args.vs30
    poes = task_args.poes
    inv_time = task_args.inv_time
    nbranches_keep = task_args.nbranches
    metric = task_args.metric

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

    k1 = next(iter(values.keys()))
    k2 = next(iter(values[k1].keys()))
    k3 = next(iter(values[k1][k2].keys()))
    rate_shape = values[k1][k2][k3].shape

    deagg_rlzs = []
    for loc in locs:
        for poe in poes:
            for agg in aggs:
                for imt in imts:
                    # get target level of shaking
                    hc = next(
                        get_hazard_curves([loc], [vs30], [hazard_model_id], [imt], [agg])
                    )  # TODO move up to reduce number of queries?

                    levels = []
                    hazard_vals = []
                    for v in hc.values:
                        levels.append(v.lvl)
                        hazard_vals.append(v.val)

                    target_level = compute_hazard_at_poe(levels, hazard_vals, poe, inv_time)
                    min_dist = math.inf

                    # find realization with nearest level of shaking
                    # TODO: repeating a lot of code here. Unify with agg processing?
                    nbranches = len(source_branches) * len(source_branches[0]['weight_combs'])
                    log.info(f'nbranches: {nbranches}')
                    metric_values = np.empty((nbranches,))
                    
                    
                    i = 0
                    tic = time.perf_counter()
                    for branch in source_branches:
                        rlz_combs = branch['rlz_combs']
                        weight_combs = branch['weight_combs']
                        branch_weight = branch['weight']
                        
                        if metric == 'distance':
                            for rlz_comb in rlz_combs:
                                log.debug(f'calculating realization i: {i}')
                                rate = np.zeros(rate_shape)
                                for rlz in rlz_comb:
                                    rate += prob_to_rate(values[rlz][loc][imt])
                                prob = rate_to_prob(rate)
                                rlz_level = compute_hazard_at_poe(levels, prob, poe, inv_time)
                                dist = abs(rlz_level - target_level)
                                metric_values[i] = dist
                                i += 1

                        elif metric == 'weight':
                            for weight in weight_combs:
                                log.debug(f'calculating realization i: {i}')
                                metric_values[i] = weight * branch_weight
                                i += 1

                        elif metric == 'product':
                            for weight, rlz_comb in zip(weight_combs,rlz_combs):
                                log.debug(f'calculating realization i: {i}')
                                rate = np.zeros(rate_shape)
                                for rlz in rlz_comb:
                                    rate += prob_to_rate(values[rlz][loc][imt])
                                rate_at_targetiml = compute_rate_at_iml(levels, rate, target_level)
                                metric_values[i] = rate_at_targetiml * weight * branch_weight
                                i += 1



                    # find nearest nbranches_keep realizations to the target
                    tic = time.perf_counter()
                    sorter = np.argsort(metric_values)
                    if metric == 'distance':
                        sorter = sorter[:nbranches_keep]
                    elif (metric == 'weight') | (metric == 'product'):
                        sorter = sorter[-nbranches_keep:]
                        sorter = np.flip(sorter)
                    metric_values = metric_values[sorter]

                    i = 0
                    j = 0
                    deagg_specs = []
                    log.info(f'indicies of nearest branches {sorter}')
                    for branch in source_branches:
                        rlz_combs = branch['rlz_combs']
                        weight_combs = branch['weight_combs']
                        branch_weight = branch['weight']

                        for weight, rlz_comb in zip(weight_combs, rlz_combs):
                            if i in sorter:
                                rate = np.zeros(rate_shape)
                                for rlz in rlz_comb:
                                    rate += prob_to_rate(values[rlz][loc][imt])
                                prob = rate_to_prob(rate)
                                rlz_level = compute_hazard_at_poe(levels, prob, poe, inv_time)
                                source_ids, gsims = get_source_and_gsim(rlz_comb, vs30)
                                dist = abs(rlz_level - target_level)

                                log.info(f'regenerating branch {i} ({len(deagg_specs)} of {nbranches_keep} for storage)')

                                rlz_weight = weight * branch_weight
                                hazard_ids = [id.split(':')[0] for id in rlz_comb]

                                rate_at_targetiml = compute_rate_at_iml(levels, rate, target_level)
                                product = rate_at_targetiml * rlz_weight

                                rank = int(np.where(sorter == i)[0])

                                deagg_spec = dict(
                                    rlz_level=rlz_level,
                                    source_ids=source_ids,
                                    gsims=gsims,
                                    rlz=rlz_comb,
                                    hazard_ids=hazard_ids,
                                    weight=rlz_weight,
                                    dist=dist,
                                    product = product,
                                    rank=rank,
                                )
                                deagg_specs.append(deagg_spec)
                                j += 1

                            i += 1

                    deagg_rlzs.append(
                        dict(
                            vs30=vs30,
                            inv_time=inv_time,
                            imt=imt,
                            agg=agg,
                            poe=poe,
                            location=loc,
                            target_level=target_level,
                            deagg_specs=deagg_specs,
                        )
                    )

    return deagg_rlzs


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

            disagg_configs = process_location_list_deagg(nt)

            self.task_queue.task_done()

            log.info('%s task done.' % self.name)
            self.result_queue.put(disagg_configs)


def process_local_deagg(hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, num_workers):
    """Run task locally using Multiprocessing. ported from THS."""

    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print('Creating %d workers' % num_workers)
    workers = [DeAggregationWorkerMP(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0

    toshi_ids = {int(k): v for k, v in toshi_ids.items()}
    source_branches = {int(k): v for k, v in source_branches.items()}

    # log.info('cget values for %s locations and %s hazard_solutions' % (len(locs), len(toshi_ids)))
    for coded_loc in coded_locations:
        for vs30 in config.deagg_vs30s:
            t = DeAggTaskArgs(
                hazard_model_id,
                coded_loc.downsample(0.1).code,
                [coded_loc.downsample(0.001).code],
                toshi_ids[vs30],
                source_branches[vs30],
                config.deagg_aggs,
                config.deagg_imts,
                levels,
                vs30,
                config.deagg_poes,
                config.deagg_invtime,
                config.deagg_nbranches,
                config.deagg_metric,
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
        results += result
        num_jobs -= 1

    toc = time.perf_counter()
    print(f'time to run deaggregations: {toc-tic:.0f} seconds')
    return results


def add_site_name(disagg_configs):
    resolution = 0.001
    disagg_configs_copy = disagg_configs.copy()
    for disagg_config in disagg_configs_copy:
        for loc in LOCATIONS_BY_ID.values():
            location = CodedLocation(loc['latitude'], loc['longitude'], resolution).downsample(0.001).code
            if location == disagg_config['location']:
                disagg_config['site_code'] = loc['id']
                disagg_config['site_name'] = loc['name']

    return disagg_configs_copy


def process_deaggregation(config: AggregationConfig):

    if not config.deaggregation:
        raise Exception('a deaggregation configuration must be specified for deagg mode')
    try:
        config.validate_deagg()
    except:
        raise Exception('invalid deaggregation configuration')

    # assume an aggregation has already been performed that covers the parameters requested in the deagg. This can waste time in 2 ways
    # 1) must re-construct the source_branches
    # 2) if an agg has not been performed then the query comes back empty and we've wasted our time
    omit: List[str] = []

    toshi_ids = {}
    for vs30 in config.deagg_vs30s:
        toshi_ids[vs30] = [
            b.hazard_solution_id
            for b in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=omit)
            if b.vs30 == vs30
        ]


    if config.reuse_source_branches_id:
        log.info("reuse sources_branches_id: %s" % config.reuse_source_branches_id)
        source_branches_id = config.reuse_source_branches_id
        source_branches = fetch_source_branches(source_branches_id)
        source_branches = {int(k): v for k, v in source_branches.items()}
    else:
        log.info("building the sources branches.")

        source_branches = {}
        for vs30 in config.deagg_vs30s:
            source_branches[vs30] = build_source_branches(
                config.logic_tree_permutations,
                config.hazard_solutions,
                config.src_correlations,
                config.gmm_correlations,
                vs30,
                omit,
                truncate=config.source_branches_truncate,
            )
        source_branches_id = save_source_branches(source_branches)
        log.info("saved source_branches to id : %s" % source_branches_id)

    # TODO deagg gets own location list?
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

    deagg_configs = process_local_deagg(
        config.hazard_model_id, toshi_ids, source_branches, coded_locations, levels, config, NUM_WORKERS
    )

    deagg_configs = add_site_name(deagg_configs)

    with open('deagg_configs.json', 'w') as deagg_file:
        json.dump(deagg_configs, deagg_file)
