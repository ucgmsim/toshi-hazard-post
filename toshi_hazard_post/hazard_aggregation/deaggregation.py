import logging
import multiprocessing
import time
from collections import namedtuple
from typing import List

from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_post.branch_combinator import build_source_branches, merge_ltbs_fromLT
from toshi_hazard_post.hazard_aggregation.aggregation import AggTaskArgs, process_location_list
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.toshi_api_support import get_deagg_config, get_gtdata, get_imtl

from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)


DeaggTaskArgs = namedtuple(
    "DeaggTaskArgs",
    "gtid, logic_tree_permutations src_correlations gmm_correlations source_branches_truncate agg hazard_model_id dimensions",
)


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
            self.result_queue.put(str(nt.gtid))


def process_deaggregation(config: AggregationConfig):
    """Aggregate the Deaggregations in parallel."""

    serial = False  # for easier debugging
    if serial:
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

    for gtid in config.deagg_gtids:
        t = DeaggTaskArgs(
            gtid,
            config.logic_tree_permutations,
            config.src_correlations,
            config.gmm_correlations,
            config.source_branches_truncate,
            config.aggs[0],  # TODO: assert len(config.aggs) == 1 on load config
            config.hazard_model_id,
            config.deagg_dimensions,
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


def process_deaggregation_serial(config: AggregationConfig):
    """Aggregate the Deaggregations in serail. For debugging."""

    results = []
    for gtid in config.deagg_gtids:
        t = DeaggTaskArgs(
            gtid,
            config.logic_tree_permutations,
            config.src_correlations,
            config.gmm_correlations,
            config.source_branches_truncate,
            config.aggs[0],  # TODO: assert len(config.aggs) == 1 on load config
            config.hazard_model_id,
            config.deagg_dimensions,
        )

        process_single_deagg(t)
        results.append(t.gtid)

    return results


def process_single_deagg(task_args: DeaggTaskArgs):

    gtdata = get_gtdata(task_args.gtid)
    imtl = get_imtl(gtdata)
    deagg_config = get_deagg_config(gtdata)

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

    levels = []  # TODO: need some "levels" for deaggs (deagg bins), this can come when we pull deagg data from THS

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
        task_args.dimensions,
        deagg_config.poe,
        imtl,
        False,
    )

    process_location_list(t)
