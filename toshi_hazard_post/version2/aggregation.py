import logging
import multiprocessing
import sys
import time
import traceback
from typing import TYPE_CHECKING, Generator, List, Tuple

from nzshm_common.location.code_location import bin_locations

from toshi_hazard_post.version2.aggregation_calc import AggTaskArgs, calc_aggregation
from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_setup import Site, get_lts, get_sites  # , get_levels
from toshi_hazard_post.version2.data import get_realizations_dataset
from toshi_hazard_post.version2.local_config import NUM_WORKERS
from toshi_hazard_post.version2.logic_tree import HazardLogicTree

if TYPE_CHECKING:
    import pyarrow.dataset as ds

    from toshi_hazard_post.version2.logic_tree import HazardComponentBranch

log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0


def test_function(task_args):
    print("I'm a test function!", task_args.site, task_args.imt)
    time.sleep(1)
    raise Exception("oops")


class AggregationWorkerMP(multiprocessing.Process):
    """A worker that batches aggregation processing."""

    def __init__(self, task_queue: multiprocessing.JoinableQueue, result_queue: multiprocessing.Queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        log.info("worker %s running." % self.name)
        proc_name = self.name

        while True:
            task_args = self.task_queue.get()
            if task_args is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)
                break
            log.info(f"worker {self.name} working on hazard for site: {task_args.site}, imt: {task_args.imt}")

            try:
                calc_aggregation(task_args)  # calc_aggregation
                self.task_queue.task_done()
                log.info('%s task done.' % self.name)
                self.result_queue.put(str(task_args.imt))
            except Exception:
                log.error(traceback.format_exc())
                args = f"{task_args.site}, {task_args.imt}"
                self.result_queue.put(f'FAILED {args} {traceback.format_exc()}')
                self.task_queue.task_done()


class TaskGenerator:
    def __init__(
        self,
        sites: List[Site],
        imts: List[str],
        component_branches: List['HazardComponentBranch'],
        compatability_key: str,
    ):
        self.imts = imts
        self.component_branches = component_branches
        self.compatability_key = compatability_key

        self.locations = [site.location for site in sites]
        self.vs30s = [site.vs30 for site in sites]

    def task_generator(self) -> Generator[Tuple[Site, str, 'ds.Dataset'], None, None]:
        for location_bin in bin_locations(self.locations, PARTITION_RESOLUTION).values():
            dataset = get_realizations_dataset(location_bin, self.component_branches, self.compatability_key)
            for location in location_bin:
                idx = self.locations.index(location)
                self.locations.pop(idx)
                vs30 = self.vs30s.pop(idx)
                site = Site(location=location, vs30=vs30)
                for imt in self.imts:
                    yield site, imt, dataset


def setup_multiproc(num_workers: int):
    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    print('Creating %d workers' % num_workers)
    workers = [AggregationWorkerMP(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()
    return task_queue, result_queue


def run_aggregation(config: AggregationConfig) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """
    num_workers = NUM_WORKERS

    time0 = time.perf_counter()
    # get the sites
    log.info("getting sites . . .")
    sites = get_sites(config.locations, config.vs30s)

    # create the logic tree objects and build the full logic tree
    log.info("getting logic trees . . . ")
    srm_lt, gmcm_lt = get_lts(config)
    logic_tree = HazardLogicTree(srm_lt, gmcm_lt)

    log.info("calculating weights and branch hash table . . . ")
    tic = time.perf_counter()
    weights = logic_tree.weights
    branch_hash_table = logic_tree.branch_hash_table
    toc = time.perf_counter()

    log.info(f'time to build weight array and hash table {toc-tic:.2f} seconds')
    log.info("Size of weight array: {}MB".format(weights.nbytes >> 20))
    log.info("Size of hash table: {}MB".format(sys.getsizeof(branch_hash_table) >> 20))

    component_branches = logic_tree.component_branches

    task_queue, result_queue = setup_multiproc(num_workers)
    task_generator = TaskGenerator(sites, config.imts, component_branches, config.compat_key)
    num_jobs = 0
    for site, imt, dataset in task_generator.task_generator():
        task_args = AggTaskArgs(
            dataset=dataset,
            site=site,
            imt=imt,
            agg_types=config.agg_types,
            weights=weights,
            branch_hash_table=branch_hash_table,
            hazard_model_id=config.hazard_model_id,
        )
        task_queue.put(task_args)
        num_jobs += 1
    total_jobs = num_jobs

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    # TODO: prevent exceptions from stopping join() (main process will just sit there)
    task_queue.join()

    # TODO: catch exceptions and report trace
    results: List[str] = []
    while num_jobs:
        result = result_queue.get()
        results.append(result)
        num_jobs -= 1

    time1 = time.perf_counter()
    log.info(f"processed {total_jobs} calculations in {round(time1 - time0, 3)} seconds")

    print("")
    print("FAILED JOBS . . . ")
    for result in results:
        if 'FAILED' in result:
            print(result)

    # print(results[0])


# if __name__ == "__main__":
#     config_filepath = "tests/version2/fixtures/hazard.toml"
#     config = AggregationConfig(config_filepath)
#     run_aggregation(config)
#     print()
#     print()
#     print()
#     run_aggregation_arrow(config)
