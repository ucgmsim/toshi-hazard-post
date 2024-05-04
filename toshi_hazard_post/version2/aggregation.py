import logging
import sys
import time
from typing import TYPE_CHECKING, Generator, List, Tuple, Union

from nzshm_common.location.coded_location import bin_locations

from toshi_hazard_post.version2.aggregation_args import AggregationArgs
from toshi_hazard_post.version2.aggregation_calc import AggTaskArgs, calc_aggregation
from toshi_hazard_post.version2.aggregation_setup import Site, get_lts, get_sites  # , get_levels
from toshi_hazard_post.version2.data import get_realizations_dataset
from toshi_hazard_post.version2.local_config import get_config
from toshi_hazard_post.version2.logic_tree import HazardLogicTree
from toshi_hazard_post.version2.parallel import setup_parallel

if TYPE_CHECKING:
    import multiprocessing
    import queue

    import pyarrow.dataset as ds

    from toshi_hazard_post.version2.logic_tree import HazardComponentBranch

log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0


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


def run_aggregation(args: AggregationArgs) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """
    config = get_config()
    num_workers = config.NUM_WORKERS

    time0 = time.perf_counter()
    # get the sites
    log.info("getting sites . . .")
    sites = get_sites(args.locations, args.vs30s)

    # create the logic tree objects and build the full logic tree
    log.info("getting logic trees . . . ")
    srm_lt, gmcm_lt = get_lts(args)
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

    task_queue: Union['queue.Queue', 'multiprocessing.JoinableQueue']
    result_queue: Union['queue.Queue', 'multiprocessing.Queue']
    task_queue, result_queue = setup_parallel(num_workers, calc_aggregation)

    task_generator = TaskGenerator(sites, args.imts, component_branches, args.compat_key)
    num_jobs = 0
    for site, imt, dataset in task_generator.task_generator():
        task_args = AggTaskArgs(
            dataset=dataset,
            site=site,
            imt=imt,
            agg_types=args.agg_types,
            weights=weights,
            branch_hash_table=branch_hash_table,
            hazard_model_id=args.hazard_model_id,
        )
        task_queue.put(task_args)
        # time.sleep(5)
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

    n_failed = len(list(filter(lambda s: 'FAILED' in s, results)))
    if n_failed:
        print("")
        print(f"THERE ARE {n_failed} FAILED JOBS . . . ")
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
