import logging
import sys
import time

from nzshm_common.location.code_location import bin_locations

from toshi_hazard_post.version2.aggregation_calc import calc_aggregation
from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_setup import Site, get_lts, get_sites  # , get_levels
from toshi_hazard_post.version2.data import get_realizations_dataset
from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0


############
# ARROW
############
def run_aggregation_arrow(config: AggregationConfig) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """

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

    # NEXT:
    # 1. can this be broken into some functions to make easier to read and make testing possible?
    # 2. parallelize (don't know if we need to share weights and hash_table data, they're pretty small. might as well as a cherry on top?)
    locations = [site.location for site in sites]
    vs30s = [site.vs30 for site in sites]
    for location_bin in bin_locations(locations, PARTITION_RESOLUTION).values():
        dataset = get_realizations_dataset(location_bin, component_branches, config.compat_key)

        for location in location_bin:
            idx = locations.index(location)
            locations.pop(idx)
            vs30 = vs30s.pop(idx)
            site = Site(location=location, vs30=vs30)
            for imt in config.imts:

                log.info(f"working on hazard for site: {site}, imts: {imt}")
                tic = time.perf_counter()

                calc_aggregation(
                    dataset=dataset,
                    site=site,
                    imt=imt,
                    agg_types=config.agg_types,
                    weights=weights,
                    branch_hash_table=branch_hash_table,
                    hazard_model_id=config.hazard_model_id,
                )

                toc = time.perf_counter()
                log.info(f'time to perform one aggregation {toc-tic:.2f} seconds')

    time1 = time.perf_counter()
    log.info(f"total toshi-hazard-post time: {round(time1 - time0, 3)}")


# if __name__ == "__main__":
#     config_filepath = "tests/version2/fixtures/hazard.toml"
#     config = AggregationConfig(config_filepath)
#     run_aggregation(config)
#     print()
#     print()
#     print()
#     run_aggregation_arrow(config)
