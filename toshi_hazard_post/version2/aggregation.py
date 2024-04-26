import logging
import sys
import time

from toshi_hazard_post.version2.aggregation_calc import calc_aggregation
from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_setup import get_lts, get_sites  # , get_levels
from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


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

    for site in sites:
        for imt in config.imts:

            log.info(f"working on hazard for site: {site}, imts: {imt}")
            tic = time.perf_counter()

            calc_aggregation(
                site=site,
                imt=imt,
                agg_types=config.agg_types,
                weights=weights,
                component_branches=component_branches,
                branch_hash_table=branch_hash_table,
                compatibility_key=config.compat_key,
                hazard_model_id=config.hazard_model_id,
            )

            toc = time.perf_counter()
            log.info(f'time to perform aggregation for one location, {len(config.imts)} imt: {toc-tic:.2f} seconds')

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
