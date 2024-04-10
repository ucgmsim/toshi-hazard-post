import csv
from dataclasses import dataclass
from collections import namedtuple
from typing import Tuple, TYPE_CHECKING, Iterable, List, Union
from pathlib import Path
from itertools import product

import numpy as np

from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from .aggregation_calc import calc_aggregation
from nzshm_common.location.location import get_locations
from nzshm_model import get_model_version
from nzshm_common.location.code_location import CodedLocation
from .logic_tree import HazardLogicTree
from collections import namedtuple
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree
from .aggregation_setup import get_lts, get_sites, get_levels


if TYPE_CHECKING:
    import numpy.typing as npt



def run_aggregation(config: AggregationConfig) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """

    # get the sites
    sites = get_sites(config.locations, config.vs30s)

    # create the logic tree objects and build the full logic tree
    # TODO: pre-calculating the logic tree will require serialization if dsitributing in cloud. However, the object cannot be serialized due to use of FilteredBranch
    srm_lt, gmcm_lt = get_lts(config)
    logic_tree = HazardLogicTree(srm_lt, gmcm_lt)

    # get the weights (this code could be moved to nzshm-model)
    # TODO: this could be done in calc_aggregation() which would avoid passing the weights array when running in
    # parrallel, however, it may be slow? determine speed and decide
    weights = np.array(logic_tree.weights)

    # get the levels for the compatibility
    levels = get_levels(config.compat_key)

    # for each independent thing (location, imt, vs30) (do we want to allow looping over vs30s?)
    # each of these can be placed in a multiprocessing queue
    for site in sites:
        for imt in config.imts:
            calc_aggregation(site, imt, config.aggs, levels, weights, logic_tree, config.compat_key, config.hazard_model_id)






if __name__ == "__main__":

    config_filepath = "tests/version2/fixtures/hazard.toml"    
    config = AggregationConfig(config_filepath)
    run_aggregation(config)