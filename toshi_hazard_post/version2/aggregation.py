import csv
from collections import namedtuple
from typing import Tuple, TYPE_CHECKING, Iterable, List, Union
from pathlib import Path

from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from .aggregation_calc import calc_aggregation
from nzshm_common.location.location import get_locations
from nzshm_model import get_model_version
from nzshm_common.location.code_location import CodedLocation
from .logic_tree import HazardLogicTree
from collections import namedtuple
from .data import get_vs30s


if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree


Site = namedtuple("Site", "location vs30")

def get_sites(locations: Iterable[str], vs30s: List[int]) -> List[Site]:
    """
    Get the sites (combined location and vs30) at which to calculate hazard.

    Parameters:
        locations: location identifiers. Identifiers can be anything accepted by nzshm_common.location.location.get_locations
        vs30s: the vs30s. If empty use the vs30s from the site files

    Returns:
        location_vs30s: Location, vs30 pairs 
    """
    locations = get_locations(locations, resolution=0.001)

    vs30s = []
    if not vs30s:
        for loc_id in locations:
            vs30s + list(get_vs30s(loc_id))
    
    return list(map(Site, locations, vs30s))


def get_lts(config: AggregationConfig) -> Tuple[SourceLogicTree, GMCMLogicTree]:
    """
    Get the SourceLogicTree and GMCMLogicTree objects

    Parameters:
        config: the aggregation configuration

    Returns:
        srm_logic_tree: the seismicity rate model logic tree
        gmcm_logic_tree: the ground motion charactorization model logic tree
    """

    if config.model_version:
        model = get_model_version(config.model_version)
        srm_logic_tree = model.source_logic_tree
        gmcm_logic_tree = model.gmm_logic_tree
    else:
        srm_logic_tree = SourceLogicTree.from_json(config.srm_file)
        gmcm_logic_tree = GMCMLogicTree.from_json(config.gmcm_file)

    return srm_logic_tree, gmcm_logic_tree

def get_levels(compat_key: str) -> 'npt.NDArray':
    """
    Get the intensity measure type levels (IMTLs) for the hazard curve from the compatibility table

    Parameters:
        compatibility_key: the key identifying the hazard calculation compatibility entry
    
    Returns:
        levels: the IMTLs for the hazard calculation
    """
    pass




def run_aggregation(config: AggregationConfig) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """

    # get the sites
    sites = get_sites(config.locations, config.vs30s)

    # create the logic tree objects and build the full logic tree
    srm_lt, gmcm_lt = get_lts(config)
    logic_tree = HazardLogicTree(srm_lt, gmcm_lt)

    # get the weights (this code could be moved to nzshm-model)
    # TODO: this could be done in calc_aggregation() which would avoid passing the weights array when running in
    # parrallel, however, it may be slow? determine speed and decide
    weights = logic_tree.weights

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