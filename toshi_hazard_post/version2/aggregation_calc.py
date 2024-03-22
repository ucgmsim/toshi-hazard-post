from typing import Tuple, TYPE_CHECKING, List, Sequence
import numpy as np
from .data import load_realizations, save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation
    from .logic_tree import HazardLogicTree
    from .aggregation import Site
    from .data import ValueStore



def build_branch_rates(logic_tree: 'HazardLogicTree', values: 'ValueStore', location: 'CodedLocation', vs30: int) -> 'npt.NDArray':
    """
    Calculate the rate for the composite branches in the logic tree (all combination of SRM branch sets and applicable GMCM models).

    Output is a numpy array with dimensions (branch, IMTL)

    Parameters:
        logic_tree:  the complete (srm + gmcm combined) logic tree
        values: component realization rates
        location: the site location
        vs30: the site vs30

    Returns:
        rates: hazard rates for every composite realization of the model
    """
    pass


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', aggs: Sequence[str]) -> 'npt.NDArray':
    """
    Calculate weighted aggregate statistics of the composite realizations

    Parameters:
        branch_rates: hazard rates for every composite realization of the model with dimensions (branch, IMTL)
        weights: one dimensional array of weights for composite branches
        aggs: the aggregate statistics to be calculated (e.g., "mean", "0.5")
    
    Returns:
        hazard: aggregate rates array with dimension (agg, IMTL)
    """
    pass


def calc_aggregation(
        site: Site,
        imt: str,
        aggs: List[str],
        levels: 'npt.NDArray',
        weights: 'npt.NDArray',
        logic_tree: HazardLogicTree,
        compatibility_key: str,
        hazard_model_id: str,
) -> bool:
    """
    Calculate hazard aggregation for a single site and imt and save result

    Parameters:
        site: location, vs30 pair
        imt: Intensity measure type (e.g. "PGA", "SA(1.5)")
        levels: IMTLs for the hazard curve
        weights: weights for the branches of the logic tree
        logic_tree: the complete (srm + gmcm combined) logic tree
        compatibility_key: the key identifying the hazard calculation compatibility entry
        hazard_model_id: the id of the hazard model for storing results in the database
    
    Returns:
        success: did the calculation complete sucessfully?
    """
    location = site.location
    vs30 = site.vs30

    values = load_realizations(logic_tree, imt, location, vs30, compatibility_key)
    branch_rates = build_branch_rates(logic_tree, values, location, vs30)
    hazard = calculate_aggs(branch_rates, weights, aggs)
    save_aggregations(hazard, location, vs30, imt, aggs, hazard_model_id)

    return True


