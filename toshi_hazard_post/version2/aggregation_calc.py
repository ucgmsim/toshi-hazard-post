from typing import Tuple, TYPE_CHECKING, List, Sequence, Optional
import numpy as np
from .data import load_realizations, save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation
    from toshi_hazard_post.version2.logic_tree import HazardLogicTree, CompositeBranch
    from toshi_hazard_post.version2.aggregation import Site   
    from toshi_hazard_post.version2.data import ValueStore



def build_branch_rates(logic_tree: 'HazardLogicTree', values: 'ValueStore', location: 'CodedLocation', vs30: int, nlevels: int) -> 'npt.NDArray':
    """
    Calculate the rate for the composite branches in the logic tree (all combination of SRM branch sets and applicable GMCM models).

    Output is a numpy array with dimensions (branch, IMTL)

    Parameters:
        logic_tree:  the complete (srm + gmcm combined) logic tree
        values: component realization rates
        location: the site location
        vs30: the site vs30
        nlevels: the number of levels (IMTLs) in the rate array

    Returns:
        rates: hazard rates for every composite realization of the model D(branch, IMTL)
    """

    rates = np.empty((logic_tree.nbranches, nlevels))
    for i, composite_branch in enumerate(logic_tree.composite_branches):
        rates[i, :] = calc_composite_rates(composite_branch)


def calc_composite_rates(composite_branch: 'CompositeBranch', values: ValueStore, nlevels: int) -> 'npt.NDArray':
    """
    Calculate the rate for a single composite branch of the logic tree by summing rates of the component branches

    Parameters:
        composite_branch: the composite branch for which to calculate rates
        values: the value store
        nlevels: the number of levels (IMTLs) in the rate array

    Returns:
        rates: hazard rates for the composite realization D(nlevels,)
    """
    rates = np.zeros((nlevels,))
    for component_branch in composite_branch:
        rates += values.get_values(component_branch)


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
) -> Optional[Exception]:
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
        exception: the raised exception if any part of the calculation fails
    """
    location = site.location
    vs30 = site.vs30

    try:
        values = load_realizations(logic_tree, imt, location, vs30, compatibility_key)
        branch_rates = build_branch_rates(logic_tree, values, location, vs30)
        hazard = calculate_aggs(branch_rates, weights, aggs)
        save_aggregations(hazard, location, vs30, imt, aggs, hazard_model_id)
    except Exception as e:
        return e
