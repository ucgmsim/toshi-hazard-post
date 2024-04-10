from typing import Tuple, TYPE_CHECKING, List, Sequence, Optional
import toshi_hazard_post.calculators as calculators
import numpy as np
from .data import load_realizations, save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation
    from toshi_hazard_post.version2.logic_tree import HazardLogicTree, HazardCompositeBranch
    from toshi_hazard_post.version2.data import ValueStore
    from toshi_hazard_post.version2.aggregation_setup import Site


def build_branch_rates(logic_tree: 'HazardLogicTree', value_store: 'ValueStore', nlevels: int) -> 'npt.NDArray':
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

    rates = np.empty((logic_tree.n_composite_branches, nlevels))
    for i, composite_branch in enumerate(logic_tree.composite_branches):
        rates[i, :] = calc_composite_rates(composite_branch, value_store, nlevels)
    return rates


def calc_composite_rates(
    composite_branch: 'HazardCompositeBranch', value_store: 'ValueStore', nlevels: int
) -> 'npt.NDArray':
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
        rates += value_store.get_values(component_branch)
    return rates


def weighted_stats(
    values: 'npt.NDArray',
    quantiles: List[str],
    sample_weight: Optional['npt.NDArray'] = None,
) -> 'npt.NDArray':
    """
    Get weighted statistics for a 1D array like object.

    Possble values for quantiles are:
    statistics of interest. Possible values are
        'mean' : weighted arithmetic mean
        'std' : weighted standard deviation
        'cov' : coefficient of varation (std/mean)
        q : quantile where q is a float or the string representation of a float between 0 and 1

    Parameters
        values: the values for which to obtain statistics
        quantiles: statistics of interest.
        sample_weight: weights for values, same length as values

    Returns
        stats: statistics in same order as quantiles
    """

    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = sample_weight / sum(sample_weight)

    get_mean = False
    get_std = False
    get_cov = False
    if ('mean' in quantiles) | ('std' in quantiles) | ('cov' in quantiles):
        mean, std = calculators.weighted_avg_and_std(values, sample_weight)
        if 'mean' in quantiles:
            get_mean = True
            mean_ind = quantiles.index('mean')
            quantiles = quantiles[0:mean_ind] + quantiles[mean_ind + 1 :]
        if 'std' in quantiles:
            get_std = True
            std_ind = quantiles.index('std')
            quantiles = quantiles[0:std_ind] + quantiles[std_ind + 1 :]
        if 'cov' in quantiles:
            get_cov = True
            cov_ind = quantiles.index('cov')
            quantiles = quantiles[0:cov_ind] + quantiles[cov_ind + 1 :]
            cov = std / mean if mean > 0.0 else 0.0

    quants = np.array([float(q) for q in quantiles])  # TODO this is hacky, need to tighten up API with typing
    # print(f'QUANTILES: {quantiles}')

    assert np.all(quants >= 0) and np.all(quants <= 1), 'quantiles should be in [0, 1]'

    wq = calculators.calculate_weighted_quantiles(values, sample_weight, quants)

    if get_cov:
        wq = np.append(np.append(wq[0:cov_ind], np.array([cov])), wq[cov_ind:])
    if get_std:
        wq = np.append(np.append(wq[0:std_ind], np.array([std])), wq[std_ind:])
    if get_mean:
        wq = np.append(np.append(wq[0:mean_ind], np.array([mean])), wq[mean_ind:])

    return wq


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

    nrows = branch_rates.shape[1]
    ncols = len(aggs)
    median = np.empty((nrows, ncols))
    for i in range(nrows):
        quantiles = weighted_stats(branch_rates[:, i], list(aggs), sample_weight=weights)
        median[i, :] = np.array(quantiles)

    return median


def calc_aggregation(
    site: 'Site',
    imt: str,
    aggs: List[str],
    levels: 'npt.NDArray',
    weights: 'npt.NDArray',
    logic_tree: 'HazardLogicTree',
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
        branch_rates = build_branch_rates(logic_tree, values, len(levels))
        hazard = calculate_aggs(branch_rates, weights, aggs)
        save_aggregations(hazard, location, vs30, imt, aggs, hazard_model_id)
    except Exception as e:
        return e

    return None
