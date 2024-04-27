import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

import toshi_hazard_post.version2.calculators as calculators

# from toshi_hazard_post.version2.data import load_realizations, save_aggregations
from toshi_hazard_post.version2.data import load_realizations, save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    import pyarrow.dataset as ds

    from toshi_hazard_post.version2.aggregation_setup import Site

log = logging.getLogger(__name__)


def convert_probs_to_rates(probs: pa.Table) -> pa.Table:
    """all aggregations must be performed in rates space, but rlz have probablities

    here we're only vectorising internally to the row, maybe this could be done over the entire columns ??
    """
    probs_array = probs.column(2).to_numpy()

    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    rates_array = np.apply_along_axis(vpr, 0, probs_array, inv_time=1.0)
    return probs.set_column(2, 'rates', pa.array(rates_array))


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


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', agg_types: Sequence[str]) -> 'npt.NDArray':
    """
    Calculate weighted aggregate statistics of the composite realizations

    Parameters:
        branch_rates: hazard rates for every composite realization of the model with dimensions (branch, IMTL)
        weights: one dimensional array of weights for composite branches with dimensions (branch,)
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5") with dimension (agg_type,)

    Returns:
        hazard: aggregate rates array with dimension (IMTL, agg_type)
    """

    log.debug(f"branch_rates with shape {branch_rates.shape}")
    log.debug(f"weights with shape {weights.shape}")
    log.debug(f"agg_types {agg_types}")

    try:
        nrows = branch_rates.shape[1]
    except Exception:
        nrows = len(branch_rates)

    ncols = len(agg_types)
    aggs = np.empty((nrows, ncols))  # (IMTL, agg_type)
    for i in range(nrows):
        quantiles = weighted_stats(branch_rates[:, i], list(agg_types), sample_weight=weights)
        aggs[i, :] = np.array(quantiles)

    log.debug(f"agg with shape {aggs.shape}")
    return aggs


def calc_composite_rates(
    branch_hashes: List[str], component_rates: Dict[str, 'npt.NDArray'], nlevels: int
) -> 'npt.NDArray':
    """
    Calculate the rate for a single composite branch of the logic tree by summing rates of the component branches

    Parameters:
        branch_hashes: the branch hashes for the component branches that comprise the composite branch
        component_rates: component realization rates keyed by component branch hash
        nlevels: the number of levels (IMTLs) in the rate array

    Returns:
        rates: hazard rates for the composite realization D(nlevels,)
    """

    # option 1, iterate and lookup on dict or pd.Series
    rates = np.zeros((nlevels,))
    for branch_hash in branch_hashes:
        rates += component_rates[branch_hash]
    return rates

    # option 2, use list comprehnsion and np.sum. Slower than 1.
    # rates = np.array([component_rates[branch.hash_digest] for branch in composite_branch])
    # return np.sum(rates, axis=0)

    # option 3, slice and sum in place using pd.Series. Very slow
    # digests = [branch.hash_digest for branch in composite_branch]
    # return component_rates[digests].sum()

    # option 4, use NDArray.sum(). Slightly slower than 1
    # return np.array([component_rates[branch.hash_digest] for branch in composite_branch]).sum(axis=0)
    # breakpoint()

    # option 5, build array and then sum. Slower than 1
    # rates = component_rates[composite_branch.branches[0].hash_digest]
    # for branch in composite_branch.branches[1:]:
    #     rates = np.vstack([rates, component_rates[branch.hash_digest]])
    # return rates.sum(axis=0)


def build_branch_rates(branch_hash_table: List[List[str]], component_rates: Dict[str, 'npt.NDArray']) -> 'npt.NDArray':
    """
    Calculate the rate for the composite branches in the logic tree (all combination of SRM branch sets and applicable
    GMCM models).

    Output is a numpy array with dimensions (branch, IMTL)

    Parameters:
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        component_rates: component realization rates keyed by component branch hash

    Returns:
        rates
    """

    nimtl = len(next(iter(component_rates.values())))
    return np.array([calc_composite_rates(branch, component_rates, nimtl) for branch in branch_hash_table])


def create_component_dict(component_rates: pa.Table) -> Dict[str, 'npt.NDArray']:
    component_rates = component_rates.append_column(
        'digest',
        pc.binary_join_element_wise(
            pc.cast(component_rates['sources_digest'], pa.string()),
            pc.cast(component_rates['gmms_digest'], pa.string()),
            "",
        ),
    )
    component_rates = component_rates.drop_columns(['sources_digest', 'gmms_digest'])
    component_rates = component_rates.to_pandas()
    component_rates.set_index('digest', inplace=True)
    # component_rates = component_rates['rates']
    return component_rates['rates'].to_dict()


def calc_aggregation(
    dataset: 'ds.Dataset',
    site: 'Site',
    imt: str,
    agg_types: List[str],
    weights: 'npt.NDArray',
    branch_hash_table: List[List[str]],
    hazard_model_id: str,
) -> None:
    """
    Calculate hazard aggregation for a single site and imt and save result

    Parameters:
        site: location, vs30 pair
        imt: Intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5")
        levels: IMTLs for the hazard curve
        weights: weights for the branches of the logic tree
        component_branches: list of the component branches that are combined to construct the full logic tree
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        compatibility_key: the key identifying the hazard calculation compatibility entry
        hazard_model_id: the id of the hazard model for storing results in the database

    Returns:
        exception: the raised exception if any part of the calculation fails
    """
    location = site.location
    vs30 = site.vs30

    log.info("loading realizations . . .")
    tic = time.perf_counter()

    component_probs = load_realizations(dataset, imt, location, vs30)
    toc = time.perf_counter()
    log.debug(f'time to load realizations {toc-tic:.2f} seconds')
    log.debug(f"rlz_table {component_probs.shape}")

    # convert probabilities to rates
    tic = time.perf_counter()
    component_rates = convert_probs_to_rates(component_probs)
    del component_probs
    toc = time.perf_counter()
    log.debug(f'time to convert_probs_to_rates() {toc-tic:.2f} seconds')

    tic = time.perf_counter()
    component_rates = create_component_dict(component_rates)
    toc = time.perf_counter()
    log.debug(f'time to convert to dict and set digest index {toc-tic:.2f} seconds')
    log.debug(f"rates_table {len(component_rates)}")

    tic = time.perf_counter()
    composite_rates = build_branch_rates(branch_hash_table, component_rates)
    toc = time.perf_counter()
    log.debug(f'time to build_ranch_rates() {toc-tic:.2f} seconds')

    tic = time.perf_counter()
    log.info("calculating aggregates . . . ")
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    toc = time.perf_counter()
    log.debug(f'time to calculate aggs {toc-tic:.2f} seconds')

    log.info("saving result . . . ")
    save_aggregations(calculators.rate_to_prob(hazard, 1.0), location, vs30, imt, agg_types, hazard_model_id)

    return None
