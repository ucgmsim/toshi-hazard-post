import logging
import time
from typing import TYPE_CHECKING, List, Sequence, Optional

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

import toshi_hazard_post.version2.calculators as calculators
from toshi_hazard_post.version2.data import save_aggregations

# from toshi_hazard_post.version2.data import load_realizations, save_aggregations
from toshi_hazard_post.version2.data import load_realizations

if TYPE_CHECKING:
    from toshi_hazard_post.version2.aggregation_setup import Site
    from toshi_hazard_post.version2.logic_tree import HazardLogicTree, HazardCompositeBranch, HazardComponentBranch
    import numpy.typing as npt

log = logging.getLogger(__name__)


def join_rates_weights(rlz_table: pa.Table, weights: pa.Table) -> pa.Table:
    """join the tables using the two digest columns

    Using duckdb for now because ...
      - still using our in-memory dataframes
      - SQL syntax
      - hides some complexity
    """
    con = duckdb.connect()
    joined = con.execute(
        f"SELECT r.sources_digest, r.gmms_digest, r.rates, w.weight FROM rlz_table r JOIN "
        f"weights w ON r.sources_digest = w.sources_digest AND r.gmms_digest = w.gmms_digest;"
    )
    rates_weights_joined = joined.arrow()

    if True:
        # af9ec2b004d7 380a95154af2
        smpl = con.execute(
            "SELECT * FROM rates_weights_joined WHERE sources_digest = 'af9ec2b004d7' "
            "AND gmms_digest = '380a95154af2';"
        )
        smpl_df = smpl.arrow().to_pandas()
        print(smpl_df)

    log.info(f"rates_weights_joined shape: {rates_weights_joined.shape}")
    # log.debug(rates_weights_joined.to_pandas()) # Don't do this for logging it takes significant time
    log.info(
        "RSS: {}MB".format(pa.total_allocated_bytes() >> 20)
    )  #     value_store.set_values(values, component_branch)
    return rates_weights_joined


def convert_probs_to_rates(rlz_table):
    """all aggregations must be performed in rates space, but rlz have probablities

    here we're only vectorising internally to the row, maybe this could be done over the entire columns ??
    """
    probs_array = rlz_table.column(2).to_numpy()

    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    rates_array = np.apply_along_axis(vpr, 0, probs_array, inv_time=1.0)
    return rlz_table.set_column(2, 'rates', pa.array(rates_array))


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

def calc_composite_rates(branch_hashes: List[str], component_rates: pa.Table, nlevels: int) -> 'npt.NDArray':

    # option 1, iterate and lookup on dict or pd.Series
    rates = np.zeros((nlevels, ))
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





def build_branch_rates(branch_hash_list: List[List[str]], component_rates) -> 'npt.NDArray':

    # nimtl = len(component_rates.column('rates')[0])
    # nimtl = len(component_rates.iloc[0])
    nimtl = len(next(iter(component_rates.values())))
    # nbranches = logic_tree.n_composite_branches
    # log.info(f'building branch rates for {nbranches} composite branches')
    # branch_rates = np.empty((nbranches, nimtl))
    # for i_branch, branch in enumerate(logic_tree.composite_branches):
    #     branch_rates[i_branch, :] = calc_composite_rates(branch, component_rates, nimtl)
    # return branch_rates
    return np.array([calc_composite_rates(branch, component_rates, nimtl) for branch in branch_hash_list])




def calc_aggregation_arrow(
    site: 'Site',
    imt: str,
    agg_types: List[str],
    weights: 'npt.NDArray',
    component_branches: List['HazardComponentBranch'],
    branch_hash_list: List[List[str]],
    compatibility_key: str,
    hazard_model_id: str,
) -> pa.table:
    """
    Calculate hazard aggregation for a single site and imt and save result

    Parameters:
        site: location, vs30 pair
        imt: Intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5")
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

    log.info("loading realizations . . .")
    tic = time.perf_counter()
    component_probs = load_realizations(component_branches, imt, location, vs30, compatibility_key)
    toc = time.perf_counter()
    log.debug(f'time to load realizations {toc-tic:.2f} seconds')
    log.debug(f"rlz_table {component_probs.shape}")
    # log.debug(rlz_table.to_pandas())
    # print(weights)

    # convert probabilities to rates
    tic = time.perf_counter()
    component_rates = convert_probs_to_rates(component_probs)
    del component_probs
    toc = time.perf_counter()
    log.debug(f'time to convert_probs_to_rates() {toc-tic:.2f} seconds')

    tic = time.perf_counter() 
    # make the digest the index
    component_rates = component_rates.append_column(
        'digest',
        pc.binary_join_element_wise(
            pc.cast(component_rates['sources_digest'], pa.string()),
            pc.cast(component_rates['gmms_digest'], pa.string()),
            ""
        )
    )
    component_rates = component_rates.drop_columns(['sources_digest', 'gmms_digest'])
    component_rates = component_rates.to_pandas()
    component_rates.set_index('digest', inplace=True)
    # component_rates = component_rates['rates']
    component_rates = component_rates['rates'].to_dict()
    toc = time.perf_counter()
    log.debug(f'time to convert to pandas and set digest index {toc-tic:.2f} seconds')
    # log.debug(f"rates_table {component_rates.shape}")
    log.debug(f"rates_table {len(component_rates)}")

    tic = time.perf_counter()
    composite_rates = build_branch_rates(branch_hash_list, component_rates)
    toc = time.perf_counter()
    log.debug(f'time to build_ranch_rates() {toc-tic:.2f} seconds')
    
    tic = time.perf_counter()
    log.info("calculating aggregates . . . ")
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    toc = time.perf_counter()
    log.debug(f'time to calculate aggs {toc-tic:.2f} seconds')

    log.info("saving result . . . ")
    save_aggregations(hazard, location, vs30, imt, agg_types, hazard_model_id)


    log.info("saving result . . . ")
    # save_aggregations(hazard, location, vs30, imt, agg_types, hazard_model_id)

    return None
