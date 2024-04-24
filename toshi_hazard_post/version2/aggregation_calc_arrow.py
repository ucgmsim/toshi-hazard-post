import logging
import time
from typing import TYPE_CHECKING, List, Sequence

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

import toshi_hazard_post.version2.calculators as calculators
from toshi_hazard_post.version2.aggregation_calc import calculate_aggs
from toshi_hazard_post.version2.data import save_aggregations

# from toshi_hazard_post.version2.data import load_realizations, save_aggregations
from toshi_hazard_post.version2.data_arrow import load_realizations as load_arrow_realizations

if TYPE_CHECKING:
    from toshi_hazard_post.version2.aggregation_setup import Site
    from toshi_hazard_post.version2.logic_tree import HazardLogicTree, HazardCompositeBranch
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


def calculate_aggs_arrow(rates_weights_table: pa.Table, agg_types: Sequence[str]):
    """

    Calculate weighted aggregate statistics of the composite realizations

    modified from OG, but parked for now until we've figured out the data shapes with CDC
    """
    # flake8: noqa
    branch_rates = rates_weights_table.column(2).to_numpy()
    weights = rates_weights_table.column(3).to_numpy()

    # rw_df = rates_weights_table.to_pandas()
    # rates_series = rw_df['rates']
    # weight_series = rw_df['weight']

    # log.debug(f"calculate_aggs(): branch_rates with shape {rates_series.shape}")
    # log.debug(f"calculate_aggs(): weights with shape {rates_series.shape}")
    log.debug(f"calculate_aggs(): agg_types {agg_types}")

    # vectorize it
    vect_wtd_avg = np.vectorize(calculators.weighted_avg_and_std, otypes=[object])

    # tic = time.perf_counter()
    # val_series_along_np = np.apply_along_axis(vpr, 0, rates, inv_time=1.0)
    # toc = time.perf_counter()
    # print(toc - tic, "numpy")
    # # wa_std = rates_series.apply(vect_wtd_avg, weights=weight_series)
    assert 0
    # nrows = rates_series.shape[0]

    # log.debug(f"{rates_series[:, 0]}")

    # OG code block: for loop
    # ncols = len(agg_types)
    # aggs = np.empty((nrows, ncols)) # (IMTL, agg_type)
    # for i in range(nrows):
    #     quantiles = aggregation_calc.weighted_stats(rates_series[:, i], list(agg_types), sample_weight=weight_series)
    #     aggs[i, :] = np.array(quantiles)

def calc_composite_rates(composite_branch: 'HazardCompositeBranch', component_rates: pa.Table, nlevels: int) -> 'npt.NDArray':

    # option 1, iterate and lookup on dict or pd.Series
    rates = np.zeros((nlevels, ))
    for branch in composite_branch:
        rates += component_rates[branch.hash_digest]
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





def build_branch_rates(logic_tree: 'HazardLogicTree', component_rates) -> 'npt.NDArray':

    # nimtl = len(component_rates.column('rates')[0])
    # nimtl = len(component_rates.iloc[0])
    nimtl = len(next(iter(component_rates.values())))
    # nbranches = logic_tree.n_composite_branches
    # log.info(f'building branch rates for {nbranches} composite branches')
    # branch_rates = np.empty((nbranches, nimtl))
    # for i_branch, branch in enumerate(logic_tree.composite_branches):
    #     branch_rates[i_branch, :] = calc_composite_rates(branch, component_rates, nimtl)
    # return branch_rates
    return np.array([calc_composite_rates(branch, component_rates, nimtl) for branch in logic_tree.composite_branches])




def calc_aggregation_arrow(
    site: 'Site',
    imt: str,
    agg_types: List[str],
    weights: 'npt.NDArray',
    logic_tree: 'HazardLogicTree',
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
    component_probs = load_arrow_realizations(logic_tree, imt, location, vs30, compatibility_key)
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
    composite_rates = build_branch_rates(logic_tree, component_rates)
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
