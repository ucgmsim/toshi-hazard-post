import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Sequence

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


@dataclass
class AggTaskArgs:
    dataset: Dict[str, 'ds.Dataset']
    site: 'Site'
    imt: str
    agg_types: List[str]
    weights: 'npt.NDArray'
    branch_hash_table: List[List[str]]
    hazard_model_id: str


def convert_probs_to_rates(probs: pa.Table) -> pa.Table:
    """all aggregations must be performed in rates space, but rlz have probablities

    here we're only vectorising internally to the row, maybe this could be done over the entire columns ??
    """
    probs_array = probs.column(2).to_numpy()

    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    rates_array = np.apply_along_axis(vpr, 0, probs_array, inv_time=1.0)
    return probs.set_column(2, 'rates', pa.array(rates_array))


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', agg_types: Sequence[str]) -> 'npt.NDArray':
    """
    Calculate weighted aggregate statistics of the composite realizations

    Parameters:
        branch_rates: hazard rates for every composite realization of the model with dimensions (branch, IMTL)
        weights: one dimensional array of weights for composite branches with dimensions (branch,)
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5") with dimension (agg_type,)

    Returns:
        hazard: aggregate rates array with dimension (agg_type, IMTL)
    """

    log.debug(f"branch_rates with shape {branch_rates.shape}")
    log.debug(f"weights with shape {weights.shape}")
    log.debug(f"agg_types {agg_types}")

    def is_float(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def index(lst, value):
        try:
            return lst.index(value)
        except ValueError:
            pass
        return None

    idx_mean = index(agg_types, "mean")
    idx_std = index(agg_types, "std")
    idx_cov = index(agg_types, "cov")
    idx_quantile = [is_float(agg) for agg in agg_types]
    quantile_points = [float(pt) for pt in filter(is_float, agg_types)]

    nlevels = branch_rates.shape[1]
    naggs = 3 + len(quantile_points)
    aggs = np.empty((naggs, nlevels))

    if (idx_mean is not None) | (idx_std is not None) | (idx_cov is not None):
        mean, std = calculators.weighted_avg_and_std(branch_rates, weights)
        cov = calculators.cov(mean, std)
    if quantile_points:
        #  Have not figured out a faster way to do this than a loop. Each level has an independent interpolation
        for i in range(nlevels):
            aggs[idx_quantile, i] = calculators.weighted_quantiles(branch_rates[:, i], weights, quantile_points)

    if idx_mean is not None:
        aggs[idx_mean, :] = mean
    if idx_std is not None:
        aggs[idx_std, :] = std
    if idx_cov is not None:
        aggs[idx_cov, :] = cov

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


def calc_aggregation(task_args: AggTaskArgs) -> None:
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
    dataset_and_flt = task_args.dataset
    site = task_args.site
    imt = task_args.imt
    agg_types = task_args.agg_types
    weights = task_args.weights
    branch_hash_table = task_args.branch_hash_table
    hazard_model_id = task_args.hazard_model_id

    time0 = time.perf_counter()
    location = site.location
    vs30 = site.vs30

    log.info("loading realizations . . .")
    time1 = time.perf_counter()
    component_probs = load_realizations(dataset_and_flt, imt, location, vs30)
    time2 = time.perf_counter()
    log.debug(f'time to load realizations {time2-time1:.2f} seconds')
    log.debug(f"rlz_table {component_probs.shape}")

    # convert probabilities to rates
    component_rates = convert_probs_to_rates(component_probs)
    del component_probs
    time3 = time.perf_counter()
    log.debug(f'time to convert_probs_to_rates() {time3-time2:.2f} seconds')

    component_rates = create_component_dict(component_rates)
    time4 = time.perf_counter()
    log.debug(f'time to convert to dict and set digest index {time4-time3:.2f} seconds')
    log.debug(f"rates_table {len(component_rates)}")

    composite_rates = build_branch_rates(branch_hash_table, component_rates)
    time5 = time.perf_counter()
    log.debug(f'time to build_ranch_rates {time5-time4:.2f} seconds')

    log.info("calculating aggregates . . . ")
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    time6 = time.perf_counter()
    log.debug(f'time to calculate aggs {time6-time5:.2f} seconds')



    log.info("saving result . . . ")
    save_aggregations(calculators.rate_to_prob(hazard, 1.0), location, vs30, imt, agg_types, hazard_model_id)
    time7 = time.perf_counter()
    log.info(f'time to perform one aggregation after loading data {time7-time2:.2f} seconds')
    log.info(f'time to perform one aggregation {time7-time0:.2f} seconds')

    return None
