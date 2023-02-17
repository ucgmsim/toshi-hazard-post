import logging
import time
from typing import Collection, Iterable, List

import numpy as np
import numpy.typing as npt

from toshi_hazard_post.calculators import calculate_weighted_quantiles, prob_to_rate, rate_to_prob, weighted_avg_and_std
from toshi_hazard_post.data_functions import ValueStore

# from toshi_hazard_post.logic_tree.branch_combinator import SourceBranchGroup
from toshi_hazard_post.logic_tree.logic_tree import GMCMBranch, HazardLogicTree

DTOL = 1.0e-6
INV_TIME = 1.0
VERBOSE = True

log = logging.getLogger(__name__)


def weighted_stats(values: npt.NDArray, quantiles: List[str], sample_weight: Iterable[float] = None) -> npt.NDArray:
    """Get weighted statistics for a 1D array like object.

    Parameters
    ----------
    values
        the values for which to obtain statistics
    quantiles
        statistics of interest. Possible values are
        'mean' : weighted arithmetic mean
        'std' : weighted standard deviation
        'cov' : coefficient of varation (std/mean)
        q : quantile where q is a float or the string representation of a float between 0 and 1
    sample_weight
        weights for values, same length as values

    Returns
    -------
    stats
        statistics in same order as quantiles
    """

    tic = time.perf_counter()

    # values = np.array(values)
    if sample_weight is None:
        sample_weight = np.ones(values.shape[0])
    sample_weight = np.array(sample_weight)
    sample_weight = sample_weight / sum(sample_weight)

    get_mean = False
    get_std = False
    get_cov = False
    if ('mean' in quantiles) | ('std' in quantiles) | ('cov' in quantiles):
        mean, std = weighted_avg_and_std(values, sample_weight)
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
            cov = std / mean

    quants = np.array([float(q) for q in quantiles])  # TODO this is hacky?
    # print(f'QUANTILES: {quantiles}')

    assert np.all(quants >= 0) and np.all(quants <= 1), 'quantiles should be in [0, 1]'

    wq = calculate_weighted_quantiles(values, sample_weight, quants)

    if get_cov:
        wq = np.vstack( (np.vstack((wq[0:cov_ind,:], cov)), wq[cov_ind:,:]) )
    if get_std:
        wq = np.vstack( (np.vstack((wq[0:std_ind,:], std)), wq[std_ind:,:]) )
    if get_mean:
        wq = np.vstack( (np.vstack((wq[0:mean_ind,:], mean)), wq[mean_ind:,:]) )

    toc = time.perf_counter()
    log.debug(f'time to calculate weighted quantiles {toc-tic} seconds')

    return wq


def calc_weighted_sum(
    gmcm_branches: List[GMCMBranch], values: ValueStore, loc: str, imt: str, start_ind: int, end_ind: int
) -> npt.NDArray:
    """Calculate the weighted sum of probabilities, first converting to rate, then back to probability. Works on
    probability array in chunks to reduce memory usage.

    Parameters
    ----------
    rlz_combs
        ToshiID:gsim_realization keys
    values
        probability values
    loc
        coded location
    imt
        intensity measure type
    start_ind
        start index of probability array to work on
    end_ind
        end index of probability array to work on

    Returns
    -------
    probability
        axis0 = realization combination
        axis1 = probability array
    """

    nrows = len(gmcm_branches)
    ncols = end_ind - start_ind
    prob_table = np.empty((nrows, ncols))

    for i, gmcm_branch in enumerate(gmcm_branches):
        rate = np.zeros((ncols,))
        for rlz in gmcm_branch.realizations:
            rate += prob_to_rate(values.values(key=rlz, loc=loc, imt=imt)[start_ind:end_ind], INV_TIME)
        prob = rate_to_prob(rate, INV_TIME)
        prob_table[i, :] = prob

    return prob_table


def calculate_aggs(branch_probs: npt.NDArray, aggs: List[str], weight_combs: Collection[float]) -> npt.NDArray:
    """Gets aggregate statistics for array of probability curves.

    Parameters
    ----------
    branch_probs
        probabilities
        aggregate statistics will be taken across axis 0
    aggs
        aggregate statistics of interest. Possible values are
        'mean' : weighted arithmetic mean
        'std' : weighted standard deviation
        'cov' : coefficient of varation (std/mean)
        q : quantile where q is a float or the string representation of a float between 0 and 1
    weight_combs
        weights for values, len(weight_combs) = branch_probs.shape[0]

    Returns
    -------
    probs
        element by element aggregate statistics
        axis 0 = probability curve (e.g. values of hazard curve)
        axis 1 = aggs
    """

    # TODO: eliminate redundant prob-->rate-->prop conversion

    branch_probs = prob_to_rate(branch_probs, INV_TIME)

    # nrows = branch_probs.shape[1]
    # ncols = len(aggs)
    # median = np.empty((nrows, ncols))
    # for i in range(nrows):
        # quantiles = weighted_stats(branch_probs[:, i], aggs, sample_weight=weight_combs)
        # median[i, :] = np.array(quantiles)
    quantiles = weighted_stats(branch_probs, aggs, sample_weight=weight_combs)
    return rate_to_prob(quantiles, INV_TIME)


# def get_len_rate(values: Dict[str, dict]) -> int:
#     """Get the length of the probability array.
#     Depricated

#     Parameters
#     ----------
#     values
#         probability values

#     Returns
#     -------
#     length
#     """

#     # TODO: is there a better way to do this? Maybe if values is stored as a DataFrame?
#     k1 = next(iter(values.keys()))
#     k2 = next(iter(values[k1].keys()))
#     k3 = next(iter(values[k1][k2].keys()))
#     rate_shape = values[k1][k2][k3].shape

#     return rate_shape[0]


def get_branch_weights(logic_tree: HazardLogicTree) -> npt.NDArray:
    """Get the weight of every realization of the full, combined source and gsim logic tree.

    Parameters
    ----------
    source_branches
        all source branches of complete logic tree

    Returns
    -------
    weights
        multiplicitive weights of all branches of full, combined logic tree
    """

    nbranches = len(logic_tree.branches)  # TODO: should these be properties?
    nrows = len(logic_tree.branches[0].gmcm_branches) * nbranches
    weights = np.empty((nrows,))
    for i, branch in enumerate(logic_tree.branches):
        w = np.array(branch.gmcm_branch_weights) * branch.weight
        weights[i * len(w) : (i + 1) * len(w)] = w

    return weights


def build_branches(
    logic_tree: HazardLogicTree,
    values: ValueStore,
    imt: str,
    loc: str,
    start_ind: int,
    end_ind: int,
) -> npt.NDArray:
    """For each source branch, calculate the weighted sum probability.

    Parameters
    ----------
    source_branches
        all source branches of complete logic tree
    values
        probability values
    imt
        intensity measure type
    loc
        coded location
    vs30
        not used
    start_ind
        start index of probability array to work on
    end_ind
        end index of probability array to work on

    Returns
    -------
    probability
        axis0 = realization combination
        axis1 = probability array
    """

    nbranches = len(logic_tree.branches)
    ncombs = len(logic_tree.branches[0].gmcm_branches)
    nrows = ncombs * nbranches
    # ncols = get_len_rate(values)
    ncols = end_ind - start_ind
    branch_probs = np.empty((nrows, ncols))

    tic = time.process_time()
    for i, branch in enumerate(logic_tree.branches):  # ~320 source branches
        # rlz_combs, weight_combs = build_rlz_table(branch, vs30)

        # set of realization probabilties for a single complete source branch
        # these can then be aggrigated in prob space (+/- impact of NB) to create a hazard curve
        branch_probs[i * ncombs : (i + 1) * ncombs, :] = calc_weighted_sum(
            branch.gmcm_branches, values, loc, imt, start_ind, end_ind
        )

        log.debug(f'built branch {i+1} of {nbranches}')

    toc = time.perf_counter()
    log.debug('build_branches took: %s ' % (toc - tic))

    return branch_probs
