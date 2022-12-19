import ast
import itertools
import logging
import time

import numpy as np
import pandas as pd
from toshi_hazard_store.query_v3 import get_hazard_metadata_v3, get_rlz_curves_v3

# from toshi_hazard_store.branch_combinator.SLT_37_GT_VS400_DATA import data as gtdata
# from toshi_hazard_store.branch_combinator.SLT_37_GT_VS400_gsim_DATA import data as gtdata
from toshi_hazard_post.calculators import prob_to_rate, rate_to_prob, calculate_weighted_quantiles, weighted_avg_and_std

# from toshi_hazard_store.branch_combinator.branch_combinator import get_weighted_branches, grouped_ltbs, merge_ltbs
# from toshi_hazard_store.branch_combinator.SLT_37_GRANULAR_RELEASE_1 import logic_tree_permutations

DTOL = 1.0e-6
INV_TIME = 1.0
VERBOSE = True

log = logging.getLogger(__name__)



def weighted_quantile(values, quantiles, sample_weight=None):

    tic = time.perf_counter()

    values = np.array(values)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
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

    quantiles = np.array(
        [float(q) for q in quantiles]
    )  # TODO this section is hacky, need to tighten up API with typing
    # print(f'QUANTILES: {quantiles}')

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), 'quantiles should be in [0, 1]'

    wq = calculate_weighted_quantiles(values, sample_weight, quantiles)

    if get_cov:
        wq = np.append(np.append(wq[0:cov_ind], np.array([cov])), wq[cov_ind:])
    if get_std:
        wq = np.append(np.append(wq[0:std_ind], np.array([std])), wq[std_ind:])
    if get_mean:
        wq = np.append(np.append(wq[0:mean_ind], np.array([mean])), wq[mean_ind:])

    toc = time.perf_counter()
    log.debug(f'time to calculate weighted quantiles {toc-tic} seconds')

    return wq




def get_weights(branch, vs30):

    weights = {}
    ids = branch['ids']
    for meta in get_hazard_metadata_v3(ids, [vs30]):
        rlz_lt = ast.literal_eval(meta.rlz_lt)  # TODO should I be using this or gsim_lt?
        hazard_id = meta.hazard_solution_id

        for rlz, weight in rlz_lt['weight'].items():
            rlz_key = ':'.join((hazard_id, rlz))
            weights[rlz_key] = weight

    return weights



# @jit(nopython=True)
def calc_weighted_sum(rlz_combs, values, loc, imt, start_ind, end_ind):

    nrows = len(rlz_combs)
    ncols = end_ind - start_ind
    prob_table = np.empty((nrows, ncols))

    for i, rlz_comb in enumerate(rlz_combs):
        rate = np.zeros((ncols,))
        for rlz in rlz_comb:
            rate += prob_to_rate(values[rlz][loc][imt][start_ind:end_ind], INV_TIME)
        prob = rate_to_prob(rate, INV_TIME)
        prob_table[i, :] = prob

    return prob_table


def build_source_branch(values, rlz_combs, imt, loc, start_ind, end_ind):

    # TODO: there has got to be a better way to do this!
    # k1 = next(iter(values.keys()))
    # k2 = next(iter(values[k1].keys()))
    # k3 = next(iter(values[k1][k2].keys()))
    # rate_shape = values[k1][k2][k3].shape

    tic = time.perf_counter()
    # nbranches = len(rlz_combs)
    prob_table = calc_weighted_sum(rlz_combs, values, loc, imt, start_ind, end_ind)

    toc = time.perf_counter()
    log.debug('build_source_branch took: %s' % (toc - tic))
    return prob_table


def calculate_aggs(branch_probs, aggs, weight_combs):

    branch_probs = prob_to_rate(branch_probs, INV_TIME)

    nrows = branch_probs.shape[1]
    ncols = len(aggs)
    median = np.empty((nrows, ncols))
    for i in range(nrows):
        quantiles = weighted_quantile(branch_probs[:, i], aggs, sample_weight=weight_combs)
        median[i, :] = np.array(quantiles)

    return rate_to_prob(median, INV_TIME)


def get_len_rate(values):

    k1 = next(iter(values.keys()))
    k2 = next(iter(values[k1].keys()))
    k3 = next(iter(values[k1][k2].keys()))
    rate_shape = values[k1][k2][k3].shape

    return rate_shape[0]


def get_branch_weights(source_branches):

    nbranches = len(source_branches)
    nrows = len(source_branches[0]['rlz_combs']) * nbranches
    weights = np.empty((nrows,))
    for i, branch in enumerate(source_branches):
        weight_combs = branch['weight_combs']
        w = np.array(weight_combs) * branch['weight']
        weights[i * len(w) : (i + 1) * len(w)] = w

    return weights


def build_branches(source_branches, values, imt, loc, vs30, start_ind, end_ind):
    '''for each source branch, assemble the gsim realization combinations'''

    nbranches = len(source_branches)
    ncombs = len(source_branches[0]['rlz_combs'])
    nrows = ncombs * nbranches
    # ncols = get_len_rate(values)
    ncols = end_ind - start_ind
    branch_probs = np.empty((nrows, ncols))

    tic = time.process_time()
    for i, branch in enumerate(source_branches):  # ~320 source branches
        # rlz_combs, weight_combs = build_rlz_table(branch, vs30)
        rlz_combs = branch['rlz_combs']

        # set of realization probabilties for a single complete source branch
        # these can then be aggrigated in prob space (+/- impact of NB) to create a hazard curve
        branch_probs[i * ncombs : (i + 1) * ncombs, :] = build_source_branch(
            values, rlz_combs, imt, loc, start_ind, end_ind
        )

        log.debug(f'built branch {i+1} of {nbranches}')

    toc = time.perf_counter()
    log.debug('build_branches took: %s ' % (toc - tic))

    return branch_probs


def load_source_branches():

    source_branches = [
        dict(name='test', ids=['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDE0', 'A'], weight=1.0)
        # dict(name='A', ids=['A_CRU', 'A_HIK', 'A_PUY'], weight=0.25),
        # dict(name='B', ids=['B_CRU', 'B_HIK', 'B_PUY'], weight=0.75),
    ]

    return source_branches


def concat_df_files(df_file_names):
    columns = ['lat', 'lon', 'imt', 'agg', 'level', 'hazard']

    hazard_curves = pd.DataFrame(columns=columns)

    dtype = {'lat': str, 'lon': str}

    for df_file_name in df_file_names:
        binned_hazard_curves = pd.read_json(df_file_name, dtype=dtype)
        hazard_curves = pd.concat([hazard_curves, binned_hazard_curves], ignore_index=True)

    return hazard_curves


def compute_rate_at_iml(levels, rates, target_level):

    return np.exp(np.interp(np.log(target_level), np.log(levels), np.log(rates)))


def compute_hazard_at_poe(levels, values, poe, inv_time):

    rp = -inv_time / np.log(1 - poe)
    haz = np.exp(np.interp(np.log(1 / rp), np.flip(np.log(values)), np.flip(np.log(levels))))
    return haz


def get_source_ids(toshi_ids, vs30):

    source_info = []
    for meta in get_hazard_metadata_v3(toshi_ids, [vs30]):
        rlz_lt = ast.literal_eval(meta.rlz_lt)
        source_ids = list(rlz_lt['source combination'].values())[0].split('|')
        nrlz = len(rlz_lt['source combination'])
        source_info.append(dict(source_ids=source_ids, nrlz=nrlz, source_tree_hazid=meta.hazard_solution_id))

    return source_info


def get_source_and_gsim(rlz, vs30):

    gsims = {}
    source_ids = []
    for rlz_key in rlz:
        id, gsim_rlz = rlz_key.split(':')
        meta = next(get_hazard_metadata_v3([id], [vs30]))
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        rlz_lt = ast.literal_eval(meta.rlz_lt)
        trt = gsim_lt['trt']['0']
        if gsims.get(trt):
            # if not gsims[trt] == rlz_lt[trt][gsim_rlz]:
            if not gsims[trt] == gsim_lt['uncertainty'][gsim_rlz]:
                raise Exception(f'single branch has more than one gsim for trt {trt}')
        # gsims[trt] = rlz_lt[trt][gsim_rlz]
        gsims[trt] = gsim_lt['uncertainty'][gsim_rlz]
        source_ids += (rlz_lt['source combination'][gsim_rlz]).split('|')

    source_ids = [sid for sid in source_ids if sid]  # remove empty strings

    return source_ids, gsims


if __name__ == "__main__":
    pass
    """
    tic_total = time.perf_counter()

    # TODO: I'm making assumptions that the levels array is the same for every realization, imt, run, etc.
    # If that were not the case, I would have to add some interpolation

    binned_locs = locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001)

    vs30 = 400
    aggs = ['mean', 0.05, 0.1, 0.2, 0.5, 0.8, 0.9, 0.95]

    # source_branches = load_source_branches()
    # omit = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDEy']  # this is the failed/clonded job
    omit: List[str] = []
    toshi_ids = [b.hazard_solution_id for b in merge_ltbs(logic_tree_permutations, gtdata=gtdata, omit=omit)]

    grouped = grouped_ltbs(merge_ltbs(logic_tree_permutations, gtdata=gtdata, omit=omit))
    source_branches = get_weighted_branches(grouped)

    # imts = get_imts(source_branches, vs30)
    binned_locs = locations_nzpt2_and_nz34_chunked(grid_res=1.0, point_res=0.001)
    levels = get_levels(source_branches, list(binned_locs.values())[0], vs30)  # TODO: get seperate levels for every IMT

    # print(source_branches)
    imts = ['PGA']
    # imts = get_imts(source_branches, vs30)
    levels = get_levels(source_branches, list(binned_locs.values())[0], vs30)  # TODO: get seperate levels for every IMT

    columns = ['lat', 'lon', 'imt', 'agg', 'level', 'hazard']
    # index = range(len(locs)*len(imts)*len(aggs)*len(levels))
    hazard_curves = pd.DataFrame(columns=columns)

    tic = time.perf_counter()
    for i in range(len(source_branches)):
        rlz_combs, weight_combs = build_rlz_table(source_branches[i], vs30)
        source_branches[i]['rlz_combs'] = rlz_combs
        source_branches[i]['weight_combs'] = weight_combs
    toc = time.perf_counter()
    print(f'time to build all realization tables {toc-tic:.1f} seconds')

    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001).items():
        binned_hazard_curves = process_location_list(locs, toshi_ids, source_branches, aggs, imts, levels, vs30)
        binned_hazard_curves.to_json(f"./df_{key}_aggregates.json")
        hazard_curves = pd.concat([hazard_curves, binned_hazard_curves])

    toc = time.perf_counter()
    print(f'agg time: {toc-tic:.1f} seconds')
    print(f'total imts: {len(imts)}')
    print(f'total locations: {len(locs)}')
    print(f'total aggregations: {len(aggs)}')
    print(f'total levels: {len(levels)}')
    print(f'total time: {toc-tic_total:.1f} seconds')

    print(hazard_curves)
    """
