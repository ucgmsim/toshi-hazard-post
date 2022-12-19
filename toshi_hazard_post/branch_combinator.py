import ast
import itertools
import json
import logging
import math
from collections import namedtuple

from functools import reduce
from operator import mul

from toshi_hazard_store.query_v3 import get_hazard_metadata_v3


DTOL = 1.0e-6

log = logging.getLogger(__name__)


def get_branches():
    assert 0


def preload_meta(ids, vs30):

    metadata = {}
    for meta in get_hazard_metadata_v3(ids, [vs30]):
        hazard_id = meta.hazard_solution_id
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        metadata[hazard_id] = gsim_lt

    return metadata


def build_rlz_table(branch, metadata, correlations=None):
    """
    build the table of ground motion combinations and weights for a single source branch
    assumes only one source per run and the same gsim weights in every run
    """

    if correlations:
        correlation_master = [corr[0] for corr in correlations]
        correlation_puppet = [corr[1] for corr in correlations]
    else:
        correlation_master = []
        correlation_puppet = []

    rlz_sets = {}
    weight_sets = {}
    trts = set()

    for hazard_id in branch['ids']:
        gsim_lt = metadata[hazard_id]
        trts = set(gsim_lt['trt'].values())
        for trt in trts:
            if not rlz_sets.get(trt):
                rlz_sets[trt] = {}
                weight_sets[trt] = {}
            for rlz, gsim in gsim_lt['uncertainty'].items():
                rlz_key = ':'.join((hazard_id, rlz))
                if not rlz_sets[trt].get(gsim):
                    rlz_sets[trt][gsim] = []
                rlz_sets[trt][gsim].append(rlz_key)
                weight_sets[trt][gsim] = 1 if gsim in correlation_puppet else gsim_lt['weight'][rlz]

    # find correlated gsims and mappings between gsim name and rlz_key

    if correlations:
        all_rlz = [(gsim, rlz) for rlz_set in rlz_sets.values() for gsim, rlz in rlz_set.items()]
        correlation_list = []
        all_rlz_copy = all_rlz.copy()
        for rlzm in all_rlz:
            for i, cm in enumerate(correlation_master):
                if cm == rlzm[0]:
                    correlation_list.append(rlzm[1].copy())
                    for rlzp in all_rlz_copy:
                        if correlation_puppet[i] == rlzp[0]:
                            correlation_list[-1] += rlzp[1]

    rlz_sets_tmp = rlz_sets.copy()
    weight_sets_tmp = weight_sets.copy()
    for k, v in rlz_sets.items():
        rlz_sets_tmp[k] = []
        weight_sets_tmp[k] = []
        for gsim in v.keys():
            rlz_sets_tmp[k].append(v[gsim])
            weight_sets_tmp[k].append(weight_sets[k][gsim])

    rlz_lists = list(rlz_sets_tmp.values())
    weight_lists = list(weight_sets_tmp.values())

    # TODO: fix rlz from the same ID grouped together
    rlz_iter = itertools.product(*rlz_lists)
    weight_iter = itertools.product(*weight_lists)
    rlz_combs = []
    weight_combs = []

    for src_group, weight_group in zip(rlz_iter, weight_iter):
        if correlations:
            foo = [s for src in src_group for s in src]
            if any([len(set(foo).intersection(set(cl))) == len(cl) for cl in correlation_list]):
                rlz_combs.append(foo)
                weight_combs.append(reduce(mul, weight_group, 1))
        else:
            rlz_combs.append([s for src in src_group for s in src])
            weight_combs.append(reduce(mul, weight_group, 1))

    sum_weight = sum(weight_combs)
    if not ((sum_weight > 1.0 - DTOL) & (sum_weight < 1.0 + DTOL)):
        print(sum_weight)
        raise Exception('weights do not sum to 1')

    return rlz_combs, weight_combs, rlz_sets


def build_source_branches(
    logic_tree_permutations, gtdata, src_correlations, gmm_correlations, vs30, omit, toshi_ids, truncate=None
):
    """ported from THS. aggregate_rlzs_mp"""
    # pr.enable()

    grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), vs30)

    source_branches = get_weighted_branches(grouped, src_correlations)

    if truncate:
        # for testing only
        source_branches = source_branches[:truncate]

    metadata = preload_meta(toshi_ids, vs30)

    for i in range(len(source_branches)):
        rlz_combs, weight_combs, rlz_sets = build_rlz_table(
            source_branches[i], metadata, gmm_correlations
        )  # TODO: add correlations to GMCM LT
        source_branches[i]['rlz_combs'] = rlz_combs
        source_branches[i]['weight_combs'] = weight_combs
        source_branches[i]['rlz_sets'] = rlz_sets

    # pr.disable()
    # pr.print_stats(sort='time')

    return source_branches


def get_weighted_branches(grouped_ltbs, correlations=None):
    """build source branches"""

    # TODO: only handles one combined job and one permutation set
    permute = grouped_ltbs  # tree_permutations[0][0]['permute']

    # turn off correlations if there is a missing group
    if correlations:
        group1 = set([cr[0]['group'] for cr in correlations['correlations']])
        group2 = set([cr[1]['group'] for cr in correlations['correlations']])
        if not all([gr in grouped_ltbs.keys() for gr in group1.union(group2)]):
            correlations = None

    # check that each permute group weights sum to 1.0
    for key, group in permute.items():
        group_weight = 0
        for member in group:
            group_weight += member.weight
        if (group_weight < 1.0 - DTOL) | (group_weight > 1.0 + DTOL):
            log.error('Group: %s has items: %s and weight: %s' % (key, len(group), group_weight))
            log.error('group: %s' % (group))
            raise Exception(f'group {key} weight does not sum to 1.0 ({group_weight}')

    # do the thing
    id_groups = []
    for key, group in permute.items():
        id_group = []
        for member in group:
            id_group.append(
                {'id': member.hazard_solution_id, 'weight': member.weight, 'tag': member.tag, 'group': member.group}
            )
        id_groups.append(id_group)

    branches = itertools.product(*id_groups)
    source_branches = []
    for i, branch in enumerate(branches):
        name = str(i)
        ids = [leaf['id'] for leaf in branch]
        group_and_tags = [{'group': leaf['group'], 'tag': leaf['tag']} for leaf in branch]
        tags = [leaf['tag'] for leaf in branch]

        if correlations:
            for correlation in correlations['correlations']:
                if all(cor in group_and_tags for cor in correlation):
                    weights = [leaf['weight'] for leaf in branch if leaf['group'] != correlations['dropped_group']]
                    weight = math.prod(weights)
                    branch_dict = dict(name=name, ids=ids, weight=weight, tags=tags)
                    source_branches.append(branch_dict)
                    break
        else:
            weights = [leaf['weight'] for leaf in branch]
            weight = math.prod(weights)
            branch_dict = dict(name=name, ids=ids, weight=weight, tags=tags)
            source_branches.append(branch_dict)

    return source_branches


Member = namedtuple("Member", "group tag weight inv_id bg_id hazard_solution_id vs30")


def weight_and_ids(data):
    def get_tag(args):
        for arg in args:
            if arg['k'] == "logic_tree_permutations":
                return json.loads(arg['v'].replace("'", '"'))[0]['permute']  # ['members'][0]
        assert 0

    def get_vs30(args):
        for arg in args:
            if arg['k'] == "vs30":
                return int(float(arg['v']))
        assert 0

    nodes = data['data']['node1']['children']['edges']
    for obj in nodes:
        if obj['node']['child']['hazard_solution']:
            tag = get_tag(obj['node']['child']['arguments'])
            vs30 = get_vs30(obj['node']['child']['arguments'])
            hazard_solution_id = obj['node']['child']['hazard_solution']['id']
            yield Member(**tag[0]['members'][0], group=None, hazard_solution_id=hazard_solution_id, vs30=vs30)


def all_members_dict(ltbs):
    """LTBS from ther toshi GT - NB some may be failed jobs..."""
    res = {}

    def members():
        for grp in ltbs[0][0]['permute']:
            # print(grp['group'])
            for m in grp['members']:
                yield Member(**m, group=grp['group'], hazard_solution_id=None, vs30=None)

    for m in members():
        res[f'{m.inv_id}{m.bg_id}'] = m
    return res


def merge_ltbs(logic_tree_permutations, gtdata, omit):
    members = all_members_dict(logic_tree_permutations)
    # weights are the actual Hazard weight @ 1.0
    for toshi_ltb in weight_and_ids(gtdata):
        if toshi_ltb.hazard_solution_id in omit:
            log.debug(f'skipping {toshi_ltb}')
            continue
        d = toshi_ltb._asdict()
        d['weight'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].weight
        d['group'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].group
        yield Member(**d)


def merge_ltbs_fromLT(logic_tree_permutations, gtdata, omit):
    """map source IDs in LT definition to Hazard IDs from GT"""
    members = all_members_dict(logic_tree_permutations)
    # weights are the actual Hazard weight @ 1.0
    for toshi_ltb in weight_and_ids(gtdata):
        if toshi_ltb.hazard_solution_id in omit:
            log.debug(f'skipping {toshi_ltb}')
            continue
        if members.get(f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'):
            d = toshi_ltb._asdict()
            d['weight'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].weight
            d['group'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].group
            d['tag'] = members[f'{toshi_ltb.inv_id}{toshi_ltb.bg_id}'].tag
            yield Member(**d)


def grouped_ltbs(merged_ltbs, vs30):
    """groups by trt"""
    grouped = {}
    for ltb in merged_ltbs:
        if ltb.vs30 == vs30:
            if ltb.group not in grouped:
                grouped[ltb.group] = []
            grouped[ltb.group].append(ltb)
    return grouped
