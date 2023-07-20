#! hazard_logic_tree.py

"""
Classes to define hazard logic trees and gather openquake run info from Toshi
"""
import itertools
from dataclasses import dataclass, field
from functools import reduce
from math import isclose
from operator import mul
from typing import Any, Dict, List

from nzshm_model.source_logic_tree.logic_tree import CompositeBranch, FlattenedSourceLogicTree

from toshi_hazard_post.toshi_api_support import SourceSolutionMap, toshi_api

DTOL = 1.0e-6


@dataclass
class GMCMBranch:
    # gmms: List[str]
    realizations: List[str]  # [int] or [str]?
    weight: float


# @dataclass
# class SourceBranch:
#     name: str
#     tags: List[str]
#     weight: float
#     toshi_hazard_ids: List[str]
#     inv_ids: List[str] = field(default_factory=lambda: [])
#     bg_ids: List[str] = field(default_factory=lambda: [])
#     gmcm_branches: List[GMCMBranch] = field(default_factory=lambda: [])

#     @property  # type: ignore
#     def gmcm_branch_weights(self) -> List[float]:
#         return [branch.weight for branch in self.gmcm_branches]

#     @property  # type: ignore
#     def n_gmcm_branches(self) -> int:
#         return len(self.gmcm_branches)

#     @property  # type: ignore
#     def gmcm_branch_realizations(self) -> List[List[str]]:
#         return [branch.realizations for branch in self.gmcm_branches]


# @dataclass
# class SourceBranchGroup(MutableSequence):
#     _branches: List[SourceBranch] = field(default_factory=lambda: [])

#     def __getitem__(self, i):
#         return self._branches[i]

#     def __setitem__(self, i, item):
#         self._branches[i] = item

#     def __delitem__(self, i):
#         del self._branches[i]

#     def __len__(self):
#         return len(self._branches)

#     def insert(self, i, item):
#         self._branches.insert(i, item)


@dataclass
class HazardBranch:
    """Replaces Source Branch"""

    source_branch: CompositeBranch
    hazard_ids: List[str]
    gmcm_branches: List[GMCMBranch] = field(default_factory=list)

    @property
    def gmcm_branch_weights(self):
        return [branch.weight for branch in self.gmcm_branches]

    @property
    def branch_realizations(self):
        return [branch.realizations for branch in self.gmcm_branches]

    @property
    def weight(self):
        return self.source_branch.weight

    def set_gmcm_branches(self, metadata: Dict[str, dict], correlations: List[List[str]]) -> None:  # noqa: C901
        """
        Build the table of ground motion combinations and weights for a single source branch.
        Assumes one source branch per run and the same gsim weights in every run. Can enforce correlations of ground
        motion models.

        Parameters
        ----------
        branch
            single source branch definition (from build_full_source_lt() )
        metadata
            GMCM logic tree metadata (from preload_meta() )
        correlations
            GMCM logic tree correlations, each inner list element contains two ground motion model strings to correlate.
        """

        if correlations:
            correlation_master = [corr[0] for corr in correlations]
            correlation_puppet = [corr[1] for corr in correlations]
        else:
            correlation_master = []
            correlation_puppet = []

        rlz_sets: Dict[str, Any] = {}
        weight_sets: Dict[str, Any] = {}
        # trts = set()

        for hazard_id in self.hazard_ids:
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
                    weight_combs.append(reduce(mul, weight_group, 1.0))
            else:
                rlz_combs.append([s for src in src_group for s in src])
                weight_combs.append(reduce(mul, weight_group, 1.0))

        sum_weight = sum(weight_combs)
        # if not ((sum_weight > 1.0 - DTOL) & (sum_weight < 1.0 + DTOL)):
        if not isclose(sum_weight, 1.0):
            print(sum_weight)
            raise Exception('weights do not sum to 1')

        gmcm_branches = []
        for rlz_comb, weight in zip(rlz_combs, weight_combs):
            gmcm_branches.append(
                GMCMBranch(
                    # [rlz.split(':')[-1] for rlz in rlz_comb],
                    rlz_comb,
                    weight,
                )
            )
        # TODO: record mapping between rlz number and gmm name
        # return gmcm_branches
        self.gmcm_branches = gmcm_branches


@dataclass
class HazardLogicTree:
    """replaces SourceBranchGroup"""

    # TODO: unique mapping for each vs30 so that gtIDs from different vs30 runs can be combined

    name: str
    gt_ids: List[str]
    branches: List[HazardBranch] = field(default_factory=list)

    @property
    def hazard_ids(self):
        toshi_ids = [id for branch in self.branches for id in branch.hazard_ids]
        return list(set(toshi_ids))

    @classmethod
    def from_flattened_slt(cls, flat_slt: FlattenedSourceLogicTree, gt_ids: List[str]):
        source_solution_map = SourceSolutionMap()
        for id in gt_ids:
            source_solution_map.append(toshi_api.get_hazard_gt(id))

        def yield_haz_branches(branches):
            for comp_branch in branches:
                hazard_ids = []
                for branch in comp_branch.branches:
                    hazard_id = source_solution_map.get_solution_id(
                        onfault_nrml_id=branch.onfault_nrml_id, distributed_nrml_id=branch.distributed_nrml_id
                    )
                    hazard_ids.append(hazard_id) if hazard_id else None
                yield HazardBranch(comp_branch, hazard_ids)

        return cls(flat_slt.title, gt_ids, list(yield_haz_branches(flat_slt.branches)))

    def __repr__(self):
        return f'{self.__class__} name: {self.name} number of branches: {len(self.branches)}'
