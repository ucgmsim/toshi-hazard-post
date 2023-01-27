"""Test toshi_hazard_post.branch_combinator core functions."""

import json
import unittest
from functools import reduce
from operator import mul
from pathlib import Path
import pytest
from dacite import from_dict

from toshi_hazard_post.logic_tree.branch_combinator import (
    build_full_source_lt,
    grouped_ltbs,
    merge_ltbs_fromLT,
    build_rlz_table,
    SourceBranch,
    GMCMBranch,
    SourceBranchGroup,
)


def load_gmcm_branches(filepath):
    with open(filepath) as json_file:
        gmcm_branches_asdict = json.load(json_file)

    gmcm_branches = []
    for branch_dict in gmcm_branches_asdict:
        gmcm_branches.append(GMCMBranch(**branch_dict))
    return gmcm_branches


def convert_source_branches(source_branches_old):

    source_branches = SourceBranchGroup()
    for branch in source_branches_old:
        source_branches.append(
            SourceBranch(
                branch['name'],
                branch['tags'],
                branch['weight'],
                branch['ids'],
            )
        )
    return source_branches


def convert_gmcm_branches(rlz_combs, weight_combs):

    gmcm_branches = []
    for rlz_comb, weight in zip(rlz_combs, weight_combs):
        gmcm_branches.append(
            GMCMBranch(
                # [],
                # [rlz.split(':')[-1] for rlz in rlz_comb],
                rlz_comb,
                weight,
            )
        )

    return gmcm_branches


class TestCombinator(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches.json')
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_cor.json')
        self._vs30 = 400

    def test_build_full_source_lt(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))

        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), self._vs30)
        source_branches = build_full_source_lt(grouped)  # TODO: add correlations to source LT

        # print(source_branches)

        # test function output against precanned result
        # expected = convert_source_branches(json.load(open(self._sb_file, 'r')))
        expected = from_dict(data_class=SourceBranchGroup, data=json.load(open(self._sb_file, 'r')))
        assert expected == source_branches

        # now test some specifics
        perms = []
        for bunch in logic_tree_permutations[0]:
            for group in bunch['permute']:
                perms.append(len(group['members']))

        print(perms)
        assert len(source_branches) == reduce(mul, perms, 1)

        # weights sum to 1
        total_weight = sum([branch.weight for branch in source_branches])
        assert total_weight == pytest.approx(1.0)

    def test_merge_ltbs_fromLT(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))

        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        merged_ltbs = list(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit))

        assert merged_ltbs[0].vs30 == 400


class TestCorrelatedCombinator(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches_correlated.json')
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_cor.json')
        self._vs30 = 400

    def test_build_full_source_lt_correlated(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))

        correlations = ltb_data['src_correlations']
        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), self._vs30)
        source_branches = build_full_source_lt(grouped, correlations)  # TODO: add correlations to source LT

        # test that we get the correct ids
        expected = from_dict(data_class=SourceBranchGroup, data=json.load(open(self._sb_file, 'r')))

        assert expected == source_branches

        expected_ids = [set(exp.toshi_hazard_ids) for exp in expected]
        received_ids = [set(sb.toshi_hazard_ids) for sb in source_branches]
        assert expected_ids == received_ids

        # test individual weights
        for i, branch in enumerate(source_branches):
            assert branch.weight == pytest.approx(expected[i].weight)

        # weights sum to 1
        total_weight = sum([branch.weight for branch in source_branches])
        assert total_weight == pytest.approx(1.0)


class TestGroupedLTBs(unittest.TestCase):
    def setUp(self):
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_vs30.json')

    def test_build_full_source_lt_correlated_vs30(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))
        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        gltb_150 = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), 150)
        hazard_ids_150 = [g.hazard_solution_id for group in gltb_150.values() for g in group]
        assert all(['150_' in id for id in hazard_ids_150])

        gltb_400 = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit), 400)
        hazard_ids_400 = [g.hazard_solution_id for group in gltb_400.values() for g in group]
        assert all(['400_' in id for id in hazard_ids_400])


class TestBuldRealizationTable(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches_correlated.json')
        self._metadata_filepath = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'metadata.json')
        self._gmcm_branches_filepath = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'gmcm_branches.json')

    def test_build_rlz_table(self):

        metadata = json.load(open(self._metadata_filepath, 'r'))
        # source_branches = convert_source_branches(json.load(open(self._sb_file, 'r')))
        source_branches = from_dict(data_class=SourceBranchGroup, data=json.load(open(self._sb_file, 'r')))

        gmcm_branches = build_rlz_table(source_branches[0], metadata)

        gmcm_branches_expected = load_gmcm_branches(self._gmcm_branches_filepath)

        assert len(gmcm_branches) == len(gmcm_branches_expected)
        assert sum([branch.weight for branch in gmcm_branches]) == pytest.approx(1.0)

        for branch_expected in gmcm_branches_expected:
            assert branch_expected in gmcm_branches


class TestCorrelatiedRealizationTable(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches_correlated.json')
        self._metadata_filepath = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'metadata.json')
        self._gmcm_branches_filepath = Path(
            Path(__file__).parent, 'fixtures/branch_combinator', 'gmcm_branches_correlated.json'
        )

    def test_build_correlated_rlz_table(self):

        correlations = [
            (
                '[AbrahamsonGulerce2020SInter]\nregion = "GLO"\nsigma_mu_epsilon = -1.28155',
                '[AbrahamsonGulerce2020SSlab]\nregion = "GLO"\nsigma_mu_epsilon = -1.28155',
            ),
            (
                '[AbrahamsonGulerce2020SInter]\nregion = "GLO"\nsigma_mu_epsilon = 0.0',
                '[AbrahamsonGulerce2020SSlab]\nregion = "GLO"\nsigma_mu_epsilon = 0.0',
            ),
            (
                '[AbrahamsonGulerce2020SInter]\nregion = "GLO"\nsigma_mu_epsilon = 1.28155',
                '[AbrahamsonGulerce2020SSlab]\nregion = "GLO"\nsigma_mu_epsilon = 1.28155',
            ),
            ('[Atkinson2022SInter]\nepistemic = "Central"', '[Atkinson2022SSlab]\nepistemic = "Central"'),
            ('[Atkinson2022SInter]\nepistemic = "Lower"', '[Atkinson2022SSlab]\nepistemic = "Lower"'),
            ('[Atkinson2022SInter]\nepistemic = "Upper"', '[Atkinson2022SSlab]\nepistemic = "Upper"'),
            (
                '[KuehnEtAl2020SInter]\nregion = "GLO"\nsigma_mu_epsilon = 0.0',
                '[KuehnEtAl2020SSlab]\nregion = "GLO"\nsigma_mu_epsilon = 0.0',
            ),
            ('[ParkerEtAl2020SInter]', '[ParkerEtAl2020SSlab]'),
        ]

        metadata = json.load(open(self._metadata_filepath, 'r'))
        # source_branches = convert_source_branches(json.load(open(self._sb_file, 'r')))
        source_branches = from_dict(data_class=SourceBranchGroup, data=json.load(open(self._sb_file, 'r')))
        gmcm_branches = build_rlz_table(source_branches[0], metadata, correlations)

        gmcm_branches_expected = load_gmcm_branches(self._gmcm_branches_filepath)

        assert sum([branch.weight for branch in gmcm_branches]) == pytest.approx(1.0)
        assert len(gmcm_branches) == len(gmcm_branches_expected)

        for branch_expected in gmcm_branches_expected:
            assert branch_expected in gmcm_branches
