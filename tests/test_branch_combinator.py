"""Test toshi_hazard_post.branch_combinator core functions."""

import json
import unittest
from functools import reduce
from operator import mul
from pathlib import Path
import pytest

from toshi_hazard_post.branch_combinator import get_weighted_branches, grouped_ltbs, merge_ltbs_fromLT


class TestCombinator(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches.json')
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_cor.json')

    def test_build_source_branches(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))

        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit))
        source_branches = get_weighted_branches(grouped)  # TODO: add correlations to source LT

        # print(source_branches)

        # test function output against precanned result
        expected = json.load(open(self._sb_file, 'r'))
        assert expected == source_branches

        # now test some specifics
        perms = []
        for bunch in logic_tree_permutations[0]:
            for group in bunch['permute']:
                perms.append(len(group['members']))

        print(perms)
        assert len(source_branches) == reduce(mul, perms, 1)

        # weights sum to 1
        total_weight = sum([branch['weight'] for branch in source_branches])
        assert total_weight == pytest.approx(1.0)


class TestCorrelatedCombinator(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches_correlated.json')
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_cor.json')

    def test_build_correlated_source_branches(self):

        ltb_data = json.load(open(self._ltb_file, 'r'))

        correlations = ltb_data['correlations']
        logic_tree_permutations = ltb_data['logic_tree_permutations']
        gtdata = ltb_data['hazard_solutions']
        omit = []

        grouped = grouped_ltbs(merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit))
        source_branches = get_weighted_branches(grouped, correlations)  # TODO: add correlations to source LT

        # test that we get the correct ids
        expected = json.load(open(self._sb_file, 'r'))
        expected_ids = [set(exp['ids']) for exp in expected]
        received_ids = [set(sb['ids']) for sb in source_branches]
        assert expected_ids == received_ids

        # test individual weights
        for i, branch in enumerate(source_branches):
            assert branch['weight'] == pytest.approx(expected[i]['weight'])

        # weights sum to 1
        total_weight = sum([branch['weight'] for branch in source_branches])
        assert total_weight == pytest.approx(1.0)
