"""Test toshi_hazard_post.branch_combinator core functions."""

import json
import unittest
from functools import reduce
from operator import mul
from pathlib import Path

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


class TestCorrelatedCombinator(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches_correlated.json')
        self._ltb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'SLT_test_cor.json')

    @unittest.skip('WIP')
    def test_build_correlated_source_branches(self):

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
