"""Test aggregatio core binning aggregation. """

import json
import unittest
from pathlib import Path
import numpy as np
import pytest

from dacite import from_dict

from toshi_hazard_post.logic_tree.logic_tree import GMCMBranch, HazardLogicTree


from toshi_hazard_post.data_functions import ValueStore
from toshi_hazard_post.hazard_aggregation.aggregate_rlzs import (
    weighted_stats,
    calc_weighted_sum,
    get_branch_weights,
    build_branches,
    calculate_aggs,
)


def convert_values(values_dict):
    values = ValueStore()
    for key, vd1 in values_dict.items():
        for loc, vd2 in vd1.items():
            for imt, vals in vd2.items():
                values.set_values(value=vals, key=key, loc=loc, imt=imt)
    return values


class TestAggStats(unittest.TestCase):
    def setUp(self):
        self._stats_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'quantiles_expected.npy')
        self._probs = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
        self._aggs = ["0.1", "0.5", "mean", "cov", "std", "0.9"]
        self._weights = np.array([1, 2, 1, 4, 1, 2, 2, 3, 3])

    def test_weighted_stats(self):

        stats = weighted_stats(self._probs, self._aggs, self._weights)
        stats_expected = np.load(self._stats_file)

        assert np.allclose(stats, stats_expected)


def generate_values():
    import numpy as np

    rng = np.random.default_rng(12345)

    values = ValueStore()
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_0:0",
        loc='WLG',
        imt='PGA',
    )
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_0:1",
        loc='WLG',
        imt='PGA',
    )
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_0:2",
        loc='WLG',
        imt='PGA',
    )
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_1:0",
        loc='WLG',
        imt='PGA',
    )
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_1:1",
        loc='WLG',
        imt='PGA',
    )

    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_3:0",
        loc='WLG',
        imt='PGA',
    )
    values.set_values(
        value=rng.random(
            10,
        ),
        key="hazsol_3:1",
        loc='WLG',
        imt='PGA',
    )

    return values


def generate_gmcm_branches():

    gmcm_branches = []
    realizations = [
        ["hazsol_0:0", "hazsol_1:0"],
        ["hazsol_0:0", "hazsol_1:1"],
    ]

    weights = np.array(
        [
            0.1,
            0.2,
        ]
    )

    for rlz, weight in zip(realizations, weights):
        gmcm_branches.append(GMCMBranch(rlz, weight))

    return gmcm_branches


class TestProb(unittest.TestCase):
    def setUp(self):
        self._weighted_sum_filepath = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'weighted_sum.npy')

    def test_calc_weighted_sum(self):

        values = generate_values()
        gmcm_branches = generate_gmcm_branches()
        prob_sum = calc_weighted_sum(gmcm_branches, values, 'WLG', 'PGA', 2, 8)
        expected = np.load(self._weighted_sum_filepath)

        assert np.allclose(prob_sum, expected)

        start_ind = 3
        end_ind = 7
        prob_sum = calc_weighted_sum(gmcm_branches, values, 'WLG', 'PGA', start_ind, end_ind)
        assert prob_sum.shape[1] == (end_ind - start_ind)


class TestBranchFunctions(unittest.TestCase):
    def setUp(self):
        logic_tree_filepath = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'logic_tree.json')
        branch_probs_filepath = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'branch_probs.npy')
        self._hazard_aggs_filepath = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'hazard_agg.npy')
        self._branch_weights_filepath = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'branch_weights.npy')

        self._logic_tree = from_dict(data_class=HazardLogicTree, data=json.load(open(logic_tree_filepath)))
        self._branch_probs = np.load(branch_probs_filepath)

    def test_build_branches(self):

        values = generate_values()
        imt = 'PGA'
        loc = 'WLG'
        start_ind = 2
        end_ind = 7
        branch_probs = build_branches(self._logic_tree, values, imt, loc, start_ind, end_ind)

        assert branch_probs.shape[1] == end_ind - start_ind
        assert np.allclose(branch_probs, self._branch_probs)

    def test_calculate_aggs(self):

        weights = np.array([0.1, 0.1, 0.2, 0.3, 0.1, 0.2])
        aggs = ['mean', 'std', 'cov', '0.6']
        hazard_agg = calculate_aggs(self._branch_probs, aggs, weights)
        expected = np.load(self._hazard_aggs_filepath)

        assert np.allclose(hazard_agg, expected)

    def test_get_branch_weights(self):

        branch_weights = get_branch_weights(self._logic_tree)
        expected = np.load(self._branch_weights_filepath)

        assert np.allclose(branch_weights, expected)
