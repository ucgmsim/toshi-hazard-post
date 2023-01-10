"""Test aggregatio core binning aggregation. """

import json
import unittest
from pathlib import Path
import numpy as np

from toshi_hazard_post.hazard_aggregation.aggregate_rlzs import (
    weighted_stats,
    calc_weighted_sum,
    get_branch_weights,
    get_len_rate,
    build_branches,
    calculate_aggs,
)


class TestAggStats(unittest.TestCase):
    def setUp(self):
        self._stats_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'quantiles_expected.npy')
        self._probs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        self._aggs = ["0.1", "0.5", "mean", "cov", "std", "0.9"]
        self._weights = [1, 2, 1, 4, 1, 2, 2, 3, 3]

    def test_weighted_stats(self):

        stats = weighted_stats(self._probs, self._aggs, self._weights)
        stats_expected = np.load(self._stats_file)

        assert np.allclose(stats, stats_expected)


class TestProb(unittest.TestCase):
    def setUp(self):
        self._prob_sum_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'weighted_sum.npy')
        self._weighted_sum_args_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'args.json')
        self._values_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'values.npy')
        self._rlz_combs_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'rlz_combs.json')

    def test_calc_weighted_sum(self):

        rlz_combs = json.load(open(self._rlz_combs_file))
        values = np.load(self._values_file, allow_pickle=True)[()]
        args = json.load(open(self._weighted_sum_args_file))
        prob_sum = calc_weighted_sum(rlz_combs, values, args['loc'], args['imt'], args['start_ind'], args['end_ind'])

        expected = np.load(self._prob_sum_file)

        assert np.allclose(prob_sum, expected)


class TestBranchFuns(unittest.TestCase):
    def setUp(self):
        self._weighted_sum_args_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'args.json')
        self._source_branches_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'source_branches.json')
        self._aggs_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'aggs.json')

        self._wights_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'weights.npy')
        self._values_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'values.npy')
        self._branch_probs_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'branch_probs.npy')
        self._agg_probs_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'aggregate_probs.npy')

        self._source_branches = json.load(open(self._source_branches_file))
        self._values = np.load(self._values_file, allow_pickle=True)[()]

    def test_get_branch_weights(self):

        weights = get_branch_weights(self._source_branches)

        expected = np.load(self._wights_file)

        assert np.allclose(weights, expected)

    def test_get_len_rate(self):

        ncols = get_len_rate(self._values)

        assert ncols == 44

    def test_build_branches(self):

        args = json.load(open(self._weighted_sum_args_file))
        breakpoint()
        branch_probs = build_branches(
            self._source_branches,
            self._values,
            args['imt'],
            args['loc'],
            args['vs30'],
            args['start_ind'],
            args['end_ind'],
        )

        start = 10
        stop = 20
        branch_probs_10_20 = build_branches(
            self._source_branches, self._values, args['imt'], args['loc'], args['vs30'], start, stop
        )

        expected = np.load(self._branch_probs_file)

        assert np.allclose(branch_probs, expected)
        assert np.allclose(branch_probs_10_20, expected[:, start:stop])

    def test_calculate_aggs(self):

        expected = np.load(self._agg_probs_file)
        branch_probs = np.load(self._branch_probs_file)
        aggs = json.load(open(self._aggs_file))
        weights = np.load(self._wights_file)
        aggregate_probs = calculate_aggs(branch_probs, aggs, weights)

        assert np.allclose(aggregate_probs, expected)
