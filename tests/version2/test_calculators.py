import json
import unittest
from pathlib import Path

import numpy as np
import pytest

from toshi_hazard_post.version2.calculators import (
    calculate_weighted_quantiles,
    prob_to_rate,
    rate_to_prob,
    weighted_avg_and_std,
)


class TestProbRate(unittest.TestCase):
    def setUp(self):
        self._probs = np.arange(0.1, 1.0, 0.1)
        self._inv_time = 5.5

        self._rates_file = Path(Path(__file__).parent, 'fixtures/calculators', 'rates.json')
        self._rates = np.array(json.load(open(self._rates_file)))

    def test_prob_to_rate(self):

        rates = prob_to_rate(self._probs, self._inv_time)

        assert rates.shape == self._rates.shape
        assert np.allclose(rates, self._rates)

    def test_rate_to_prob(self):

        probs = rate_to_prob(self._rates, self._inv_time)

        assert probs.shape == self._probs.shape
        assert np.allclose(probs, self._probs)


class TestMeanStd(unittest.TestCase):
    def setUp(self):
        self._weights_values_file = Path(Path(__file__).parent, 'fixtures/calculators', 'weights_and_values.json')
        self._mean_expected = 4.48577620473112
        self._std_expected = 2.6294520822489

        w_and_v = json.load(open(self._weights_values_file))
        self._weights = np.array(w_and_v['weights'])
        self._values = np.array(w_and_v['values'])

    def test_weighted_avg_and_std(self):

        mean, std = weighted_avg_and_std(self._values, self._weights)

        assert mean == pytest.approx(self._mean_expected)
        assert std == pytest.approx(self._std_expected)


class TestQuantiles(unittest.TestCase):
    def setUp(self):
        self._weights_values_file = Path(Path(__file__).parent, 'fixtures/calculators', 'weights_and_values.json')

        w_and_v = json.load(open(self._weights_values_file))
        self._weights = np.array(w_and_v['weights'])
        self._values = np.array(w_and_v['values'])

    def test_calculate_quantiles(self):

        quantiles = [0.5]
        values = np.array((1, 2, 3, 4, 5))
        weights = np.array((1, 0, 0, 1, 1))
        weighted_quantiles = calculate_weighted_quantiles(np.array(values), np.array(weights), quantiles)
        weighted_quantiles_expeced = [4]

        assert np.allclose(weighted_quantiles, weighted_quantiles_expeced)
