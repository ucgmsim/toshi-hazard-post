import unittest
import json
from pathlib import Path
from unittest import mock

import numpy as np

import toshi_hazard_post.hazard_aggregation.aggregation
from toshi_hazard_post.hazard_aggregation.aggregation import AggTaskArgs


def batch_write():
    class Saver:
        def save():
            return True

    return Saver()


values = np.load(Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'values.npy'), allow_pickle=True)[()]


@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.load_realization_values')
@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.HazardAggregation')
@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.LevelValuePairAttribute')
class TestAggregation(unittest.TestCase):
    def setUp(self):
        self._task_args_file = Path(Path(__file__).parent, 'fixtures/aggregation', 'task_args.json')
        self._values_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'values.npy')
        self._lvls_file = Path(Path(__file__).parent, 'fixtures/aggregation', 'lvls_expected.npy')
        self._vals_file = Path(Path(__file__).parent, 'fixtures/aggregation', 'vals_expected.npy')
        self._kwargs_file = Path(Path(__file__).parent, 'fixtures/aggregation', 'hazard_agg_kwargs.json')

    def test_process_location_list(self, mock_lvl_val, mock_hazard_agg, mock_load):

        mock_load.return_value = np.load(self._values_file, allow_pickle=True)[()]
        lvls_expected = np.load(self._lvls_file)
        vals_expected = np.load(self._vals_file)
        kwargs_expected = json.load(open(self._kwargs_file))
        n_lvl_vals_expected = len(lvls_expected)
        task_args = AggTaskArgs(*json.load(open(self._task_args_file)))

        toshi_hazard_post.hazard_aggregation.aggregation.process_location_list(task_args)

        mock_load.assert_called()
        assert len(mock_lvl_val.mock_calls) == n_lvl_vals_expected

        lvls = np.empty(
            n_lvl_vals_expected,
        )
        vals = np.empty(
            n_lvl_vals_expected,
        )
        for i in range(n_lvl_vals_expected):
            lvls[i] = mock_lvl_val.mock_calls[0].kwargs['lvl']
            vals[i] = mock_lvl_val.mock_calls[0].kwargs['val']

        assert np.allclose(lvls, lvls_expected)
        assert np.allclose(vals, vals_expected)

        kwds = ['vs30', 'imt', 'agg', 'hazard_model_id']
        for ind, expected in kwargs_expected.items():
            kwargs = {k: v for k, v in mock_hazard_agg.mock_calls[int(ind)].kwargs.items() if k in kwds}
            assert kwargs == expected
