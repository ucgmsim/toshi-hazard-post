import unittest
import json
from pathlib import Path
from unittest import mock
from unittest.mock import ANY

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
# @mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.batch')
# @mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.HazardAggregation.batch_write().save', new_callable=mock.mock_open)
# @mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.HazardAggregation.batch_write().save')
# @mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.HazardAggregation.save')
@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation.model.HazardAggregation.batch_write')
class TestAggregation(unittest.TestCase):
    def setUp(self):
        self._task_args_file = Path(Path(__file__).parent, 'fixtures/aggregation', 'task_args.json')
        self._values_file = Path(Path(__file__).parent, 'fixtures/aggregate_rlz', 'values.npy')


    def test_process_location_list(self, mock_ths, mock_load):
    # def test_process_location_list(self, mock_load):

        mock_load.return_value = np.load(self._values_file, allow_pickle=True)[()]
        # mock_ths.HazardAggregation.batch_write.__enter__.return_value = batch_write

     
        task_args = AggTaskArgs(*json.load(open(self._task_args_file)))

        toshi_hazard_post.hazard_aggregation.aggregation.process_location_list(task_args)

        mock_load.assert_called()
        breakpoint()

        # for agg in task_args.aggs:
        #     requests_arguments = {
        #         'values': ANY,
        #         'vs30': 400,
        #         'imt': 'PGA',
        #         'agg': agg,
        #         'hazard_model_id': 'TEST'
        #     }
        #     mock_ths.HazardAggregation.assert_any_call(**requests_arguments)
