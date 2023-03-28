from unittest import mock
import unittest
from pathlib import Path
import json

from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig
from toshi_hazard_post.hazard_aggregation.deaggregation import get_deagg_gtids


index_filepath = Path(Path(__file__).parent, 'fixtures', 'deaggregation', 'index.json')
with open(index_filepath) as index_file:
    index = json.load(index_file) 

@mock.patch('toshi_hazard_post.hazard_aggregation.deaggregation.urllib.request')
@mock.patch('toshi_hazard_post.hazard_aggregation.deaggregation.get_index_from_s3', return_value = index)
class testGetGts(unittest.TestCase):

    def setUp(self):
        
        config_filepath = Path(Path(__file__).parent, 'fixtures', 'deaggregation', 'config.toml')
        self._config = AggregationConfig(config_filepath)

    def test_get_deagg_gts(self, mock_index, mock_requests):

        gt_ids = get_deagg_gtids(self._config)

        assert len(gt_ids) == 1
        assert gt_ids[0] == 'R2VuZXJhbFRhc2s6MTMzMDAzOQ=='

        self._config.deagg_agg_targets = ['0.11']
        self.assertRaises(Exception, get_deagg_gtids)
