from unittest import mock
import unittest
from pathlib import Path
import json
from collections import namedtuple

from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig
from toshi_hazard_post.hazard_aggregation.deaggregation import get_deagg_gtids
from toshi_hazard_post.locations import get_locations

index_filepath = Path(Path(__file__).parent, 'fixtures', 'deaggregation', 'index.json')
with open(index_filepath) as index_file:
    index = json.load(index_file)

GtIdsArgs = namedtuple("GtIdsArgs",
                       """
                       hazard_gts 
                       lt_config 
                       locations 
                       deagg_agg_targets 
                       poes 
                       imts 
                       vs30s 
                       deagg_hazard_model_target 
                       inv_time 
                       iter_method
                       """)

@mock.patch('toshi_hazard_post.hazard_aggregation.deaggregation.urllib.request')
@mock.patch('toshi_hazard_post.hazard_aggregation.deaggregation.get_index_from_s3', return_value=index)
class testGetGts(unittest.TestCase):
    def setUp(self):

        config_filepath = Path(Path(__file__).parent, 'fixtures', 'deaggregation', 'config.toml')
        config = AggregationConfig(config_filepath)
        locations = get_locations(config)
        self._gt_ids_args = GtIdsArgs(
            config.hazard_gts,
            config.lt_config,
            locations,
            config.deagg_agg_targets,
            config.poes,
            config.imts,
            config.vs30s,
            config.deagg_hazard_model_target,
            config.inv_time,
            "",
        )



    def test_get_deagg_gts(self, mock_index, mock_requests):
    
        gt_ids = get_deagg_gtids(
            self._gt_ids_args.hazard_gts,
            self._gt_ids_args.lt_config,
            self._gt_ids_args.locations,
            self._gt_ids_args.deagg_agg_targets,
            self._gt_ids_args.poes,
            self._gt_ids_args.imts,
            self._gt_ids_args.vs30s,
            self._gt_ids_args.deagg_hazard_model_target,
            self._gt_ids_args.inv_time,
            self._gt_ids_args.iter_method,
        )

        assert len(gt_ids) == 1
        assert gt_ids[0] == 'R2VuZXJhbFRhc2s6MTM1OTEzMA=='

        with self.assertRaises(Exception):
            get_deagg_gtids(
                self._gt_ids_args.hazard_gts,
                self._gt_ids_args.lt_config,
                self._gt_ids_args.locations,
                ['0.11'],
                self._gt_ids_args.poes,
                self._gt_ids_args.imts,
                self._gt_ids_args.vs30s,
                self._gt_ids_args.deagg_hazard_model_target,
                self._gt_ids_args.inv_time,
                self._gt_ids_args.iter_method,
            )
