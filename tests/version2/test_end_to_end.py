import os
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

import toshi_hazard_post.version2.local_config as local_config
from toshi_hazard_post.version2.aggregation import run_aggregation
from toshi_hazard_post.version2.aggregation_args import AggregationArgs

for env_name in local_config.ENV_NAMES:
    if os.getenv(env_name):
        del os.environ['env_name']

config_filepath = Path(__file__).parent / 'fixtures' / 'end_to_end' / 'thp_config.toml'
pickle_filepath = Path(__file__).parent / 'fixtures' / 'end_to_end' / 'rlz_probs.pkl'
args_filepath = Path(__file__).parent / 'fixtures' / 'end_to_end' / 'hazard.toml'
aggs_expected_filepath = Path(__file__).parent / 'fixtures' / 'end_to_end' / 'aggregations_275_PGA_-34.500~173.000.npy'


@mock.patch('toshi_hazard_post.version2.aggregation_calc.save_aggregations')
@mock.patch('toshi_hazard_post.version2.aggregation_calc.load_realizations')
def test_end_to_end(load_mock, save_mock):
    local_config.config_override_filepath = config_filepath
    aggs_expeced = np.load(aggs_expected_filepath)

    load_mock.return_value = pd.read_pickle(pickle_filepath)
    agg_args = AggregationArgs(args_filepath)
    run_aggregation(agg_args)
    aggs = save_mock.mock_calls[0].args[0]
    assert np.allclose(aggs, aggs_expeced, rtol=1e-8, atol=1e-12)
