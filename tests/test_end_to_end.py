from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

import toshi_hazard_post.local_config
from toshi_hazard_post.aggregation import run_aggregation
from toshi_hazard_post.aggregation_args import AggregationArgs

fixture_dir = Path(__file__).parent / 'fixtures' / 'end_to_end'
parquet_filepath = fixture_dir / 'rlz_probs.pq'
args_filepath = fixture_dir / 'hazard.toml'
aggs_expected_filepath = fixture_dir / 'aggregations_275_PGA_-34.500~173.000.npy'


@mock.patch('toshi_hazard_post.aggregation_calc.save_aggregations')
@mock.patch('toshi_hazard_post.aggregation_calc.load_realizations')
def test_end_to_end(load_mock, save_mock, monkeypatch):
    aggs_expected = np.load(aggs_expected_filepath)

    def mock_config():
        return dict(NUM_WORKERS=1)

    monkeypatch.setattr(toshi_hazard_post.local_config, 'get_config', mock_config)
    load_mock.return_value = pd.read_parquet(parquet_filepath)
    df = pd.read_parquet(parquet_filepath)
    e_100 = np.array(
        [
            1.08094662e-01,
            1.08094662e-01,
            1.08085223e-01,
            1.07998222e-01,
            1.07720889e-01,
            1.07164048e-01,
            1.00059994e-01,
            7.83751309e-02,
            6.05159029e-02,
            4.78485823e-02,
            3.88501808e-02,
            1.80388670e-02,
            7.12142605e-03,
            3.78514500e-03,
            2.29136948e-03,
            1.49269367e-03,
            2.91913282e-04,
            8.44983224e-05,
            2.94818801e-05,
            1.15311404e-05,
            4.88225760e-06,
            2.19345452e-06,
            1.03349475e-06,
            5.05957019e-07,
            2.55615760e-07,
            6.96675642e-08,
            1.97543848e-08,
            5.25937427e-09,
            1.00168707e-09,
            3.23859828e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
            1.38331568e-11,
        ]
    )
    np.testing.assert_allclose(df.loc[100, 'values'], e_100)

    agg_args = AggregationArgs(args_filepath)
    run_aggregation(agg_args)
    aggs = save_mock.mock_calls[0].args[0]
    np.testing.assert_allclose(aggs, aggs_expected, rtol=1e-7, atol=2e-5)
