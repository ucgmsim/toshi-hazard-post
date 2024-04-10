from pathlib import Path

from nzshm_common.location.location import LOCATION_LISTS
from nzshm_model import get_model_version
from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree
from toshi_hazard_post.version2.aggregation_setup import get_lts, get_sites
from toshi_hazard_post.version2.aggregation_config import AggregationConfig

config_filepath = Path(__file__).parent / 'fixtures/hazard.toml'
config = AggregationConfig(config_filepath)

def test_model():
    slt, glt = get_lts(config)

    model_expected = get_model_version(config.model_version)
    assert slt == model_expected.source_logic_tree
    assert glt == model_expected.gmm_logic_tree

def test_model_from_paths():
    config.model_version = None
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt_v1.0.4.json'
    srm_filepath = Path(__file__).parent / 'fixtures/slt_v1.0.4.json'
    config.gmcm_file = gmcm_filepath
    config.srm_file = srm_filepath

    slt_expected = SourceLogicTree.from_json(srm_filepath)
    gmcm_expected = GMCMLogicTree.from_json(gmcm_filepath)

    slt, glt = get_lts(config)

    assert slt == slt_expected
    assert glt == gmcm_expected


def test_get_sites():
    vs30s = [200, 400]
    locations = ["NZ"]

    # all combinations of location and vs30
    sites = get_sites(locations, vs30s)
    assert len(sites) == len(LOCATION_LISTS[locations[0]]['locations']) * len(vs30s)

    vs30s = []
    locations = [Path(__file__).parent / 'fixtures/sites_w_vs30.csv']
    sites = get_sites(locations, vs30s)
    assert len(sites) == 2
    assert sites[0].vs30 == 250
    assert sites[1].vs30 == 400



