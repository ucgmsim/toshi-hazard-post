from pathlib import Path

from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig
from toshi_hazard_post.locations import get_locations


def test_locations():
    config_filepath = Path(Path(__file__).parent, 'fixtures', 'locations', 'config.toml')
    config = AggregationConfig(config_filepath)

    config.locations = ['-43.130~175.444', 'srg_1']
    locations = get_locations(config)
    assert locations[0] == (-43.13, 175.444)
    assert locations[1] == (-35.229696132, 173.958389289)

    config.locations = ["SRWG214"]
    assert len(get_locations(config)) == 214

    config.locations = ["NZ"]
    assert len(get_locations(config)) == 36

    config.locations = ["SRWG214", "NZ"]
    locations = get_locations(config)
    assert len(locations) == 36 + 214
    assert (-44.256621332, 171.136008497) in locations
    assert (-41.51, 173.95) in locations

    config.locations = ["NZ_0_1_NB_1_1"]
    assert len(get_locations(config)) == 3741

    config.locations = ["NZ", "NZ_0_1_NB_1_1"]
    assert len(get_locations(config)) == 3741 + 36

    config.locations = ["srg_1"]
    assert get_locations(config)[0] == (-35.229696132, 173.958389289)

    config.locations = ["WLG"]
    assert get_locations(config)[0] == (-41.3, 174.78)
