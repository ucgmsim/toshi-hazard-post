from itertools import product
from unittest import mock

import pytest
from nzshm_common.location.code_location import CodedLocation, bin_locations

from toshi_hazard_post.version2.aggregation import TaskGenerator
from toshi_hazard_post.version2.aggregation_setup import Site


@pytest.fixture(scope='module')
def sites():
    lat_lons = [
        (-45.432, 175.123),  # rounds to -45.0, 175.0
        (-45.132, 175.2),  # rounds to -45.0, 175.0
        (-45.0, 175.3),  # rounds to -45.0, 175.0
        (-45.6, 175.7),  # rounds to -46.0, 176.0
        (45.7, 175.3),  # rounds to 46.0, 175.0
        (45.8, 175.3),  # rounds to 46.0, 175.0
    ]
    locations = [CodedLocation(*lat_lon, 0.001) for lat_lon in lat_lons]
    vs30s = [200, 300]
    return [Site(loc, vs30) for loc, vs30 in product(locations, vs30s)]


@mock.patch('toshi_hazard_post.version2.aggregation.get_realizations_dataset')
def test_task_generator(mock_rlz, sites):
    task_generator = TaskGenerator(sites=sites, imts=['PGA', 'SA(1.0)'], component_branches=[], compatability_key='A')

    i = 0
    for result in task_generator.task_generator():
        i += 1

    mock_rlz.assert_called()
    assert len(mock_rlz.mock_calls) == 3
    assert i == len(sites) * 2
    for loc_bin in bin_locations([site.location for site in sites], 1.0).values():
        # this is a bit hacky, but CodedLocationBin objects don't have an __eq__() method
        args = (str(loc_bin), [], 'A')
        assert args in [(str(call.args[0]), call.args[1], call.args[2]) for call in mock_rlz.mock_calls]
