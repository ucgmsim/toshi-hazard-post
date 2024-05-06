from itertools import product

import pytest
from nzshm_common.location.coded_location import CodedLocation

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


def test_task_generator(sites):
    task_generator = TaskGenerator(sites=sites, imts=['PGA', 'SA(1.0)'])

    i = 0
    for result in task_generator.task_generator():
        i += 1

    assert i == len(sites) * 2
