import csv
from collections import namedtuple
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Iterable, List, Union

from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.location.location import get_locations

if TYPE_CHECKING:
    import numpy.typing as npt

from toshi_hazard_post.version2.ths_mock import query_levels


@dataclass
class Site:
    location: CodedLocation
    vs30: int

    def __repr__(self):
        return f"{self.location.lat}, {self.location.lon}, vs30={self.vs30}"


def get_vs30s(site_filepath: Union[str, Path]) -> Generator[int, None, None]:
    with Path(site_filepath).open() as site_file:
        reader = csv.reader(site_file)
        SiteCSV = namedtuple("SiteCSV", next(reader), rename=True)  # type:ignore
        for row in reader:
            site = SiteCSV(*row)
            yield int(site.vs30)  # type:ignore


def get_levels(compat_key: str) -> 'npt.NDArray':
    """
    Get the intensity measure type levels (IMTLs) for the hazard curve from the compatibility table

    Parameters:
        compatibility_key: the key identifying the hazard calculation compatibility entry

    Returns:
        levels: the IMTLs for the hazard calculation
    """
    return query_levels(compat_key)


def get_sites(locations: Iterable[str], vs30s: List[int]) -> List[Site]:
    """
    Get the sites (combined location and vs30) at which to calculate hazard.

    Parameters:
        locations: location identifiers. Identifiers can be anything accepted
        by nzshm_common.location.location.get_locations
        vs30s: the vs30s. If empty use the vs30s from the site files

    Returns:
        location_vs30s: Location, vs30 pairs
    """
    coded_locations = get_locations(locations, resolution=0.001)

    if vs30s:
        return [Site(location, vs30) for location, vs30 in product(coded_locations, vs30s)]

    vs30s = []
    for loc_id in locations:
        vs30s += list(get_vs30s(loc_id))
    if len(vs30s) != len(coded_locations):
        raise Exception("number of vs30s does not match number of locations")
    return list(map(Site, coded_locations, vs30s))
