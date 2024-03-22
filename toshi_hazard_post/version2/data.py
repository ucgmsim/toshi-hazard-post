from typing import TYPE_CHECKING, List, Union, Generator
from pathlib import Path
from collections import namedtuple
import csv
from .mock_ths import query_realizations

if TYPE_CHECKING:
    import numpy.typing as npt
    from .logic_tree import HazardLogicTree
    from nzshm_common.location.code_location import CodedLocation

class ValueStore:
    pass

def get_vs30s(site_filepath: Union[str, Path]) -> Generator[int, None, None]:
    with Path(site_filepath).open() as site_file:
        reader = csv.reader(site_file)
        Site = namedtuple("Site", next(reader), rename=True)
        for row in reader:
            site = Site(*row)
            yield int(site.vs30)



def load_realizations(
    logic_tree: HazardLogicTree,
    imt: str,
    location: 'CodedLocation',
    vs30: int,
    compatibility_key: str,
) -> ValueStore:
    """
    Load component realizations from the database.

    Parameters:
        logic_tree: the full (srm + gmcm) logic tree
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        location: the site location
        vs30: the site vs30
        compatibility_key: the compatibility key used to lookup the correct realizations in the database

    Returns:
        values: the component realizations rates (not probabilities)
    """
    pass

def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    aggs: List[str],
    hazard_model_id: str,
) -> None:
    """
    Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

    Parameters:
        hazard: the aggregate hazard rates (not proabilities)
        aggs: the statistical aggregate types (e.g. "mean", "0.5")
    """
    pass

