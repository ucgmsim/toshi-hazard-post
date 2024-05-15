import logging
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List

import numpy as np

from toshi_hazard_post.version2.local_config import get_config

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.coded_location import CodedLocation

log = logging.getLogger(__name__)


def write_aggs_to_ths(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
    hazard_model_id: str,
) -> None:
    config = get_config()
    if not config.work_path:
        raise Exception(" a work path must be specified")
    agg_dir = Path(config.work_path) / 'AGGREGATIONS'
    if not agg_dir.is_dir():
        agg_dir.mkdir()
    filepath = agg_dir / f"{hazard_model_id}_{vs30}_{imt}_{location.code}"
    np.save(filepath, hazard)


def query_compatibility(compatibility_key: str) -> Generator[str, None, None]:
    entries = {"A_A": "a", "NZSHM22-0": "nzshm22-0"}

    if compatibility_key in entries:
        yield entries[compatibility_key]


def query_levels(compat_key: str) -> 'npt.NDArray':
    """
    Get the intensity measure type levels (IMTLs) for the hazard curve from the compatibility table

    Parameters:
        compatibility_key: the key identifying the hazard calculation compatibility entry

    Returns:
        levels: the IMTLs for the hazard calculation
    """
    return np.array(
        [
            0.0001,
            0.0002,
            0.0004,
            0.0006,
            0.0008,
            0.001,
            0.002,
            0.004,
            0.006,
            0.008,
            0.01,
            0.02,
            0.04,
            0.06,
            0.08,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
            1.2,
            1.4,
            1.6,
            1.8,
            2.0,
            2.2,
            2.4,
            2.6,
            2.8,
            3.0,
            3.5,
            4.0,
            4.5,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
        ]
    )
