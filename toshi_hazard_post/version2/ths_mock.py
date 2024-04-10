from typing import Generator, List, TYPE_CHECKING, Iterable, Sequence
from dataclasses import dataclass
from itertools import product
import numpy as np

from nzshm_model.logic_tree import SourceBranch, GMCMBranch
from toshi_hazard_post.version2.logic_tree import HazardBranch


if TYPE_CHECKING:
    import numpy.typing as npt


def query_compatibility(compatibility_key: str) -> Generator[str, None, None]:
    entries = {"A": "a", "B": "b", "C": "c"}

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


@dataclass
class mRLZ:
    values: List[float]
    loc: str
    vs30: int
    imt: str
    source: SourceBranch
    gsims: Sequence[GMCMBranch]


def query_realizations(
    locs: List[str], vs30s: List[int], imts: List[str], branches: Iterable[HazardBranch], compat_key: str
) -> Generator[mRLZ, None, None]:

    for loc, vs30, imt, branch in product(locs, vs30s, imts, branches):
        yield mRLZ(
            loc=loc,
            vs30=vs30,
            imt=imt,
            source=branch.source_branch,
            gsims=branch.gmcm_branches,
            values=list(np.linspace(1, 0, 10) * 0.5),
        )
