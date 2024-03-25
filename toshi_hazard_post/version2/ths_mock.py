from typing import Generator, Any, List
from dataclasses import dataclass
from itertools import product
import numpy as np

def query_compatibility(compatibility_key: str) -> Generator[str, None, None]:
    entries = {"A":"a", "B":"b", "C":"c"}

    if compatibility_key in entries:
        yield entries[compatibility_key]

@dataclass
class mRLZ:
    values: List[float]
    loc: str
    vs30: int
    imt: str
    sources: List[str]
    gsims: List[str]

def query_realizations(
        locs: List[str],
        vs30s: List[int],
        imts: List[str],
        sources: List[str],
        gsims: List[str],
        compat_key: str
) -> Generator[mRLZ, None, None]:

    for loc, vs30, imt, source, gsim in product(locs, vs30s, imts, sources, gsims):
        yield mRLZ(
            loc=loc,
            vs30=vs30,
            imt=imt,
            sources=[source],
            gsims=[gsim],
            values=list(np.linspace(1, 0, 10) * 0.5),
        )
        
