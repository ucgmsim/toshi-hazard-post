import logging
import time
from typing import TYPE_CHECKING, List

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs

from toshi_hazard_post.version2.local_config import ARROW_DIR

# from toshi_hazard_post.version2.calculators import rate_to_prob, prob_to_rate


if TYPE_CHECKING:
    from nzshm_common.location.code_location import CodedLocation

    from toshi_hazard_post.version2.logic_tree import HazardLogicTree, HazardComponentBranch

log = logging.getLogger(__name__)


def load_realizations(
    component_branches: List['HazardComponentBranch'],
    imt: str,
    location: 'CodedLocation',
    vs30: int,
    compatibility_key: str,
) -> pa.table:
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
    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={location.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()

    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]

    flt0 = (
        (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
        & (pc.field('imt') == pa.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.field('compatible_calc_fk') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
    )
    columns = ['sources_digest', 'gmms_digest', 'values']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt0, columns=columns)
    t2 = time.monotonic()

    rlz_table = arrow_scanner.to_table()
    t3 = time.monotonic()

    log.info(
        f"load dataset: {round(t1-t0, 6)}, scanner:{round(t2-t1, 6)}, to_arrow {round(t3-t2, 6)}"
    )
    log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
    log.info("loaded %s realizations in arrow", rlz_table.shape[0])
    return rlz_table


# def save_aggregations(
#     hazard: 'npt.NDArray',
#     location: 'CodedLocation',
#     vs30: int,
#     imt: str,
#     agg_types: List[str],
#     hazard_model_id: str,
# ) -> None:
#     """
#     Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

#     Parameters:
#         hazard: the aggregate hazard rates (not proabilities)
#         location: the site location
#         vs30: the site vs30
#         imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
#         agg_types: the statistical aggregate types (e.g. "mean", "0.5")
#         hazard_model_id: the model id for storing in the database
#     """
#     hazard = rate_to_prob(hazard, 1.0)
#     write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)
