import logging
import time
from typing import TYPE_CHECKING, List

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs

from toshi_hazard_post.version2.local_config import THS_DIR, THS_FS, THS_S3_BUCKET, THS_S3_REGION, ArrowFS
from toshi_hazard_post.version2.ths_mock import write_aggs_to_ths

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation, CodedLocationBin

    from toshi_hazard_post.version2.logic_tree import HazardComponentBranch

log = logging.getLogger(__name__)


def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
    hazard_model_id: str,
) -> None:
    """
    Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

    Parameters:
        hazard: the aggregate hazard rates (not proabilities)
        location: the site location
        vs30: the site vs30
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the statistical aggregate types (e.g. "mean", "0.5")
        hazard_model_id: the model id for storing in the database
    """
    write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)


def get_local_fs(local_dir):
    return fs.LocalFileSystem(), str(local_dir)


def get_s3_fs(region, bucket):
    session = boto3.session.Session()
    credentials = session.get_credentials()
    filesystem = fs.S3FileSystem(
        secret_key=credentials.secret_key,
        access_key=credentials.access_key,
        region=region,
        session_token=credentials.token,
    )
    root = bucket
    return filesystem, root


def get_arrow_filesystem():
    if THS_FS is ArrowFS.LOCAL:
        log.info(f"retrieving relization data from local repository {THS_DIR}")
        filesystem, root = get_local_fs(THS_DIR)
    elif THS_FS is ArrowFS.AWS:
        log.info(f"retrieving relization data from S3 repository {THS_S3_REGION}:{THS_S3_BUCKET}")
        filesystem, root = get_s3_fs(THS_S3_REGION, THS_S3_BUCKET)
    else:
        filesystem = root = None
    return filesystem, root


def get_realizations_dataset(
    location_bin: 'CodedLocationBin',
    component_branches: List['HazardComponentBranch'],
    compatibility_key: str,
) -> ds.Dataset:
    """
    Get a pyarrow Dataset filtered to a location bin (partition), component branches, and compatability key

    Parameters:
        location_bin: the location bin that the database is partitioned on
        component_branches: the branches to filter into the dataset
        compatability_key: the hazard engine compatability ley to filter into the dataset

    Returns:
        dataset: the dataset with the filteres applied
    """
    filesystem, root = get_arrow_filesystem()

    partition = f"nloc_0={location_bin.code}"

    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]

    flt0 = (
        (pc.field('compatible_calc_fk') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
    )

    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    dataset = dataset.filter(flt0)
    t1 = time.monotonic()
    log.info(f"time to get realizations dataset {t1-t0:.6f}")

    return dataset


def load_realizations(
    dataset: ds.Dataset,
    imt: str,
    location: 'CodedLocation',
    vs30: int,
) -> pa.table:
    """
    Load component realizations from the database.

    Parameters:
        component_branches: list of the component branches that are combined to construct the full logic tree
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        location: the site location
        vs30: the site vs30
        compatibility_key: the compatibility key used to lookup the correct realizations in the database

    Returns:
        values: the component realizations rates (not probabilities)
    """

    flt = (
        (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
        & (pc.field('imt') == pa.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
    )

    t0 = time.monotonic()
    columns = ['sources_digest', 'gmms_digest', 'values']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt, columns=columns)
    t1 = time.monotonic()

    rlz_table = arrow_scanner.to_table()
    t2 = time.monotonic()
    if len(rlz_table) == 0:
        raise Exception(f"no realizations were found in the database for {location=}, {imt=}, {vs30=}")

    log.info(f"load scanner:{round(t1-t0, 6)}s, to_arrow {round(t2-t1, 6)}s")
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
