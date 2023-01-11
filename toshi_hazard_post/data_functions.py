import logging
import time
from typing import Any, List

import numpy as np
from toshi_hazard_store.query_v3 import get_hazard_metadata_v3, get_rlz_curves_v3

from toshi_hazard_post.branch_combinator import SourceBranchGroup
from toshi_hazard_post.util.file_utils import get_disagg
from toshi_hazard_post.util.toshi_client import download_csv

DOWNLOAD_DIR = '/work/chrisdc/NZSHM-WORKING/PROD/'
log = logging.getLogger(__name__)


def get_levels(source_branches: SourceBranchGroup, locs: List[str], vs30: int) -> Any:
    """Get the values of the levels (shaking levels) for the hazard curve from Toshi-Hazard-Store

    Parameters
    ----------
    source_branches : list
        complete logic tree with Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30 : int
        vs30

    Returns
    -------
    levels : List[float]
        shaking levels of hazard curve"""

    id = source_branches[0].toshi_hazard_ids[0]

    log.info(f"get_levels locs[0]: {locs[0]} vs30: {vs30}, id {id}")
    hazard = next(get_rlz_curves_v3([locs[0]], [vs30], None, [id], None))

    return hazard.values[0].lvls


def get_imts(source_branches: SourceBranchGroup, vs30: int) -> list:
    """Get the intensity measure types (IMTs) for the hazard curve from Toshi-Hazard-Store

    Parameters
    ----------
    source_branches : list
        complete logic tree with Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30 : int
        vs30

    Returns
    -------
    levels : List[str]
        IMTs of hazard curve"""

    ids = source_branches[0].toshi_hazard_ids
    meta = next(get_hazard_metadata_v3(ids, [vs30]))
    imts = list(meta.imts)
    imts.sort()

    return imts


def load_realization_values(toshi_ids, locs, vs30s):
    """Load hazard curves from Toshi-Hazard-Store.

    Parameters
    ----------
    toshi_ids : List[str]
        Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30s : List[int]
        vs30s

    Returns
    values : dict
        hazard curve values (probabilities) keyed by Toshi ID and gsim realization number
    """

    tic = time.perf_counter()
    # unique_ids = []
    # for branch in source_branches:
    #     unique_ids += branch['ids']
    # unique_ids = list(set(unique_ids))
    log.info('loading %s hazard IDs ... ' % len(toshi_ids))

    values = {}
    try:
        for res in get_rlz_curves_v3(locs, vs30s, None, toshi_ids, None):
            key = ':'.join((res.hazard_solution_id, str(res.rlz)))
            if key not in values:
                values[key] = {}
            values[key][res.nloc_001] = {}
            for val in res.values:
                values[key][res.nloc_001][val.imt] = np.array(val.vals)
                # for i, v in enumerate(val.vals):
                #     if not v:  # TODO: not sure what this is for
                #         log.debug(
                #             '%s th value at location: %s, imt: %s, hazard key %s is %s'
                #             % (i, res.nloc_001, val.imt, key, v)
                #         )
    except Exception as err:
        logging.warning(
            'load_realization_values() got exception %s with toshi_ids: %s , locs: %s vs30s: %s'
            % (err, toshi_ids, locs, vs30s)
        )
        raise

    # check that the correct number of records came back
    ids_ret = []
    for k1, v1 in values.items():
        nlocs_ret = len(v1.keys())
        if not nlocs_ret == len(locs):
            log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        ids_ret += [k1.split(':')[0]]
    ids_ret = set(ids_ret)
    if len(ids_ret) != len(toshi_ids):
        log.warn('Missing %s toshi IDs' % (len(toshi_ids) - len(ids_ret)))
        # log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        # log.warn('missing %s locations.' % (len(locs) - nlocs_ret))
        toshi_ids = set(toshi_ids)
        log.warn('Missing ids: %s' % (toshi_ids - ids_ret))

    toc = time.perf_counter()
    print(f'time to load realizations: {toc-tic:.1f} seconds')

    return values


def load_realization_values_deagg(toshi_ids, locs, vs30s, deagg_dimensions):
    """Load deagg matricies from oq-engine csv archives. Temporoary until deaggs are stored in THS.

    Parameters
    ----------
    toshi_ids : List[str]
        Openquake Hazard Solutions Toshi IDs
    locs : List[str]
        coded locations
    vs30s : List[int]
        not used

    Returns
    values : dict
        hazard curve values (probabilities) keyed by Toshi ID and gsim realization number
    bins : dict
        bin centers for each deagg dimension
    """

    tic = time.perf_counter()
    log.info('loading %s hazard IDs ... ' % len(toshi_ids))

    values = {}

    # download csv archives
    downloads = download_csv(toshi_ids, DOWNLOAD_DIR)
    log.info('finished downloading csv archives')
    for i, download in enumerate(downloads.values()):
        csv_archive = download['filepath']
        hazard_solution_id = download['hazard_id']
        disaggs, bins, location, imt = get_disagg(csv_archive, deagg_dimensions)
        log.info(f'finished loading data from csv archive {i+1} of {len(downloads)}')
        for rlz in disaggs.keys():
            key = ':'.join((hazard_solution_id, rlz))
            if key not in values:
                values[key] = {}
            values[key][location] = {}
            values[key][location][imt] = np.array(disaggs[rlz])

    # check that the correct number of records came back
    ids_ret = []
    for k1, v1 in values.items():
        nlocs_ret = len(v1.keys())
        if not nlocs_ret == len(locs):
            log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        ids_ret += [k1.split(':')[0]]
    ids_ret = set(ids_ret)
    if len(ids_ret) != len(toshi_ids):
        log.warn('Missing %s toshi IDs' % (len(toshi_ids) - len(ids_ret)))
        log.warn('location %s missing %s locations.' % (k1, len(locs) - nlocs_ret))
        toshi_ids = set(toshi_ids)
        print('Missing ids: %s' % (toshi_ids - ids_ret))

    toc = time.perf_counter()
    print(f'time to load realizations: {toc-tic:.1f} seconds')

    return values, bins
