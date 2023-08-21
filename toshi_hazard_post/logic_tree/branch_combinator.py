import ast
import logging
import time
from pathlib import Path
from typing import Collection, Dict, List, Union

# from toshi_hazard_store.query_v3 import get_hazard_metadata_v3
import toshi_hazard_store
from nzshm_model.source_logic_tree.logic_tree import FlattenedSourceLogicTree
from nzshm_model.source_logic_tree.slt_config import from_config

from .logic_tree import HazardLogicTree

DTOL = 1.0e-6

log = logging.getLogger(__name__)


def preload_meta(ids: Collection[str], vs30: int) -> Dict[str, dict]:
    """Retreive the GMCM logic tree metadata from Toshi-Hazard-Store.

    Parameters
    ----------
    ids
        Toshi IDs of Openquake Hazard Solutions
    vs30

    Returns
    -------
    metadata
        dictionary of ground motion logic tree metadata dictionaries
    """
    metadata = {}
    tic = time.perf_counter()
    for i, meta in enumerate(toshi_hazard_store.query_v3.get_hazard_metadata_v3(ids, [vs30])):
        hazard_id = meta.hazard_solution_id
        # log.info(f'loaded metadata for {hazard_id}')
        gsim_lt = ast.literal_eval(meta.gsim_lt)
        metadata[hazard_id] = gsim_lt
        if i == len(ids) - 1:
            break

    toc = time.perf_counter()
    log.debug(f'time to load metadata from THS: {toc-tic} seconds.')
    print(len(ids))
    print(len(metadata))
    return metadata


def get_logic_tree(
    lt_config_filepath: Union[str, Path],
    hazard_gts: List[str],
    vs30: int,
    gmm_correlations: List[List[str]],
    truncate: int = None,
) -> HazardLogicTree:

    fslt = FlattenedSourceLogicTree.from_source_logic_tree(from_config(lt_config_filepath))
    log.info('built FlattenedSourceLogicTree')
    logic_tree = HazardLogicTree.from_flattened_slt(fslt, hazard_gts)
    log.info('built HazardLogicTree')
    log.info(f'hazard ids: {logic_tree.hazard_ids}')
    tic = time.perf_counter()
    metadata = preload_meta(logic_tree.hazard_ids, vs30)
    toc = time.perf_counter()
    print(f'time to load metadata {toc-tic} seconds')
    log.info('loaded metadata')

    tic = time.perf_counter()
    for branch in logic_tree.branches:
        # log.info('set one gmcm branch')
        branch.set_gmcm_branches(metadata, gmm_correlations)
    log.info('set gmcm branches')
    toc = time.perf_counter()
    print(f'time to set gmcm branches {toc-tic} seconds')

    # for testing
    if truncate:
        logic_tree.branches = logic_tree.branches[:truncate]

    return logic_tree
