"""Hazard aggregation task dispatch."""
import json
import logging
from pathlib import Path

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store.aggregate_rlzs import get_imts, get_levels
from toshi_hazard_store.aggregate_rlzs_mp import build_source_branches
from toshi_hazard_store.branch_combinator.branch_combinator import merge_ltbs_fromLT

from toshi_hazard_post.local_config import SNS_AGG_TASK_TOPIC, WORK_PATH
from toshi_hazard_post.util.sns import publish_message

from .aggregation_config import AggregationConfig
from .toshi_api_support import process_one_file

log = logging.getLogger(__name__)


def process_aws(
    toshi_ids,
    source_branches,
    coded_locations,
    levels,
    config,
):
    """AWS lambda function handler function."""
    for coded_loc in coded_locations:
        log.info(f'coded_loc.code {coded_loc.downsample(0.001).code}')
        for vs30 in config.vs30s:
            # Send message to initiate the process remotely
            publish_message({'hello': 'world'}, SNS_AGG_TASK_TOPIC)


def push_test_message():
    """For local SNS testing only."""
    publish_message({'hello': 'world'}, SNS_AGG_TASK_TOPIC)


def distribute_aggregation(config: AggregationConfig):
    """Configure the tasks using toshi to store the configuration."""
    toshi_ids = [
        branch.hazard_solution_id
        for branch in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=[])
    ]
    log.debug("toshi_ids: %s" % toshi_ids)

    log.info("building the sources branches.")
    source_branches = build_source_branches(
        config.logic_tree_permutations, config.hazard_solutions, config.vs30s[0], omit=[], truncate=None
    )

    if 1 == 0:
        filepath = Path(WORK_PATH, 'source_branches.json')
        with open(filepath, 'w') as sbf:
            sbf.write(json.dumps(source_branches, indent=2))
        assert 0
        source_branches_id = process_one_file(filepath, tag=None)
        log.info("Produced source_branches id : %s from file %s" % (source_branches_id, filepath))

    locations = (
        load_grid(config.locations)
        if not config.location_limit
        else load_grid(config.locations)[: config.location_limit]
    )
    coded_locations = [CodedLocation(*loc) for loc in locations]

    example_loc_code = coded_locations[0].downsample(0.001).code
    levels = get_levels(source_branches, [example_loc_code], config.vs30s[0])  # TODO: get seperate levels for every IMT
    avail_imts = get_imts(source_branches, config.vs30s[0])
    for imt in config.imts:
        assert imt in avail_imts

    process_aws(toshi_ids, source_branches, coded_locations, levels, config)
