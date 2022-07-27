"""Hazard aggregation task dispatch."""
import json
import logging
from dataclasses import asdict
from pathlib import Path

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store.aggregate_rlzs import get_imts, get_levels
from toshi_hazard_store.aggregate_rlzs_mp import build_source_branches
from toshi_hazard_store.branch_combinator.branch_combinator import merge_ltbs_fromLT

from toshi_hazard_post.local_config import SNS_AGG_TASK_TOPIC, WORK_PATH
from toshi_hazard_post.util.sns import publish_message

# from toshi_hazard_post.util.util import compress_config
from .aggregation_config import AggregationConfig
from .aggregation_task import DistributedAggregationTaskArguments, fetch_source_branches
from .toshi_api_support import save_sources_to_toshi

log = logging.getLogger(__name__)


def push_test_message():
    """For local SNS testing only."""
    publish_message({'hello': 'world'}, SNS_AGG_TASK_TOPIC)


def save_source_branches(source_branches):
    """Save the source_branches.json required by every aggregation task."""
    filepath = Path(WORK_PATH, 'source_branches.json')
    with open(filepath, 'w') as sbf:
        sbf.write(json.dumps(source_branches, indent=2))

    # print(f'lzha size: {len(compress_config(json.dumps(source_branches, indent=2)))}')

    source_branches_id = save_sources_to_toshi(filepath, tag=None)
    log.debug("Produced source_branches id : %s from file %s" % (source_branches_id, filepath))
    return source_branches_id


def distribute_aggregation(config: AggregationConfig):
    """Configure the tasks using toshi to store the configuration."""
    toshi_ids = [
        branch.hazard_solution_id
        for branch in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=[])
    ]
    log.debug("toshi_ids: %s" % toshi_ids)

    # build source branches or reuse existing (for testin/debugging only)
    if config.reuse_source_branches_id:
        log.info("reuse sources_branches_id: %s" % config.reuse_source_branches_id)
        source_branches_id = config.reuse_source_branches_id
        source_branches = fetch_source_branches(source_branches_id)
    else:
        log.info("building the sources branches.")
        source_branches = build_source_branches(
            config.logic_tree_permutations,
            config.hazard_solutions,
            config.vs30s[0],
            omit=[],
            truncate=config.source_branches_truncate,
        )
        source_branches_id = save_source_branches(source_branches)
        log.info("saved source_branches to id : %s" % source_branches_id)

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

    for coded_loc in coded_locations:
        log.info(f'coded_loc.code {coded_loc.downsample(0.001).code}')
        for vs30 in config.vs30s:
            # for agg in config.aggs:
            # Send message to initiate the process remotely
            data = DistributedAggregationTaskArguments(
                config.hazard_model_id, source_branches_id, toshi_ids, coded_loc, config.aggs, config.imts, levels, vs30
            )
            print(data)
            log.info('task done')
            assert 0
            publish_message({'aggregation_task_arguments': asdict(data)}, SNS_AGG_TASK_TOPIC)
