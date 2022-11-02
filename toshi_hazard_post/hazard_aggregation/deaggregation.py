import json
import logging
from collections import namedtuple
from typing import List

from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID
from toshi_hazard_store.query_v3 import get_hazard_curves

from toshi_hazard_post.branch_combinator import merge_ltbs_fromLT
from toshi_hazard_post.hazard_aggregation.aggregate_rlzs import compute_hazard_at_poe
from toshi_hazard_post.hazard_aggregation.locations import get_locations
from toshi_hazard_post.local_config import NUM_WORKERS


from .aggregate_rlzs import get_source_ids
from .aggregation_config import AggregationConfig

log = logging.getLogger(__name__)

DeAggTaskArgs = namedtuple(
    "DeAggTaskArgs",
    "hazard_model_id grid_loc locs toshi_ids source_branches aggs imts levels vs30 poes inv_time nbranches metric",
)


def add_site_name(disagg_configs):
    resolution = 0.001
    disagg_configs_copy = disagg_configs.copy()
    for disagg_config in disagg_configs_copy:
        for loc in LOCATIONS_BY_ID.values():
            location = CodedLocation(loc['latitude'], loc['longitude'], resolution).downsample(0.001).code
            if location == disagg_config['location']:
                disagg_config['site_code'] = loc['id']
                disagg_config['site_name'] = loc['name']

    return disagg_configs_copy


def process_local_config_deagg(hazard_model_id, toshi_ids, loc, vs30, poes, agg, imt, inv_time):

    hc = next(get_hazard_curves([loc], [vs30], [hazard_model_id], [imt], [agg]))
    levels = []
    hazard_vals = []
    for v in hc.values:
        levels.append(v.lvl)
        hazard_vals.append(v.val)
    
    source_info = get_source_ids(toshi_ids, vs30)
    deagg_configs = []
    for poe in poes:
        target_level = compute_hazard_at_poe(levels, hazard_vals, poe, inv_time)
    
        deagg_configs += [dict(
            vs30=vs30,
            inv_time=inv_time,
            imt=imt,
            agg=agg,
            poe=poe,
            location=loc,
            target_level=target_level,
            deagg_specs=source_info,
        )]

    return deagg_configs


def process_config_deaggregation(config: AggregationConfig):

    if not config.deaggregation:
        raise Exception('a deaggregation configuration must be specified for deagg mode')

    try:
        config.validate_deagg()
    except AssertionError as error:
        log.error(error)
        raise Exception('invalid deaggregation configuration')

    # assume an aggregation has already been performed that covers the parameters requested in the deagg. This can waste time in 2 ways
    # 1) must re-construct the source_branches
    # 2) if an agg has not been performed then the query comes back empty and we've wasted our time

    omit: List[str] = []
    toshi_ids = {}
    for vs30 in config.vs30s:
        toshi_ids[vs30] = [
            b.hazard_solution_id
            for b in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=config.hazard_solutions, omit=omit)
            if b.vs30 == vs30
        ]

    locations = get_locations(config)
    resolution = 0.001
    coded_locations = [CodedLocation(*loc, resolution) for loc in locations]
    locs = [loc.downsample(0.001).code for loc in coded_locations]
    poes = config.deagg_poes
    aggs = config.aggs
    imts = config.imts
    inv_time = config.deagg_invtime
    hazard_model_id = config.hazard_model_id

    deagg_configs = []

    # prcoess_local_config_deagg(hazard_model_id, toshi_ids, config, NUM_WORKERS)

    for loc in locs:
        for vs30 in config.vs30s:
            for agg in aggs:
                for imt in imts:
                    deagg_configs += process_local_config_deagg(
                        hazard_model_id, toshi_ids[vs30], loc, vs30, poes, agg, imt, inv_time
                    )

    deagg_configs = add_site_name(deagg_configs)
    with open('deagg_configs.json', 'w') as deagg_file:
        json.dump(deagg_configs, deagg_file)
