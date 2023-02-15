"""Handler for aggregation task."""
import argparse
import io
import json
import logging
import logging.config
import os
import sys
import time
from typing import Dict
from zipfile import ZipFile

import requests
import yaml
from dacite import from_dict
from nshm_toshi_client.toshi_file import ToshiFile
from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_post.local_config import API_KEY, API_URL, LOGGING_CFG, NUM_WORKERS, S3_URL
from toshi_hazard_post.logic_tree.branch_combinator import SourceBranchGroup
from toshi_hazard_post.util import decompress_config

from .aggregation import DistributedAggregationTaskArguments, process_aggregation_local

# from toshi_hazard_store import model
# from toshi_hazard_store.aggregate_rlzs import process_location_list


log = logging.getLogger(__name__)

# configure logging
if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    print(f'logging config from: {LOGGING_CFG}')
else:
    logging.basicConfig(level=logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    log.addHandler(screen_handler)
    print(f'logging config not found: {LOGGING_CFG}')


def fetch_source_branches(source_branches_id: str) -> Dict:
    """Fetch and unpack the source_branches from toshi."""

    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filenode = toshi_api.get_download_url(source_branches_id)
    r = requests.get(filenode['file_url'], stream=True)
    if r.ok:
        zcontent = ZipFile(io.BytesIO(r.content))
        fcontent = zcontent.open('source_branches.json')
    else:
        raise Exception(r.status_code)

    source_branches_dict = json.load(fcontent)
    return {int(k): from_dict(data_class=SourceBranchGroup, data=v) for k, v in source_branches_dict.items()}


def process_args(args):
    """Call the process worker with args, sources branches etc ."""
    log.info("args: %s" % args)
    log.debug("using API_KEY with len: %s" % len(API_KEY))

    resolution = 0.001
    source_branches = fetch_source_branches(args.source_branches_id)

    print([loc for loc in args.locations])
    locations = [CodedLocation(loc['lat'], loc['lon'], resolution) for loc in args.locations]
    results = process_aggregation_local(
        args.hazard_model_id, args.toshi_ids, source_branches, locations, args.levels, args, num_workers=NUM_WORKERS
    )
    log.info(results)


#  _                     _ _             _                     _ _
# | | __ _ _ __ ___   __| | |__   __ _  | |__   __ _ _ __   __| | | ___ _ __
# | |/ _` | '_ ` _ \ / _` | '_ \ / _` | | '_ \ / _` | '_ \ / _` | |/ _ \ '__|
# | | (_| | | | | | | (_| | |_) | (_| | | | | | (_| | | | | (_| | |  __/ |
# |_|\__,_|_| |_| |_|\__,_|_.__/ \__,_| |_| |_|\__,_|_| |_|\__,_|_|\___|_|
#
def handler(event, context):
    """This is the handler function called by SNS."""

    def process_event(evt):
        """Unpack the message from the event and do the work."""
        log.info("begin process_event()")
        message = json.loads(evt['Sns']['Message'])
        args = DistributedAggregationTaskArguments(**message['aggregation_task_arguments'])
        process_args(args)

    for evt in event.get('Records', []):
        process_event(evt)
        log.info("process_event() completed OK")

    return True


# _ __ ___   __ _(_)_ __
#  | '_ ` _ \ / _` | | '_ \
#  | | | | | | (_| | | | | |
#  |_| |_| |_|\__,_|_|_| |_|
#
# This is run in the BATCH environment by ./script/container_task.sh.
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    args = parser.parse_args()

    try:
        # LOCAL and CLUSTER this is a file
        config_file = args.config
        f = open(args.config, 'r', encoding='utf-8')
        config = json.load(f)
    except Exception:
        # for AWS this must now be a compressed JSON string
        config = json.loads(decompress_config(args.config))

    # Wait for some more time, scaled by taskid to avoid S3 consistency issue
    time.sleep(config['job_arguments']['task_id'])

    process_args(args=DistributedAggregationTaskArguments(**config['task_arguments']))
