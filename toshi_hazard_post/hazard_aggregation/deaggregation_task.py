"""Handler for deaggregation task."""
import argparse
import json
import logging
import logging.config
import os
import sys
import time
from pathlib import Path, PurePath
from typing import Union
from zipfile import ZipFile

import requests
import yaml
from nshm_toshi_client.toshi_file import ToshiFile

from toshi_hazard_post.local_config import API_KEY, API_URL, LOGGING_CFG, S3_URL, WORK_PATH
from toshi_hazard_post.util import decompress_config

from .deaggregation import DeaggProcessArgs, process_deaggregation_local

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


def extract_lt_config(archive_filepath: Union[Path, PurePath]):

    with ZipFile(archive_filepath) as zipf:
        return zipf.extract('lt_config.py', path=WORK_PATH)


def fetch_lt_config(lt_config_id: str) -> Path:
    """Fetch and unpack the logic tree config file from toshi."""

    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filenode = toshi_api.get_download_url(lt_config_id)
    folder = Path(WORK_PATH, 'downloads', lt_config_id)
    folder.mkdir(parents=True, exist_ok=True)
    file_path = PurePath(folder, 'lt_config.py.zip')

    r = requests.get(filenode['file_url'])
    if r.ok:
        with open(str(file_path), 'wb') as f:
            f.write(r.content)
            log.info(f"downloaded logic tree config file: {file_path} {f}")
            # TODO: confirm full file downloaded w/o errors
    else:
        raise Exception(r.status_code)

    lt_filepath = extract_lt_config(file_path)

    return lt_filepath


def process_args(args: DeaggProcessArgs) -> None:
    """Call the process worker with args, sources branches etc ."""
    log.info("args: %s" % args)
    log.debug("using API_KEY with len: %s" % len(API_KEY))

    lt_config = fetch_lt_config(args.lt_config_id)
    args.lt_config = lt_config

    process_deaggregation_local(args)

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
        args = DeaggProcessArgs(**message['aggregation_task_arguments'])
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

    process_args(args=DeaggProcessArgs(**config['task_arguments']))
