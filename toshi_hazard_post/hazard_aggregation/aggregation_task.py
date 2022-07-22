"""Handler for aggregation task."""
import json
import logging
import logging.config
import os

import yaml

# from toshi_hazard_store.aggregate_rlzs import process_location_list
from toshi_hazard_post.local_config import LOGGING_CFG  # API_KEY, API_URL,

log = logging.getLogger(__name__)

if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    print(f'logging config from: {LOGGING_CFG}')
else:
    logging.basicConfig(level=logging.DEBUG)
    print(f'logging config not found: {LOGGING_CFG}')


def process_event(evt):
    """Unpack the message from the event and do the work."""
    log.info("begin process_event()")
    print(evt)

    message = json.loads(evt['Sns']['Message'])

    print(message)

    # """
    # toshi_ids,
    # source_branches,
    # coded_locations,
    # levels,
    # config
    # """

    # # _id = message['id']
    # # general_task_id = message.get('general_task_id', None)

    # # if general_task_id:
    # #     process_general_task_request(general_task_id, message)
    # # else:
    # #     raise ValueError("need a general_task_id")

    # # unpack message info
    # #
    # result = process_location_list(
    #     [coded_loc.downsample(0.001).code], toshi_ids, source_branches, config.aggs, config.imts, levels, vs30
    # )
    # print(result)
    # log.info('done one loc one vs30')
    # return

    log.info("process_event() completed OK")


def handler(event, context):
    """This is the handler function called by SNS."""
    for evt in event.get('Records', []):
        process_event(evt)
    return True
