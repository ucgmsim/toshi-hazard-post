"""Handler for aggregation task."""
import io
import json
import logging
import logging.config
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterator, List
from zipfile import ZipFile

import pandas as pd
import requests
import yaml
from nshm_toshi_client.toshi_file import ToshiFile
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model
from toshi_hazard_store.aggregate_rlzs import process_location_list

from toshi_hazard_post.local_config import API_KEY, API_URL, LOGGING_CFG, S3_URL

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


@dataclass
class DistributedAggregationTaskArguments:
    """Class for pass arguments to Distributed Tasks."""

    hazard_model_id: str
    source_branches_id: str
    toshi_ids: List[str]
    location: CodedLocation
    aggs: List[str]
    imts: List[str]
    levels: List[float]
    vs30: float


def models_from_dataframe(
    location: CodedLocation, data: pd.DataFrame, args: DistributedAggregationTaskArguments
) -> Iterator[model.HazardAggregation]:
    """Generate for HazardAggregation models from dataframe."""
    for agg in args.aggs:
        values = []
        df_agg = data[data['agg'] == agg]
        for imt, val in enumerate(args.imts):
            values.append(
                model.IMTValuesAttribute(
                    imt=val,
                    lvls=df_agg.level.tolist(),
                    vals=df_agg.hazard.tolist(),
                )
            )
        print(values[0])
        yield model.HazardAggregation(
            values=values,
            vs30=args.vs30,
            agg=agg,
            hazard_model_id=args.hazard_model_id,
        ).set_location(location)


def fetch_source_branches(source_branches_id: str) -> Dict:
    # Fetch and unpack the source_branches from toshi
    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filenode = toshi_api.get_download_url(source_branches_id)
    r = requests.get(filenode['file_url'], stream=True)
    if r.ok:
        zcontent = ZipFile(io.BytesIO(r.content))
        fcontent = zcontent.open('source_branches.json')
    else:
        raise Exception(r.status_code)
    return json.load(fcontent)


def process_event(evt):
    """Unpack the message from the event and do the work."""
    log.info("begin process_event()")
    log.debug("using API_KEY with len: %s" % len(API_KEY))

    # message = json.loads(evt['Sns']['Message'])
    # args = DistributedAggregationTaskArguments(**message['aggregation_task_arguments'])

    args = DistributedAggregationTaskArguments(
        hazard_model_id='HAZARD_MODEL_TEST_1',
        source_branches_id='RmlsZToxMTc3NDc=',
        toshi_ids=[
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU0',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTUz',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTkx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTUx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjAy',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTky',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTUw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk0',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTg3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTkz',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTg2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTU4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTk1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjAz',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTkw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTg4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTUy',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTg5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTYx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjQ4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjUx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjQ5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjUw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MTYw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjI4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjI2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjA1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjA0',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjI3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjE1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjMx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjMy',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjI5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjA3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjMw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjUy',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjUz',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU0',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjYx',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjYw',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjY2',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjU5',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjYy',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjY0',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjYz',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjY1',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjY3',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjY4',
            'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA4MjQ3',
        ],
        location={"lat": -46.4, "lon": 166.6},
        aggs=[
            'mean',
            '0.005',
            '0.01',
            '0.025',
            '0.05',
            '0.1',
            '0.2',
            '0.5',
            '0.8',
            '0.9',
            '0.95',
            '0.975',
            '0.99',
            '0.995',
        ],
        imts=[
            'PGA',
            'SA(0.1)',
            'SA(0.2)',
            'SA(0.3)',
            'SA(0.4)',
            'SA(0.5)',
            'SA(0.7)',
            'SA(1.0)',
            'SA(1.5)',
            'SA(2.0)',
            'SA(3.0)',
            'SA(4.0)',
            'SA(5.0)',
        ],
        levels=[
            0.01,
            0.02,
            0.04,
            0.06,
            0.08,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1,
            1.2,
            1.4,
            1.6,
            1.8,
            2,
            2.2,
            2.4,
            2.6,
            2.8,
            3,
            3.5,
            4,
            4.5,
            5,
        ],
        vs30=400,
    )

    log.info("args: %s" % args)
    source_branches = fetch_source_branches(args.source_branches_id)

    # Perform the aggregation.
    coded_loc = CodedLocation(**args.location).downsample(0.001)
    result_df = process_location_list(
        [coded_loc.code], args.toshi_ids, source_branches, args.aggs, args.imts, args.levels, args.vs30
    )
    log.info(result_df)

    # Save the results.
    with model.HazardAggregation.batch_write() as batch:
        for hag in models_from_dataframe(coded_loc, result_df, args):
            batch.save(hag)
    log.info("process_event() completed OK")


def handler(event, context):
    """This is the handler function called by SNS."""
    for evt in event.get('Records', []):
        process_event(evt)
    return True


if __name__ == '__main_':
    pass  # noqa
