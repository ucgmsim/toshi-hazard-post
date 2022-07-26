"""Handler for aggregation task."""
import io
import json
import logging
import logging.config
import os
from typing import Iterator
from zipfile import ZipFile

import pandas as pd
import requests
import yaml
from nshm_toshi_client.toshi_file import ToshiFile
from nzshm_common.location.code_location import CodedLocation
from toshi_hazard_store import model
from toshi_hazard_store.aggregate_rlzs import process_location_list

from toshi_hazard_post.local_config import API_KEY, API_URL, LOGGING_CFG, S3_URL

from .aws_aggregation import DistributedAggregationTaskArguments

log = logging.getLogger(__name__)

# configure logging
if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    print(f'logging config from: {LOGGING_CFG}')
else:
    logging.basicConfig(level=logging.DEBUG)
    print(f'logging config not found: {LOGGING_CFG}')


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


def process_event(evt):
    """Unpack the message from the event and do the work."""
    log.info("begin process_event()")

    message = json.loads(evt['Sns']['Message'])
    args = DistributedAggregationTaskArguments(**message['aggregation_task_arguments'])
    log.info("args: %s" % args)

    # Fetch and unpack the source_branches from toshi
    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filenode = toshi_api.get_download_url(args.source_branches_id)
    r = requests.get(filenode['file_url'], stream=True)
    if r.ok:
        zcontent = ZipFile(io.BytesIO(r.content))
        fcontent = zcontent.open('source_branches.json')
    else:
        raise Exception(r.status_code)
    source_branches = json.load(fcontent)

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
