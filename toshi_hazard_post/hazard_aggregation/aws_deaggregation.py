"""Hazard aggregation task dispatch."""
import logging
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterator

import boto3

import toshi_hazard_post.hazard_aggregation.deaggregation_task
from toshi_hazard_post.local_config import API_URL, S3_URL, WORK_PATH
from toshi_hazard_post.locations import get_locations
from toshi_hazard_post.util import BatchEnvironmentSetting, get_ecs_job_config

from ..toshi_api_support import toshi_api
from .aggregation_config import AggregationConfig
from .deaggregation import DeaggProcessArgs

log = logging.getLogger(__name__)

TEST_SIZE = None  # 16  # HOW many locations to run MAX (also see TOML limit)
# MEMORY = 8192  # 7168 #8192 #30720 #15360 # 10240
# NUM_WORKERS = 1  # noqa
MEMORY = 15360  # 7168 #8192 #30720 #15360 # 10240
NUM_WORKERS = 4  # noqa
NUM_MACHINES = 300
STRIDE = 100
TIME_LIMIT = 10 * 60  # minutes


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def batch_job_config(task_arguments: Dict, job_arguments: Dict, task_id: int) -> Dict[str, Any]:
    """Create an AWS Batch job configuration."""
    job_name = f"ToshiHazardPost-HazardDeAggregation-{task_id}"
    config_data = dict(task_arguments=task_arguments, job_arguments=job_arguments)
    extra_env = [
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_STAGE", value="PROD"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_REGION", value="ap-southeast-2"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_POST_WORKERS", value=str(NUM_WORKERS)),
        # DEPLOYMENT_STAGE: ${self:custom.stage}
    ]
    return get_ecs_job_config(
        job_name,
        config_data,
        toshi_api_url=API_URL,
        toshi_s3_url=S3_URL,
        task_module=toshi_hazard_post.hazard_aggregation.deaggregation_task.__name__,
        time_minutes=TIME_LIMIT,
        memory=MEMORY,
        vcpu=NUM_WORKERS,
        job_definition="BigLeverOnDemandEC2-THP-HazardAggregation",
        job_queue="ToshiHazardPost_HazAgg_JQ",  # "BigLever_32GB_8VCPU_v2_JQ", #"BigLeverOnDemandEC2-job-queue"
        extra_env=extra_env,
        use_compression=True,
    )


def batch_job_configs(config: AggregationConfig, lt_config_id: str) -> Iterator[Dict[str, Any]]:

    locations = get_locations(config)

    task_count = 0
    locs_processed = 0
    for location_chunk in chunks(locations, NUM_WORKERS):
        data = DeaggProcessArgs(
            lt_config_id=lt_config_id,
            lt_config='',
            source_branches_truncate=config.source_branches_truncate,
            hazard_model_id=config.hazard_model_id,
            aggs=config.aggs,
            deagg_dimensions=config.deagg_dimensions,
            stride=config.stride,
            skip_save=config.skip_save,
            hazard_gts=config.hazard_gts,
            locations=location_chunk,
            deagg_agg_targets=config.deagg_agg_targets,
            poes=config.poes,
            imts=config.imts,
            vs30s=config.vs30s,
            deagg_hazard_model_target=config.deagg_hazard_model_target,
            inv_time=config.inv_time,
            num_workers=NUM_WORKERS,
        )
        locs_processed += NUM_WORKERS
        task_count += 1
        yield batch_job_config(task_arguments=asdict(data), job_arguments=dict(task_id=task_count), task_id=task_count)
        if TEST_SIZE and locs_processed >= TEST_SIZE:
            break


def save_logic_tree_config(lt_config: Path):

    """Save the logic_tree config file required by every deagg task."""

    filepath = Path(WORK_PATH, 'lt_config.py')
    shutil.copyfile(lt_config, filepath)

    lt_config_id = toshi_api.save_sources_to_toshi(filepath, tag=None)
    log.debug("Produced logic_tree file id : %s from file %s" % (lt_config_id, filepath))
    return lt_config_id


def distribute_deaggregation(config: AggregationConfig, process_mode: str) -> None:
    """Configure the tasks using toshi to store the configuration."""

    log.info("saving logic tree config.")
    lt_config_id = save_logic_tree_config(config.lt_config)

    if process_mode == 'AWS_BATCH':

        batch_client = boto3.client(
            service_name='batch', region_name='us-east-1', endpoint_url='https://batch.us-east-1.amazonaws.com'
        )
        for job_config in batch_job_configs(config, lt_config_id):
            print('AWS_CONFIG: ', job_config)
            print()
            res = batch_client.submit_job(**job_config)
            print(res)
            print()

    if process_mode == 'AWS_LAMBDA':
        """Not really tested recently, lambda too puny for this work. TODO: deprecate."""
        pass
        """
        coded_locations = [CodedLocation(*loc) for loc in locations]
        for data in lambda_job_configs(config, coded_locations, toshi_ids, source_branches, levels, vs30):
            print('lamba_CONFIG: ', data)
            # Send message to initiate the process remotely
            publish_message({'aggregation_task_arguments': asdict(data)}, SNS_AGG_TASK_TOPIC)
        """
