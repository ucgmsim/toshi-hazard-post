"""Hazard aggregation task dispatch."""
import logging
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterator

import boto3

import toshi_hazard_post.hazard_aggregation.aggregation_task
from toshi_hazard_post.local_config import API_URL, NUM_WORKERS, S3_URL, WORK_PATH
from toshi_hazard_post.util import BatchEnvironmentSetting, get_ecs_job_config

from ..toshi_api_support import toshi_api
from .aggregation_config import AggregationConfig
from .deaggregation import DistributedDeaggTaskArguments

log = logging.getLogger(__name__)

TEST_SIZE = None  # 16  # HOW many locations to run MAX (also see TOML limit)
MEMORY = 8192  # 7168 #8192 #30720 #15360 # 10240
NUM_WORKERS = 1  # noqa
STRIDE = 200


def batch_job_config(task_arguments: Dict, job_arguments: Dict, task_id: int) -> Dict[str, Any]:
    """Create an AWS Batch job configuration."""
    job_name = f"ToshiHazardPost-HazardAggregation-{task_id}"
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
        time_minutes=600,
        memory=MEMORY,
        vcpu=NUM_WORKERS,
        job_definition="BigLeverOnDemandEC2-THP-HazardAggregation",
        job_queue="ToshiHazardPost_HazAgg_JQ",  # "BigLever_32GB_8VCPU_v2_JQ", #"BigLeverOnDemandEC2-job-queue"
        extra_env=extra_env,
        use_compression=True,
    )


def batch_job_configs(
    config: AggregationConfig,
    lt_config_id: str,
) -> Iterator[Dict[str, Any]]:

    task_count = 0
    locs_processed = 0
    for gtid in config.hazard_gts:
        data = DistributedDeaggTaskArguments(
            gtid,
            config.source_branches_truncate,
            config.hazard_model_id,
            config.aggs,
            config.deagg_dimensions,
            STRIDE,
            config.skip_save,
            lt_config_id,
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
