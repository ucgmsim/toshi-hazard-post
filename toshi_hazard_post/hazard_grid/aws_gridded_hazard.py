import itertools
from dataclasses import asdict
from typing import Dict, Generator, Iterable

import boto3
from nzshm_common.location import CodedLocation

import toshi_hazard_post.hazard_grid.grid_task
from toshi_hazard_post.hazard_grid.gridded_hazard import DistributedGridTaskArguments
from toshi_hazard_post.local_config import API_URL, S3_URL
from toshi_hazard_post.util import BatchEnvironmentSetting, get_ecs_job_config

NUM_MACHINES = 300
NUM_WORKERS = 4
TIME_LIMIT = 12 * 60
MEMORY = 15360


def batch_job_config(task_arguments: Dict, job_arguments: Dict, task_id: int):

    job_name = f"ToshiHazardPost-GriddedHazard-{task_id}"
    config_data = dict(task_arguments=task_arguments, job_arguments=job_arguments)

    extra_env = [
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_STAGE", value="PROD"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_REGION", value="ap-southeast-2"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_POST_WORKERS", value=str(NUM_WORKERS)),
    ]
    return get_ecs_job_config(
        job_name,
        config_data,
        toshi_api_url=API_URL,
        toshi_s3_url=S3_URL,
        task_module=toshi_hazard_post.hazard_grid.grid_task.__name__,
        time_minutes=TIME_LIMIT,
        memory=MEMORY,
        vcpu=NUM_WORKERS,
        job_definition="BigLeverOnDemandEC2-THP-HazardAggregation",
        job_queue="ToshiHazardPost_GriddedHaz_JQ",  # "BigLever_32GB_8VCPU_v2_JQ", #"BigLeverOnDemandEC2-job-queue"
        extra_env=extra_env,
        use_compression=True,
    )


def tasks_by_chunk(
    poe_levels: Iterable[float],
    hazard_model_ids: Iterable[str],
    vs30s: Iterable[float],
    imts: Iterable[str],
    aggs: Iterable[str],
    chunk_size: int,
) -> Generator[DistributedGridTaskArguments, None, None]:

    count = 0
    total = 0
    task_chunk = DistributedGridTaskArguments(
        location_grid_id='',
        poe_levels=[],
        hazard_model_ids=[],
        vs30s=[],
        imts=[],
        aggs=[],
        filter_locations=[],
    )

    for (hazard_model_id, vs30, imt, agg) in itertools.product(hazard_model_ids, vs30s, imts, aggs):
        count += 1
        total += 1
        task_chunk.hazard_model_ids.append(hazard_model_id)
        task_chunk.vs30s.append(vs30)
        task_chunk.imts.append(imt)
        task_chunk.aggs.append(agg)
        if count == chunk_size:
            task_chunk.poe_levels = poe_levels
            yield task_chunk
            count = 0
            task_chunk = DistributedGridTaskArguments(
                location_grid_id='',
                poe_levels=[],
                hazard_model_ids=[],
                vs30s=[],
                imts=[],
                aggs=[],
                filter_locations=[],
            )
        elif total == len(hazard_model_ids) * len(vs30s) * len(imts) * len(aggs):
            task_chunk.poe_levels = poe_levels
            yield task_chunk


def batch_job_configs(
    location_grid_id: str,
    poe_levels: Iterable[float],
    hazard_model_ids: Iterable[str],
    vs30s: Iterable[float],
    imts: Iterable[str],
    aggs: Iterable[str],
    filter_locations: Iterable[CodedLocation] = None,
):

    task_count = 0
    items_processed = 0

    for task_chunk in tasks_by_chunk(poe_levels, hazard_model_ids, vs30s, imts, aggs, chunk_size=NUM_WORKERS):

        print(task_chunk)

        data = DistributedGridTaskArguments(
            location_grid_id=location_grid_id,
            poe_levels=task_chunk.poe_levels,
            hazard_model_ids=task_chunk.hazard_model_ids,
            vs30s=task_chunk.vs30s,
            imts=task_chunk.imts,
            aggs=task_chunk.aggs,
            filter_locations=filter_locations,
        )
        items_processed += NUM_WORKERS
        task_count += 1
        yield batch_job_config(
            task_arguments=asdict(data),
            job_arguments=dict(task_id=task_count, num_machines=NUM_MACHINES),
            task_id=task_count,
        )


def distribute_gridded_hazard(
    location_grid_id: str,
    poe_levels: Iterable[float],
    hazard_model_ids: Iterable[str],
    vs30s: Iterable[float],
    imts: Iterable[str],
    aggs: Iterable[str],
    filter_locations: Iterable[CodedLocation] = None,
):

    batch_client = boto3.client(
        service_name='batch', region_name='us-east-1', endpoint_url='https://batch.us-east-1.amazonaws.com'
    )

    for job_config in batch_job_configs(
        location_grid_id,
        poe_levels,
        hazard_model_ids,
        vs30s,
        imts,
        aggs,
        filter_locations,
    ):
        print('AWS_CONFIG: ', job_config)
        print()
        res = batch_client.submit_job(**job_config)
        print(res)
        print()
