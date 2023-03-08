"""Helper function for sceduling AWS Batch tasks."""

import collections
import json
import urllib.parse
from typing import Any, Dict, List

from .util import compress_config

BatchEnvironmentSetting = collections.namedtuple('BatchEnvironmentSetting', 'name value')


def get_ecs_job_config(
    job_name,
    config,
    toshi_api_url,
    toshi_s3_url,
    task_module,
    time_minutes,
    memory,
    vcpu,
    job_definition="Fargate-runzi-opensha-JD",
    job_queue="BasicFargate_Q",
    extra_env: List[BatchEnvironmentSetting] = None,
    use_compression=False,
) -> Dict[str, Any]:
    """Build a config for batch."""
    if "Fargate" in job_definition:
        assert vcpu in [0.25, 0.5, 1, 2, 4]
        assert memory in [
            512,
            1024,
            2048,  # value = 0.25
            1024,
            2048,
            3072,
            4096,  # value = 0.5
            2048,
            3072,
            4096,
            5120,
            6144,
            7168,
            8192,  # value = 1
            4096,
            5120,
            6144,
            7168,
            8192,
            9216,
            10240,
            11264,
            12288,
            13312,
            14336,
            15360,
            16384,  # value = 2
            8192,
            9216,
            10240,
            11264,
            12288,
            13312,
            14336,
            15360,
            16384,
            17408,
            18432,
            19456,
            20480,
            21504,
            22528,
            23552,
            24576,
            25600,
            26624,
            27648,
            28672,
            29696,
            30720,  # value = 4
        ]

    config = {
        "jobName": job_name,
        "jobQueue": job_queue,
        "jobDefinition": job_definition,
        "containerOverrides": {
            "command": ["-s", "/app/scripts/container_task.sh"],
            "resourceRequirements": [{"value": str(memory), "type": "MEMORY"}, {"value": str(vcpu), "type": "VCPU"}],
            "environment": [
                {
                    "name": "TASK_CONFIG_JSON_QUOTED",
                    "value": compress_config(json.dumps(config))
                    if use_compression
                    else urllib.parse.quote(json.dumps(config)),
                },
                {"name": "NZSHM22_TOSHI_S3_URL", "value": toshi_s3_url},
                {"name": "NZSHM22_TOSHI_API_URL", "value": toshi_api_url},
                {"name": "PYTHON_TASK_MODULE", "value": task_module},
            ],
        },
        "propagateTags": True,
        "timeout": {"attemptDurationSeconds": (time_minutes * 60) + 1800},
    }

    if extra_env:
        for ex in extra_env:
            config['containerOverrides']['environment'].append(dict(name=ex.name, value=ex.value))

    return config
