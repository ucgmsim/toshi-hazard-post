"""This module exports configuration for the current system."""

import os
from pathlib import PurePath

from toshi_hazard_post.util.get_secret import get_secret


def boolean_env(environ_name):
    """Allow a few ways to configure boolean viea ENV variables."""
    return bool(os.getenv(environ_name, '').upper() in ["1", "Y", "YES", "TRUE"])


WORK_PATH = os.getenv('NZSHM22_SCRIPT_WORK_PATH', PurePath(os.getcwd(), "tmp"))
USE_API = boolean_env('NZSHM22_TOSHI_API_ENABLED')

API_URL = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
S3_URL = os.getenv('NZSHM22_TOSHI_S3_URL', "http://localhost:4569")

# Get API key from AWS secrets manager
if USE_API and 'TEST' in API_URL.upper():
    API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_TEST", "us-east-1").get("NZSHM22_TOSHI_API_KEY_TEST")
elif USE_API and 'PROD' in API_URL.upper():
    API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_PROD", "us-east-1").get("NZSHM22_TOSHI_API_KEY_PROD")
else:
    API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")

IS_OFFLINE = boolean_env('SLS_OFFLINE')  # set by serverless-wsgi plugin
# IS_TESTING = boolean_env('TESTING', 'False')


DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
LOGGING_CFG = os.getenv('LOGGING_CFG', 'toshi_hazard_post/logging.yaml')
CLOUDWATCH_APP_NAME = os.getenv('CLOUDWATCH_APP_NAME', 'CLOUDWATCH_APP_NAME_unconfigured')
SNS_AGG_TASK_TOPIC = os.getenv('SNS_AGG_TASK_TOPIC', 'undefined_topic')


REGION = os.getenv('REGION', 'ap-southeast-2')  # SYDNEY
NZSHM22_HAZARD_STORE_STAGE = os.getenv('NZSHM22_HAZARD_STORE_STAGE', 'LOCAL').upper()
