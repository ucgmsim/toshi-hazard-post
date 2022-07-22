"""This module exports configuration for the current system."""

import os
from pathlib import PurePath

from toshi_hazard_post.util import get_secret


def boolean_env(environ_name):
    """Allow a few ways to configure boolean viea ENV variables."""
    return bool(os.getenv(environ_name, '').upper() in ["1", "Y", "YES", "TRUE"])


API_URL = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
S3_URL = os.getenv('NZSHM22_TOSHI_S3_URL', "http://localhost:4569")

# Get API key from AWS secrets manager
if 'TEST' in API_URL.upper():
    API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_TEST", "us-east-1").get("NZSHM22_TOSHI_API_KEY_TEST")
elif 'PROD' in API_URL.upper():
    API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_PROD", "us-east-1").get("NZSHM22_TOSHI_API_KEY_PROD")
else:
    API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")

# LOCAL SYSTEM SETTINGS
WORK_PATH = os.getenv('NZSHM22_SCRIPT_WORK_PATH', PurePath(os.getcwd(), "tmp"))
