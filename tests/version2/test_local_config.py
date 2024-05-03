import os
from pathlib import Path

import pytest

import toshi_hazard_post.version2.local_config as local_config

env_vars = [
    "THP_NUM_WORKERS",
    "THP_SCRIPT_WORK_PATH",
    "THP_THS_LOCAL_DIR",
    "THP_THS_S3_BUCKET",
    "THP_THS_AWS_REGION",
    "THP_THS_FS",
]

default_attrs = [
    ("NUM_WORKERS", 1),
    ("WORK_PATH", 'default work path'),
    ("THS_DIR", 'default ths local dir'),
    ("THS_S3_BUCKET", 'default ths s3 bucket'),
    ("THS_S3_REGION", 'default ths aws region'),
    ("THS_FS", local_config.ArrowFS.LOCAL),
]

user_attrs = [
    ("NUM_WORKERS", 2),
    ("WORK_PATH", 'user work path'),
    ("THS_DIR", 'user ths local dir'),
    ("THS_S3_BUCKET", 'user ths s3 bucket'),
    ("THS_S3_REGION", 'user ths aws region'),
    ("THS_FS", local_config.ArrowFS.AWS),
]


@pytest.fixture(scope='function', params=list(range(len(env_vars) - 1)))
def env_attr_val_fixture(request):
    env_attrs = [
        ["NUM_WORKERS", 3],
        ["WORK_PATH", 'env work path'],
        ["THS_DIR", 'env ths local dir'],
        ["THS_S3_BUCKET", 'env ths s3 bucket'],
        ["THS_S3_REGION", 'env ths aws region'],
        ["THS_FS", local_config.ArrowFS.LOCAL],
    ]
    env_attr_val = [[evar] + attr for evar, attr in zip(env_vars, env_attrs)]
    # the way the fixture is parameterized makes the THS_FS annoying to check
    env_attr_val = env_attr_val[:-1]
    return env_attr_val[request.param]


config_default_filepath = Path(__file__).parent / 'fixtures/local_config/thp_config.toml'
user_filepath = Path(__file__).parent / 'fixtures/local_config/user_config.toml'

# clear env vars
for var in env_vars:
    if os.getenv(var):
        del os.environ[var]


@pytest.mark.parametrize("attr,value", default_attrs)
def test_default_precidence(attr, value):
    local_config.config_default_filepath = config_default_filepath
    assert getattr(local_config.get_config(), attr) == value


@pytest.mark.parametrize("attr,value", user_attrs)
def test_user_precidence(attr, value):
    local_config.config_default_filepath = config_default_filepath
    local_config.config_override_filepath = user_filepath
    assert getattr(local_config.get_config(), attr) == value


def test_env_precidence(env_attr_val_fixture):
    env_var, attr, value = env_attr_val_fixture
    local_config.config_default_filepath = config_default_filepath
    local_config.config_override_filepath = user_filepath
    os.environ[env_var] = str(value)
    assert getattr(local_config.get_config(), attr) == value


def test_arrow_fs():
    os.environ['THP_THS_FS'] = 'FOOBAR'
    with pytest.raises(KeyError):
        local_config.get_config()
