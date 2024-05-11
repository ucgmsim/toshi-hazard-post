import os
from pathlib import Path

import pytest

import toshi_hazard_post.version2.local_config as local_config
from toshi_hazard_post.version2.local_config import ENV_NAMES as env_vars

# env_vars = [
#     "THP_num_workers",
#     "THP_work_path",
#     "THP_ths_local_dir",
#     "THP_ths_s3_bucket",
#     "THP_THS_AWS_REGION",
#     "THP_ths_fs",
# ]

default_attrs = [
    ("num_workers", 1),
    ("work_path", 'default work path'),
    ("ths_local_dir", 'default ths local dir'),
    ("ths_s3_bucket", 'default ths s3 bucket'),
    ("ths_aws_region", 'default ths aws region'),
    ("ths_fs", local_config.ArrowFS.LOCAL),
]

user_attrs = [
    ("num_workers", 2),
    ("work_path", 'user work path'),
    ("ths_local_dir", 'user ths local dir'),
    ("ths_s3_bucket", 'user ths s3 bucket'),
    ("ths_aws_region", 'user ths aws region'),
    ("ths_fs", local_config.ArrowFS.AWS),
]


@pytest.fixture(scope='function', params=list(range(len(env_vars) - 1)))
def env_attr_val_fixture(request):
    env_attrs = [
        ["num_workers", 3],
        ["work_path", 'env work path'],
        ["ths_local_dir", 'env ths local dir'],
        ["ths_s3_bucket", 'env ths s3 bucket'],
        ["ths_aws_region", 'env ths aws region'],
        ["ths_fs", local_config.ArrowFS.LOCAL],
    ]
    env_attr_val = [[evar] + attr for evar, attr in zip(env_vars, env_attrs)]
    # the way the fixture is parameterized makes the ths_fs annoying to check
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
