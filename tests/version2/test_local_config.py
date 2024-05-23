import os
from pathlib import Path

import pytest

import toshi_hazard_post.version2.local_config as local_config
from toshi_hazard_post.version2.local_config import ENV_NAMES as env_vars

# env_vars = [
#     "THP_num_workers",
#     "THP_work_path",
#     "THP_ths_rlz_local_dir",
#     "THP_ths_rlz_s3_bucket",
#     "THP_ths_rlz_aws_region",
#     "THP_ths_rlz_fs",
# ]

default_attrs = [
    ("num_workers", 1),
    ("ths_rlz_fs", local_config.ArrowFS.LOCAL),
    ("ths_agg_fs", local_config.ArrowFS.LOCAL),
]

user_attrs = [
    ("num_workers", 2),
    ("work_path", 'user work path'),
    ("ths_rlz_local_dir", 'user ths local dir'),
    ("ths_rlz_s3_bucket", 'user ths s3 bucket'),
    ("ths_rlz_aws_region", 'user ths aws region'),
    ("ths_rlz_fs", local_config.ArrowFS.AWS),
    ("ths_agg_local_dir", 'user agg ths local dir'),
    ("ths_agg_s3_bucket", 'user agg ths s3 bucket'),
    ("ths_agg_aws_region", 'user agg ths aws region'),
    ("ths_agg_fs", local_config.ArrowFS.AWS),
]


@pytest.fixture(scope='function', params=list(range(8)))
def env_attr_val_fixture(request):
    attrs_vals = [
        ["num_workers", 3],
        ["work_path", 'env work path'],
        ["ths_rlz_local_dir", 'env ths local dir'],
        ["ths_rlz_s3_bucket", 'env ths s3 bucket'],
        ["ths_rlz_aws_region", 'env ths aws region'],
        ["ths_agg_local_dir", 'env agg ths local dir'],
        ["ths_agg_s3_bucket", 'env agg ths s3 bucket'],
        ["ths_agg_aws_region", 'env agg ths aws region'],
    ]
    env_attr_val = [[local_config.PREFIX + item[0].upper()] + item for item in attrs_vals]
    return env_attr_val[request.param]


user_filepath = Path(__file__).parent / 'fixtures/local_config/user_config.toml'

# clear env vars
for var in env_vars:
    if os.getenv(var):
        del os.environ[var]


@pytest.mark.parametrize("attr,value", default_attrs)
def test_default_precidence(attr, value):
    assert getattr(local_config.get_config(), attr) == value


@pytest.mark.parametrize("attr,value", user_attrs)
def test_user_precidence(attr, value):
    local_config.config_override_filepath = user_filepath
    assert getattr(local_config.get_config(), attr) == value


def test_env_precidence(env_attr_val_fixture):
    env_var, attr, value = env_attr_val_fixture
    local_config.config_override_filepath = user_filepath
    os.environ[env_var] = str(value)
    assert getattr(local_config.get_config(), attr) == value


def test_arrow_fs():
    os.environ['THP_THS_RLZ_FS'] = 'FOOBAR'
    with pytest.raises(KeyError):
        local_config.get_config()

    os.environ['THP_THS_AGG_FS'] = 'FOOBAR'
    with pytest.raises(KeyError):
        local_config.get_config()
