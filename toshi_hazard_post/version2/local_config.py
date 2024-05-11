"""
Package to set compute configuration. Configuration parameters can be set by default file, user file, and/or
environment variables in that order of increasing precidence.

Environment varaible parameters are uppercase, config file is case insensitive.

Config file format is toml

Parameters in config file do not have THP_ prefix

To use the local configuration, set local_config.config_override_filepath to the desired config file path. Then call
get_config() inside a function. Note that get_config() must be called in function scope. If called in module scope,
changes to config_coverride_filepath will not be effective

Parameters:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes
    THP_WORK_PATH: path for saving any local files
    THP_THS_FS: the filesystem to use for the realization datastore (LOCAL or AWS)
    THP_THS_LOCAL_DIR: the path to the local realization datastore (required if NZHSM22_THS_FS == "LOCAL")
    THP_THS_S3_BUCKET: the S3 bucket where the relaization datastore is kept (required if NZHSM22_THS_FS == "AWS")
    THP_THS_AWS_REGION: the AWS region if using S3 datastore
"""

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Union

import toml


class ArrowFS(Enum):
    LOCAL = auto()
    AWS = auto()


# TODO: this will have to change when we refactor the package structure
config_default_filepath = Path(__file__).parent.parent.parent / 'thp_config.toml'

# this is set by the thp script if the user specifies a config file
config_override_filepath: Optional[Path] = None


@dataclass
class Config:
    num_workers: Optional[int] = None
    work_path: Optional[str] = None
    ths_local_dir: Optional[str] = None
    ths_s3_bucket: Optional[str] = None
    ths_aws_region: Optional[str] = None
    ths_fs: Optional[ArrowFS] = None


PREFIX = 'THP_'
ENV_NAMES = [(PREFIX + key).upper() for key in Config.__dataclass_fields__.keys()]


def update_config_from_file(filepath: Union[str, Path]):

    config_from_file = dict()
    file_config = toml.load(filepath)
    for k, v in file_config.items():
        config_from_file[k] = v
    return config_from_file


def get_config() -> Config:

    # loading order determines precidence
    config_from_file = dict()
    if config_default_filepath.exists():
        config_from_file.update(update_config_from_file(config_default_filepath))
    if config_override_filepath:
        config_from_file.update(update_config_from_file(config_override_filepath))

    # env vars take highest precidence
    config = Config()
    for name in Config.__dataclass_fields__.keys():
        env_name = PREFIX + name.upper()
        setattr(config, name, os.getenv(env_name, config_from_file.get(name)))
    config.num_workers = int(config.num_workers)
    if config.ths_s3_bucket and config.ths_s3_bucket[-1] == '/':
        config.ths_s3_bucket = config.ths_s3_bucket[:-1]
    try:
        config.ths_fs = ArrowFS[config.ths_fs]
    except KeyError:
        msg = f"THP_THS_FS must be in {[x.name for x in ArrowFS]}"
        raise KeyError(msg)

    return config
