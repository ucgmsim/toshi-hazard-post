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
from collections import UserDict
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional, Union

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
    NUM_WORKERS: int
    WORK_PATH: str
    THS_LOCAL_DIR: str
    THS_S3_BUCKET: str
    THS_S3_REGION: str
    THS_FS: ArrowFS

PREFIX = 'THP_'
ENV_NAMES = [PREFIX + key for key in Config.__dataclass_fields__.keys()]

class DotDict(UserDict):
    """
    dot notation access to dictionary attributes
    forces keys to lowercase
    a dot notation request is equivelent to get and will return None if the key is not present
    """

    def __setitem__(self, key: Any, item: Any) -> None:
        if not isinstance(key, str):
            raise TypeError("key must be str type")
        super().__setitem__(key.lower(), item)

    def __getattr__(self, name) -> Any:
        return self.get(name, None)

    def __setattr__(self, name: str, value: Any) -> None:
        self.__dict__[name] = value  # instead of self[name] to avoid recusion problem


def update_config_from_file(filepath: Union[str, Path]):

    config_from_file = DotDict()
    file_config = toml.load(filepath)
    for k, v in file_config.items():
        config_from_file[k] = v
    return config_from_file


def get_config() -> Config:

    # loading order determines precidence
    config_from_file = DotDict()
    if config_default_filepath.exists():
        config_from_file.update(update_config_from_file(config_default_filepath))
    if config_override_filepath:
        config_from_file.update(update_config_from_file(config_override_filepath))

    # env vars take highest precidence
    NUM_WORKERS = int(os.getenv('THP_NUM_WORKERS', config_from_file.num_workers))
    WORK_PATH = os.getenv('THP_WORK_PATH', config_from_file.work_path)
    THS_LOCAL_DIR = os.getenv('THP_THS_LOCAL_DIR', config_from_file.ths_local_dir)
    THS_S3_BUCKET = os.getenv('THP_THS_S3_BUCKET', config_from_file.ths_s3_bucket)
    if THS_S3_BUCKET and THS_S3_BUCKET[-1] == '/':
        THS_S3_BUCKET = THS_S3_BUCKET[:-1]
    THS_S3_REGION = os.getenv('THP_THS_S3_REGION', config_from_file.ths_aws_region)
    arrow_fs = os.getenv('THP_THS_FS', config_from_file.ths_fs).upper()

    try:
        THS_FS = ArrowFS[arrow_fs]
    except KeyError:
        msg = f"THP_THS_FS must be in {[x.name for x in ArrowFS]}"
        raise KeyError(msg)

    return Config(
        NUM_WORKERS=NUM_WORKERS,
        WORK_PATH=WORK_PATH,
        THS_LOCAL_DIR=THS_LOCAL_DIR,
        THS_S3_BUCKET=THS_S3_BUCKET,
        THS_S3_REGION=THS_S3_REGION,
        THS_FS=THS_FS,
    )
