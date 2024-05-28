"""
Package to set compute configuration. Configuration parameters can be set by default file, user file, and/or
environment variables in that order of increasing precidence.

Environment varaible parameters are uppercase, config file is case insensitive.

Config file format is toml

To use the local configuration, set local_config.config_override_filepath to the desired config file path. Then call
get_config() inside a function. Note that get_config() must be called in function scope. If called in module scope,
changes to config_coverride_filepath will not be effective

Parameters:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes
    THP_WORK_PATH: path for saving any local files
    THP_THS_{RLZ|AGG}_FS: the filesystem to use for the {realization or aggregate} datastore (LOCAL or AWS)
    THP_THS_{RLZ|AGG}_LOCAL_DIR: the path to the local {realization or aggregate} datastore (if using LOCAL)
    THP_THS_{RLZ|AGG}_S3_BUCKET: the S3 bucket where the {realization or aggregate} datastore is kept (if using AWS)
    THP_THS_{RLZ|AGG}_AWS_REGION: the AWS region for {realization or aggregate} if using S3 datastore
"""

# TODO: there's nothing to check that the minimum of config parameters are set
# TODO: Config class has reduncancy. Can have one type for Arrow and have an instance for rlz and an insance for agg

import os
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Union

import toml


class ArrowFS(Enum):
    LOCAL = auto()
    AWS = auto()


DEFAULT_NUM_WORKERS = 1
DEFAULT_FS = ArrowFS.LOCAL

# this is set by the thp script if the user specifies a config file
config_override_filepath: Optional[Path] = None


@dataclass
class Config:

    thp_num_workers: int
    _thp_num_workers: int = field(init=False, repr=False)

    thp_ths_rlz_fs: ArrowFS
    _thp_ths_rlz_fs: ArrowFS = field(init=False, repr=False)
    thp_ths_agg_fs: ArrowFS
    _thp_ths_agg_fs: ArrowFS = field(init=False, repr=False)

    thp_work_path: str = '/tmp'

    thp_ths_rlz_local_dir: Optional[str] = None
    thp_ths_rlz_s3_bucket: Optional[str] = None
    thp_ths_rlz_aws_region: Optional[str] = None

    thp_ths_agg_local_dir: Optional[str] = None
    thp_ths_agg_s3_bucket: Optional[str] = None
    thp_ths_agg_aws_region: Optional[str] = None

    @property  # type: ignore
    def thp_num_workers(self):
        return self._thp_num_workers

    @thp_num_workers.setter
    def thp_num_workers(self, value: int):
        self._thp_num_workers = int(value)

    @property  # type: ignore
    def thp_ths_rlz_fs(self):
        return self._thp_ths_rlz_fs

    @thp_ths_rlz_fs.setter
    def thp_ths_rlz_fs(self, value: Union[str, ArrowFS]):
        if isinstance(value, str):
            self._thp_ths_rlz_fs = Config.set_fs(value)
        else:
            self._thp_ths_rlz_fs = value

    @property  # type: ignore
    def thp_ths_agg_fs(self):
        return self._thp_ths_agg_fs

    @thp_ths_agg_fs.setter
    def thp_ths_agg_fs(self, value: Union[str, ArrowFS]):
        if isinstance(value, str):
            self._thp_ths_agg_fs = Config.set_fs(value)
        else:
            self._thp_ths_agg_fs = value

    @staticmethod
    def set_bucket(bucket):
        if bucket and bucket[-1] == '/':
            return bucket[:-1]
        return bucket

    @staticmethod
    def set_fs(fs: str):
        if fs:
            try:
                return ArrowFS[fs.upper()]
            except KeyError:
                msg = f"filesystem set to '{fs}', but ths_rlz_fs and ths_agg_fs must be in {[x.name for x in ArrowFS]}"
                raise KeyError(msg)


ENV_NAMES = [key.upper() for key in Config.__dataclass_fields__.keys()]


def get_config_from_file(filepath: Union[str, Path]):

    config_from_file = dict()
    file_config = toml.load(filepath)
    for k, v in file_config.items():
        config_from_file[k] = v
    return config_from_file


def get_config_from_env():

    config_from_env = dict()
    for name in Config.__dataclass_fields__.keys():
        env_name = name.upper()
        if os.getenv(env_name):
            config_from_env[name] = os.getenv(env_name)
    return config_from_env


def get_config() -> Config:

    config_dict = dict()
    if config_override_filepath:
        config_dict.update(get_config_from_file(config_override_filepath))
    # env vars take highest precidence
    config_dict.update(get_config_from_env())

    config = Config(thp_num_workers=DEFAULT_NUM_WORKERS, thp_ths_rlz_fs=DEFAULT_FS, thp_ths_agg_fs=DEFAULT_FS)
    for k, v in config_dict.items():
        setattr(config, k.lower(), v)

    return config
