import os
from enum import Enum, auto
from pathlib import PurePath


class ArrowFS(Enum):
    LOCAL = auto()
    AWS = auto()


WORK_PATH = os.getenv('NZSHM22_SCRIPT_WORK_PATH', PurePath(os.getcwd(), "tmp"))
THS_DIR = os.getenv('NZSHM22_THS_LOCAL_DIR', PurePath(os.getcwd(), "tmp"))
THS_S3_BUCKET = os.getenv('NSHM22_THS_S3_BUCKET')
if THS_S3_BUCKET and THS_S3_BUCKET[-1] == '/':
    THS_S3_BUCKET = THS_S3_BUCKET[:-1]

THS_S3_REGION = os.getenv('NZSHM22_HAZARD_STORE_REGION', 'ap-southeast-2')

try:
    THS_FS = ArrowFS[os.getenv('NZSHM22_THS_FS', "LOCAL").upper()]
except KeyError:
    msg = f"NZSHM22_THS_FS must be in {[x.name for x in ArrowFS]}"
    raise KeyError(msg)
