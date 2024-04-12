import os
from enum import Enum, auto
from pathlib import PurePath


class ArrowFS(Enum):
    LOCAL = auto()
    AWS = auto()


WORK_PATH = os.getenv('NZSHM22_SCRIPT_WORK_PATH', PurePath(os.getcwd(), "tmp"))
ARROW_DIR = os.getenv('NZSHM22_THS_REPO', PurePath(os.getcwd(), "tmp"))

try:
    ARROW_FS = ArrowFS[os.getenv('NZSHM22_THS_FS', "LOCAL").upper()]
except KeyError:
    msg = f"NZSHM22_THS_FS must be in {[x.name for x in ArrowFS]}"
    raise KeyError(msg)
