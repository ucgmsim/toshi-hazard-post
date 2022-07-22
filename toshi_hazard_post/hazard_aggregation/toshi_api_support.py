"""ToshiAPI interface utility & helpers."""

import logging
from pathlib import Path, PurePath
from typing import Union

from nshm_toshi_client.toshi_file import ToshiFile

from toshi_hazard_post.local_config import API_KEY, API_URL, S3_URL, WORK_PATH
from toshi_hazard_post.util import archive

log = logging.getLogger(__name__)


def create_archive(filename: str, working_path: Union[str, PurePath]) -> str:
    """Verify source and if OK return the path to the zipped contents."""
    log.info(f"create_archive {filename}.zip in working_path={working_path}")
    if Path(filename).exists():
        return archive(filename, Path(working_path, f"{Path(filename).name}.zip"))
    raise Exception("file does not exist.")


def process_one_file(filepath: str, tag: str = None) -> str:
    """Archive and upload one file."""
    log.info(f"Processing */{Path(filepath).name} :: {tag}")

    archive_path = create_archive(filepath, WORK_PATH)
    log.info(f'archived {filepath} in {archive_path}.')

    headers = {"x-api-key": API_KEY}
    toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    filename = Path(filepath).name

    meta = dict(filename=filename)
    if tag:
        meta['tag'] = str(tag)

    archive_file_id, post_url = toshi_api.create_file(archive_path, meta=meta)
    toshi_api.upload_content(post_url, archive_path)
    log.info(f"pushed {archive_path} to ToshiAPI {API_URL} with id {archive_file_id}")
    return archive_file_id
