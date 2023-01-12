import json
import tempfile
import unittest
from collections import namedtuple
from unittest import mock
from pathlib import Path
from dacite import from_dict

import toshi_hazard_post.hazard_aggregation.aws_aggregation
import toshi_hazard_post.hazard_aggregation.aggregation_task
from toshi_hazard_post.branch_combinator import SourceBranchGroup
from .test_branch_combinator import convert_source_branches, load_gmcm_branches

MockRequests = namedtuple('MockRequests', 'ok content')


class MockOpen:
    def __init__(self, path):
        self._path = Path(path)

    def open(self, dummy):
        return self._path.open()


@mock.patch('toshi_hazard_post.hazard_aggregation.aws_aggregation.save_sources_to_toshi')
@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation_task.ZipFile')
@mock.patch.multiple(
    'toshi_hazard_post.hazard_aggregation.aggregation_task',
    io=mock.DEFAULT,
    ToshiFile=mock.DEFAULT,
    requests=mock.DEFAULT,
)
class TestSourceBranches(unittest.TestCase):
    def setUp(self):
        self._sb_file = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'source_branches.json')
        self._gmcm_branches_filepath = Path(Path(__file__).parent, 'fixtures/branch_combinator', 'gmcm_branches.json')

    def test_save_and_fetch(self, mock_zipfile, mock_save_sources, io, ToshiFile, requests):

        gmcm_branches = load_gmcm_branches(self._gmcm_branches_filepath)
        # source_branches = {400: convert_source_branches(json.load(open(self._sb_file, 'r')))}
        source_branches = {400: from_dict(data_class=SourceBranchGroup, data=json.load(open(self._sb_file, 'r')))}
        for i in range(len(source_branches)):
            source_branches[400][i].gmcm_branches = gmcm_branches

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(toshi_hazard_post.hazard_aggregation.aws_aggregation, 'WORK_PATH', tmp_dir):
                toshi_hazard_post.hazard_aggregation.aws_aggregation.save_source_branches(source_branches)

            mock_zipfile.return_value = MockOpen(Path(tmp_dir, 'source_branches.json'))
            source_branches_fetched = toshi_hazard_post.hazard_aggregation.aggregation_task.fetch_source_branches(
                'foobar'
            )

        assert source_branches_fetched == source_branches
