import json
import tempfile
import unittest
from collections import namedtuple
from unittest import mock
from pathlib import Path
from dacite import from_dict

import toshi_hazard_post.hazard_aggregation.aws_aggregation
import toshi_hazard_post.hazard_aggregation.aws_deaggregation
import toshi_hazard_post.hazard_aggregation.aggregation_task
import toshi_hazard_post.hazard_aggregation.deaggregation_task
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree

MockRequests = namedtuple('MockRequests', 'ok content')


class MockOpen:
    def __init__(self, path):
        self._path = Path(path)

    def open(self, dummy):
        return self._path.open()

    def extract(self, dummy):
        return self._path


@mock.patch('toshi_hazard_post.hazard_aggregation.aws_aggregation.toshi_api.save_sources_to_toshi')
@mock.patch('toshi_hazard_post.hazard_aggregation.aggregation_task.ZipFile')
@mock.patch.multiple(
    'toshi_hazard_post.hazard_aggregation.aggregation_task',
    io=mock.DEFAULT,
    ToshiFile=mock.DEFAULT,
    requests=mock.DEFAULT,
)
class TestSourceBranches(unittest.TestCase):
    def setUp(self):
        self._logic_tree_file = Path(Path(__file__).parent, 'fixtures/aws', 'logic_tree.json')

    def test_save_and_fetch(self, mock_zipfile, mock_save_sources, io, ToshiFile, requests):

        logic_trees = {400: from_dict(data_class=HazardLogicTree, data=json.load(open(self._logic_tree_file, 'r')))}

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(toshi_hazard_post.hazard_aggregation.aws_aggregation, 'WORK_PATH', tmp_dir):
                toshi_hazard_post.hazard_aggregation.aws_aggregation.save_logic_trees(logic_trees)

            mock_zipfile.return_value = MockOpen(Path(tmp_dir, 'logic_trees.json'))
            logic_trees_fetched = toshi_hazard_post.hazard_aggregation.aggregation_task.fetch_logic_trees('foobar')

        assert logic_trees_fetched == logic_trees


@unittest.skip("difficult to mock")
@mock.patch('toshi_hazard_post.hazard_aggregation.aws_deaggregation.toshi_api.save_sources_to_toshi')
@mock.patch('toshi_hazard_post.hazard_aggregation.deaggregation_task.ZipFile')
@mock.patch.multiple(
    'toshi_hazard_post.hazard_aggregation.deaggregation_task',
    ToshiFile=mock.DEFAULT,
    requests=mock.DEFAULT,
)
class TestLogicTreeFile(unittest.TestCase):
    def setUp(self):
        self._logic_tree_file = Path(Path(__file__).parent, 'fixtures/aws', 'SLT_v8p0p1_gmm_v2_correctedhik_400.py')

    def test_save_and_fetch_lt(self, mock_zipfile, mock_save_sources, ToshiFile, requests):

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(toshi_hazard_post.hazard_aggregation.aws_deaggregation, 'WORK_PATH', tmp_dir):
                with mock.patch.object(toshi_hazard_post.hazard_aggregation.deaggregation_task, 'WORK_PATH', tmp_dir):
                    toshi_hazard_post.hazard_aggregation.aws_deaggregation.save_logic_tree_config(self._logic_tree_file)
                    mock_zipfile.return_value = MockOpen(Path(tmp_dir, 'lt_config.py'))
                    # lt_config = toshi_hazard_post.hazard_aggregation.deaggregation_task.fetch_lt_config('foobar')

        # assert 0
        # assert logic_trees_fetched == logic_trees
