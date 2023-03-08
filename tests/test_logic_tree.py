from pathlib import Path
import json
from unittest import mock, TestCase
from typing import List

from dacite import from_dict

from nzshm_model.source_logic_tree.logic_tree import FlattenedSourceLogicTree
from toshi_hazard_post.toshi_api_support import SourceSolutionMap
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree, GMCMBranch


def test_sourcesolutionmap():

    gt_query_filepath = Path(Path(__file__).parent, 'fixtures/logic_tree', 'hazard_gt.json')
    gt_expected_filepath = Path(Path(__file__).parent, 'fixtures/logic_tree', 'hazard_gt_expected.json')

    with open(gt_query_filepath, 'r') as gt_query_file:
        hazard_jobs = json.load(gt_query_file)['data']['node1']['children']['edges']
    source_solution_map = SourceSolutionMap(hazard_jobs)

    with open(gt_expected_filepath, 'r') as gte:
        expected = json.load(gte)

    assert source_solution_map._dict == expected


def mock_source_solution_map() -> SourceSolutionMap:
    ssm = SourceSolutionMap()
    ssm._dict = {
        'onfault_0:dist_0': 'hazsol_0',
        'onfault_1:dist_1': 'hazsol_1',
        'onfault_2:dist_2': 'hazsol_2',
        'onfault_3:dist_3': 'hazsol_3',
        'onfault_4:dist_4': 'hazsol_4',
        'onfault_5:dist_5': 'hazsol_5',
    }
    return ssm


def expected_gmcm_branches() -> List[GMCMBranch]:
    gmcm_branches = []
    realizations = [
        ["hazsol_0:0", "hazsol_3:0"],
        ["hazsol_0:0", "hazsol_3:1"],
        ["hazsol_0:1", "hazsol_3:0"],
        ["hazsol_0:1", "hazsol_3:1"],
        ["hazsol_0:2", "hazsol_3:0"],
        ["hazsol_0:2", "hazsol_3:1"],
    ]

    weights = [
        0.25 * 0.2,
        0.25 * 0.8,
        0.5 * 0.2,
        0.5 * 0.8,
        0.25 * 0.2,
        0.25 * 0.8,
    ]

    for rlz, weight in zip(realizations, weights):
        gmcm_branches.append(GMCMBranch(rlz, weight))

    return gmcm_branches


@mock.patch('toshi_hazard_post.logic_tree.logic_tree.toshi_api.get_hazard_gt')
class TestHazardLogicTree(TestCase):
    def setUp(self):
        flat_lt_filepath = Path(Path(__file__).parent, 'fixtures/logic_tree', 'flattened_lt.json')
        with open(flat_lt_filepath) as flat_lt_file:
            data = json.load(flat_lt_file)
        self.flattened_lt = from_dict(data_class=FlattenedSourceLogicTree, data=data)

        metadata_filepath = Path(Path(__file__).parent, 'fixtures/logic_tree', 'metadata.json')
        with open(metadata_filepath) as mdf:
            self.metadata = json.load(mdf)

    def test_build_from_flat(self, mock_api):

        mock_api.return_value = mock_source_solution_map()

        # build a HazardLogicTree mocking the call to ToshiApi which would return a SourceSolutionMap for a particular
        # GT ID
        logic_tree = HazardLogicTree.from_flattened_slt(self.flattened_lt, ['mock'])

        # check that the hazard ids in logic_tree are the correct ones
        assert set(logic_tree.hazard_ids) == set(mock_source_solution_map()._dict.values())

        # check that the number of branches matches the input FlattenedSourceLogicTree
        assert len(logic_tree.branches) == len(self.flattened_lt.branches)

        # check that the CompositeBranch es on the HazardLogicTree branches are the same as the CompositeBranch es from
        # the FlattenedSourceLogicTree
        comp_branches = [branch.source_branch for branch in logic_tree.branches]
        expected = [branch for branch in self.flattened_lt.branches]
        assert comp_branches == expected

    def test_set_gmm_branches(self, mock_api):

        mock_api.return_value = mock_source_solution_map()

        # build a HazardLogicTree from a FlattenedSourceLogicTree same as above
        logic_tree = HazardLogicTree.from_flattened_slt(self.flattened_lt, ['mock'])

        # for the 0th branch of the HazardLogicTree run set_gmcm_branches() no correlations
        logic_tree.branches[0].set_gmcm_branches(self.metadata, [])

        # check that the gmcm_branches are correct
        assert logic_tree.branches[0].gmcm_branches == expected_gmcm_branches()
