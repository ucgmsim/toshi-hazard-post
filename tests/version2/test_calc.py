import numpy as np
import pytest
from pathlib import Path

from toshi_hazard_post.version2.logic_tree import HazardLogicTree
from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree

from toshi_hazard_post.version2.aggregation_calc import calc_composite_rates
from toshi_hazard_post.version2.data import ValueStore


        

@pytest.fixture(scope='module')
def logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt_v1.0.4.json'
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt_v1.0.4.json'
    slt = SourceLogicTree.from_json(slt_filepath)
    glt = GMCMLogicTree.from_json(gmcm_filepath)
    return HazardLogicTree(slt, glt)


@pytest.fixture(scope='module')
def value_store(logic_tree):

    value_store = ValueStore()
    for i, branch in enumerate(logic_tree.component_branches):
        values = np.linspace(0,1,10) * i
        value_store.set_values(values, branch)
    return value_store


def test_calc_composite_rates(logic_tree, value_store):

    nlevels = 10 
    rates = calc_composite_rates(next(logic_tree.composite_branches), value_store, nlevels)
    assert rates.shape == (10,)
