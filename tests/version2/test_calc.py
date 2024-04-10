import numpy as np
import pytest
from pathlib import Path

from toshi_hazard_post.version2.logic_tree import HazardLogicTree
from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree

from toshi_hazard_post.version2.aggregation_calc import calc_composite_rates, build_branch_rates
from toshi_hazard_post.version2.data import ValueStore

NLEVELS = 10
        

@pytest.fixture(scope='function')
def logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt_v1.0.4.json'
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt_v1.0.4.json'
    slt = SourceLogicTree.from_json(slt_filepath)
    glt = GMCMLogicTree.from_json(gmcm_filepath)
    return HazardLogicTree(slt, glt)

@pytest.fixture(scope='function')
def value_store_all(logic_tree):

    value_store = ValueStore()
    for i, branch in enumerate(logic_tree.component_branches):
        values = np.linspace(0,1,10) * i
        value_store.set_values(values, branch)
    return value_store


@pytest.fixture(scope='function')
def value_store_small(logic_tree):

    value_store = ValueStore()
    for i, branch in enumerate(next(logic_tree.composite_branches).branches):
        values = np.linspace(0,1,10) * i
        value_store.set_values(values, branch)
    return value_store


# here we use value_store_all to make sure that component_branches and composite_branches.branches load into ValueStore the same way
def test_calc_composite_rates1(logic_tree, value_store_all):
    rates = calc_composite_rates(next(logic_tree.composite_branches), value_store_all, NLEVELS)
    assert rates.shape == (NLEVELS, )

# here we use values_store_small to make sure that the calculated rate is correct
def test_calc_composite_rates2(logic_tree, value_store_small):
    rates_expected = np.zeros((NLEVELS,))
    rates = calc_composite_rates(next(logic_tree.composite_branches), value_store_small, NLEVELS)
    for i in range(len(next(logic_tree.composite_branches).branches)):
        rates_expected += np.linspace(0, 1, NLEVELS) * i
    
    assert np.array_equal(rates, rates_expected)


def test_build_branch_rates1(logic_tree, value_store_all):

    rates = build_branch_rates(logic_tree, value_store_all, NLEVELS)
    nbranches = len(list(logic_tree.composite_branches))
    assert rates.shape == (nbranches, NLEVELS)
