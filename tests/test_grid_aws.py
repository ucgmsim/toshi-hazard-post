import math

from toshi_hazard_post.hazard_grid.aws_gridded_hazard import tasks_by_chunk

def test_tasks_by_chunk():

    poe_levels = [0.02, 0.10, 0.2]
    hazard_model_ids = ['HAZARD_MODEL_A', 'HAZARD_MODEL_B']
    vs30s = [1, 2, 3]
    imts =  ['PGA', 'SA(0.2)', 'SA(0.5)', 'SA(1.0)', 'SA(3.0)']
    aggs =  ['mean', '0.9', '0.8']
    chunk_size = 4

    num_chunks = 0
    for chunk in tasks_by_chunk(
        poe_levels,
        hazard_model_ids,
        vs30s,
        imts,
        aggs,
        chunk_size,
    ):
        num_chunks += 1
    
    assert num_chunks == math.ceil(len(hazard_model_ids)*len(vs30s)*len(imts)*len(aggs)/4)


def test_tasks_by_chunk_tasks():

    poe_levels = [0.02, 0.1]
    hazard_model_ids = ['HAZARD_MODEL_A']
    vs30s = [1, 2, 3]
    imts =  ['PGA']
    aggs =  ['mean']
    chunk_size = 4

    poe_levels_out = []
    hazard_model_ids_out = []
    vs30s_out = []
    imts_out = []
    aggs_out = []
    for chunk in tasks_by_chunk(
        poe_levels,
        hazard_model_ids,
        vs30s,
        imts,
        aggs,
        chunk_size,
    ):
        poe_levels_out += chunk.poe_levels
        hazard_model_ids_out += chunk.hazard_model_ids
        vs30s_out += chunk.vs30s
        imts_out += chunk.imts
        aggs_out += chunk.aggs
    
    assert set(poe_levels) == set(poe_levels_out)
    assert set(hazard_model_ids) == set(hazard_model_ids_out)
    assert set(vs30s) == set(vs30s_out)
    assert set(imts) == set(imts_out)
    assert set(aggs) == set(aggs_out)
