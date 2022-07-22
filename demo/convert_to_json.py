"""Convert a python  THS configuration to THP json form."""
import json

from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import data as gtdata
from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import logic_tree_permutations

data = dict(logic_tree_permutations=logic_tree_permutations, hazard_solutions=gtdata)

print(json.dumps(data, indent=2))
