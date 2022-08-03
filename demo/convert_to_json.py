"""Convert a python  THS configuration to THP json form."""
import json

from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import data as gtdata
from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import logic_tree_permutations

data = dict(logic_tree_permutations=logic_tree_permutations, hazard_solutions=gtdata)

print(json.dumps(data, indent=2))


def process_gt(GTID):
    from copy import copy  # noqa

    query = """
    query BIG_ASS_SLT {
      node1: node(id: "R2VuZXJhbFRhc2s6MTA4NTU5") {
        id
        ... on GeneralTask {
          children {
            total_count
            edges {
              node {
                child {
                  ... on OpenquakeHazardTask {
                    result
                    arguments {
                      k
                      v
                    }
                    result
                    hazard_solution {
                      id
                      modified_config {
                        id
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }"""

    sss = execute(query)  # noqa
    dd = json.loads(sss)
    succeeded = filter(
        lambda obj: obj['node']['child']['result'] == 'SUCCESS', dd['data']['node1']['children']['edges']
    )
    new_edges = list(succeeded)
    new_data = copy(dd)
    new_data['data']['node1']['children']['edges'] = new_edges

    new_data = copy(dd)
    new_data['data']['node1']['children']['edges'] = new_edges
    ff = open('success.json', 'w')
    ff.write(json.dumps(new_data, indent=2))
