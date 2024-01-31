"""ToshiAPI interface utility & helpers."""

# TODO: refactor to single ToshiApi class (if possible)

import ast
import json
import logging
from collections import namedtuple
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional, Union

from nshm_toshi_client.toshi_file import ToshiFile

from toshi_hazard_post.local_config import API_KEY, API_URL, S3_URL, WORK_PATH
from toshi_hazard_post.util import archive

log = logging.getLogger(__name__)

DeaggConfig = namedtuple("DeaggConfig", "vs30 imt location poe inv_time deagg_agg_target")


def args_to_dict(args: List) -> Dict:

    return {arg['k']: arg['v'] for arg in args}


def get_deagg_config(data: Dict[str, Any]) -> DeaggConfig:

    node = data['data']['node1']['children']['edges'][0]
    args = node['node']['child']['arguments']
    args = args_to_dict(args)
    deagg_agg_target = args['agg']
    location = ast.literal_eval(args['location_list'])[0]
    vs30 = int(args['vs30'])
    imt = args['imt']
    poe = float(args['poe'])
    inv_time = int(args['inv_time'])

    # for arg in args:
    #     if arg['k'] == "disagg_config":
    #         disagg_args = json.loads(arg['v'].replace("'", '"').replace('None', 'null'))
    #     if arg['k'] == "hazard_agg_target":
    #         deagg_agg_target = arg['v']

    # location = disagg_args['location']
    # vs30 = int(disagg_args['vs30'])
    # imt = disagg_args['imt']
    # poe = float(disagg_args['poe'])
    # inv_time = int(disagg_args['inv_time'])
    return DeaggConfig(vs30, imt, location, poe, inv_time, deagg_agg_target)


def get_imtl(gtdata: Dict[str, Any]) -> Optional[float]:

    for arg in gtdata['data']['node1']['children']['edges'][0]['node']['child']['arguments']:
        # if arg['k'] == 'disagg_config':
        #     return json.loads(arg['v'].replace("'", '"').replace('None', 'null'))['level']
        if arg['k'] == 'level':
            return float(arg['v'])

    return None


def create_archive(filename: Union[str, Path], working_path: Union[str, PurePath]) -> str:
    """Verify source and if OK return the path to the zipped contents."""
    log.info(f"create_archive {filename}.zip in working_path={working_path}")
    if Path(filename).exists():
        return archive(filename, Path(working_path, f"{Path(filename).name}.zip"))
    raise Exception("file does not exist.")


def sanitize_json(jstring):
    return jstring.replace("'", '"').replace('None', 'null').replace("False", 'false').replace("True", 'true')


class SourceSolutionMap:
    """A mapping between nrml ids and hazard solution ids"""

    def __init__(self, hazard_jobs: List[dict] = []) -> None:
        self._dict: Dict[str, str] = {}
        if hazard_jobs:
            for job in hazard_jobs:
                jargs = args_to_dict(job['node']['child']['arguments'])
                lt = json.loads(sanitize_json(jargs['srm_logic_tree']))
                sources = lt['fault_systems'][0]['branches'][0]['sources']
                onfault_ids = list(filter(lambda x: x['type'] == 'inversion', sources))
                dsm_ids = list(filter(lambda x: x['type'] == 'distributed', sources))
                onfault_nrml_id = onfault_ids[0]['nrml_id'] if onfault_ids else ''
                distributed_nrml_id = dsm_ids[0]['nrml_id'] if dsm_ids else ''
                hazard_solution = job['node']['child']['hazard_solution']
                if (
                    hazard_solution
                ):  # don't return a result if there is no solution (usually due to no ruptures near the site)
                    self._dict[self.__key(onfault_nrml_id, distributed_nrml_id)] = hazard_solution['id']
        return

    def append(self, other: 'SourceSolutionMap'):
        self._dict.update(other._dict)

    def get_solution_id(self, *, onfault_nrml_id: str, distributed_nrml_id: str) -> Optional[str]:
        return self._dict.get(self.__key(onfault_nrml_id, distributed_nrml_id))

    @staticmethod
    def __key(onfault_nrml_id: str, distributed_nrml_id: str) -> str:
        return ':'.join((str(onfault_nrml_id), str(distributed_nrml_id)))


class ToshiApi(ToshiFile):
    def save_sources_to_toshi(self, filepath: Union[str, Path], tag: Optional[str] = None) -> str:
        """Archive and upload one file."""
        log.info(f"Processing */{Path(filepath).name} :: {tag}")

        archive_path = create_archive(filepath, WORK_PATH)
        log.info(f'archived {filepath} in {archive_path}.')

        filename = Path(filepath).name

        meta = dict(filename=filename)
        if tag:
            meta['tag'] = str(tag)

        archive_file_id, post_url = self.create_file(Path(archive_path), meta=meta)
        self.upload_content(post_url, archive_path)
        log.info(f"pushed {archive_path} to ToshiAPI {API_URL} with id {archive_file_id}")
        return archive_file_id

    def get_disagg_gt(self, general_task_id: str) -> Dict[str, Any]:
        qry = '''
        query disagg_gt ($general_task_id:ID!) {
            node1: node(id: $general_task_id) {
            id
            ... on GeneralTask {
              subtask_count
              children {
                total_count
                edges {
                  node {
                    child {
                      ... on OpenquakeHazardTask {
                        arguments {k v}
                        result
                        hazard_solution {
                          id
                          csv_archive { id }
                          hdf5_archive { id }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        '''

        print(qry)
        input_variables = dict(general_task_id=general_task_id)
        executed = self.run_query(qry, input_variables)
        return {'data': executed}

    def get_hazard_gt(self, id: str) -> SourceSolutionMap:

        qry = '''
        query hazard_gt ($general_task_id:ID!) {
            node1: node(id: $general_task_id) {
                id
                ... on GeneralTask {
                    children {
                        total_count
                        edges {
                            node {
                                child {
                                    ... on OpenquakeHazardTask {
                                        arguments {
                                            k v
                                        }
                                        result
                                        hazard_solution {
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
        '''

        input_variables = dict(general_task_id=id)
        executed = self.run_query(qry, input_variables)
        if executed.get('node1'):
            return SourceSolutionMap(executed['node1']['children']['edges'])
        else:
            return SourceSolutionMap()


headers = {"x-api-key": API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
