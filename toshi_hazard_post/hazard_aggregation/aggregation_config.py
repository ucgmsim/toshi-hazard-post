"""Handle the standard configuration file."""

import logging
from pathlib import Path

import toml

log = logging.getLogger(__name__)


class AggregationConfig:
    """Aggregation configuration is a toml file and a related json file.

    The json file containing logic_tree_permutations and hazard_solutions.
    """

    def __init__(self, config):
        """Create a new AggregationConfig object."""
        self._config_file = config
        self.config = toml.load(config)
        self.validate()

        self.hazard_model_id = self.config['aggregation']['hazard_model_id']
        self.hazard_gts = self.config['aggregation'].get('gtids')
        self.aggs = self.config['aggregation']['aggs']
        self.lt_config = Path(
            self.config['aggregation']['logic_tree_file']
        )  # TODO: offer alternatives to loading a file (e.g. get model by version or a serialized SourceLogicTree)
        assert self.lt_config.exists()
        self.stride = self.config['aggregation'].get('stride')

        self.imts = self.config['aggregation'].get('imts')
        self.vs30s = self.config['aggregation'].get('vs30s')
        self.locations = self.config['aggregation'].get('locations')

        self.save_rlz = False
        if not self.config.get('deaggregation'):
            self.deaggregation = False
            self.save_rlz = self.config['aggregation'].get('save_rlz')
            self.validate_agg()
        else:
            self.deaggregation = True
            
            self.deagg_dimensions = list(map(str.lower, self.config['deaggregation']['dimensions']))
            self.inv_time = self.config['deaggregation'].get('inv_time')
            self.deagg_agg_targets = self.config['deaggregation'].get('agg_targets')
            self.poes = self.config['deaggregation'].get('poes')
            self.deagg_hazard_model_target = self.config['deaggregation'].get('hazard_model_target')
            self.validate_deagg()

        # debug/test option defaults
        self.location_limit = 0
        self.source_branches_truncate = 0
        self.reuse_source_branches_id = None
        self.run_serial = False
        self.skip_save = False
        if self.config.get('debug'):
            self.skip_save = self.config['debug'].get('skip_save')
            self.location_limit = self.config.get('debug').get('location_limit', 0)
            self.source_branches_truncate = self.config.get('debug').get('source_branches_truncate', 0)
            self.reuse_source_branches_id = self.config.get('debug').get('reuse_source_branches_id')
            self.run_serial = self.config.get('debug').get('run_serial')

    def validate(self):
        """Check the configuration is valid."""
        print(self.config['aggregation'])
        assert self.config['aggregation']['hazard_model_id']
        # assert self.config['aggregation']['gtids']
        assert self.config['aggregation']['aggs']
        assert self.config['aggregation']['logic_tree_file']

    def validate_agg(self):
        assert self.hazard_gts
        assert self.imts
        assert self.vs30s
        assert self.locations

    def validate_deagg(self):
        """Check the deagg configuration is valid."""
        print(self.config['deaggregation'])
        assert len(self.deagg_dimensions) == len(set(self.deagg_dimensions))

        valid_dimensions = ['eps', 'dist', 'mag', 'trt']
        assert all([dim in valid_dimensions for dim in self.deagg_dimensions])

        dspec = bool(
            self.inv_time or
            self.deagg_agg_targets or
            self.poes or
            self.imts or 
            self.vs30s or 
            self.deagg_hazard_model_id
        )
        if self.hazard_gts and dspec:
            raise Exception("for disaggregation you can only provied EITHER gtid(s) or a disagg spec")
        assert self.hazard_gts or dspec
