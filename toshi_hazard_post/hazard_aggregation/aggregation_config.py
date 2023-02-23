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
        self.hazard_gts = self.config['aggregation']['gtids']
        self.aggs = self.config['aggregation']['aggs']
        self.lt_config = Path(
            self.config['aggregation']['logic_tree_file']
        )  # TODO: offer alternatives to loading a file (e.g. get model by version or a serialized SourceLogicTree)
        assert self.lt_config.exists()
        self.stride = self.config['aggregation'].get('stride')

        if not self.config.get('debug'):
            self.imts = self.config['aggregation']['imts']
            self.vs30s = self.config['aggregation']['vs30s']
            self.locations = self.config['aggregation']['locations']
            self.save_rlz = self.config['aggregation']['save_rlz']
            self.deaggregation = False
        else:
            self.deaggregation = True
            self.deagg_dimensions = self.config['deaggregation']['dimensions']
            self.validate_deagg(self)

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
        assert self.config['aggregation']['gtids']
        assert self.config['aggregation']['aggs']
        assert self.config['aggregation']['logic_tree_file']


    def validate_deagg(self):
        """Check the deagg configuration is valid."""
        print(self.conifg['deggregation'])
        assert len(self.deagg_dimensions) == len(set(self.deagg_dimensions))
        
        valid_dimensions = ['eps','dist','mag','trt']
        assert all([dim in valid_dimensions for dim in self.deagg_dimensions])