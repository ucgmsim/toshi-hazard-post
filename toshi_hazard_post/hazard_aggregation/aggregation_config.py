"""Handle the standard configuration file."""

import json
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
        self.stage = self.config['aggregation']['stage']
        self.imts = self.config['aggregation']['imts']
        self.vs30s = self.config['aggregation']['vs30s']
        self.aggs = self.config['aggregation']['aggs']
        self.locations = self.config['aggregation']['locations']
        self._load_ltf()

        # debug/test option defaults
        self.location_limit = 0
        self.source_branches_truncate = 0
        self.reuse_source_branches_id = None
        if self.config.get('debug'):
            self.location_limit = self.config.get('debug').get('location_limit', 0)
            self.source_branches_truncate = self.config.get('debug').get('source_branches_truncate', 0)
            self.reuse_source_branches_id = self.config.get('debug').get('reuse_source_branches_id')

        # deaggregation
        #TODO: that's a lot of repeated config parameters. Do we want to make the config files seperate?
        self.deaggregation = False
        if self.config.get('deaggregation'):
            self.deaggregation = True
            self.deagg_poes = self.config.get('deaggregation').get('poes')
            self.deagg_aggs = self.config.get('deaggregation').get('aggs')
            self.deagg_imts = self.config.get('deaggregation').get('imts')
            self.deagg_invtime = self.config.get('deaggregation').get('inv_time')
            self.deagg_vs30s = self.config.get('deaggregation').get('vs30s')
            self.deagg_nbranches = self.config.get('deaggregation').get('num_branches', 150)
            self.deagg_metric = self.config.get('deaggregation').get('metric', 'distance')


    def _load_ltf(self):
        ltf = Path(Path(self._config_file).parent, self.config['aggregation']['logic_tree_file'])
        assert ltf.exists()
        self.logic_tree_permutations = json.load(ltf.open('r'))['logic_tree_permutations']
        self.hazard_solutions = json.load(ltf.open('r'))['hazard_solutions']
        self.src_correlations = json.load(ltf.open('r')).get('src_correlations')
        self.gmm_correlations = json.load(ltf.open('r')).get('gmm_correlations')

    def validate(self):
        """Check the configuration is valid."""
        print(self.config['aggregation'])
        assert self.config['aggregation']['hazard_model_id']
        assert self.config['aggregation']['stage']
        assert self.config['aggregation']['imts']
        assert self.config['aggregation']['vs30s']
        assert self.config['aggregation']['aggs']
        assert self.config['aggregation']['locations']
        assert self.config['aggregation']['logic_tree_file']

    def validate_deagg(self):
        """Check the deaggregation configuration is valid."""
        print(self.config['deaggregation'])
        assert self.config['deaggregation']['poes']
        assert self.config['deaggregation']['aggs']
        assert self.config['deaggregation']['inv_time']
        assert (self.deagg_metric == 'distance') | (self.deagg_metric == 'weight') | (self.deagg_metric == 'product')
        

