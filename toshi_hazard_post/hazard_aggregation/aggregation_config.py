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
        # self.stage = self.config['aggregation']['stage']  # not used
        self.imts = self.config['aggregation']['imts']

        # TODO: can we remove vs30s and just assume we're processing one at a time? Get vs30 (needed for THS) from
        # ToshiAPI?
        self.vs30s = self.config['aggregation']['vs30s']

        self.aggs = self.config['aggregation']['aggs']
        self.locations = self.config['aggregation'].get('locations')
        self.save_rlz = self.config['aggregation'].get('save_rlz')
        self.stride = self.config['aggregation'].get('stride')
        # self._load_ltf()
        self.hazard_gts = self.config['aggregation']['gtids']
        self.lt_config = Path(
            self.config['aggregation']['logic_tree_file']
        )  # TODO: offer alternatives to loading a file (e.g. get model by version or a serialized SourceLogicTree)
        assert self.lt_config.exists()

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

        # deaggregation
        # TODO: that's a lot of repeated config parameters. Do we want to make the config files seperate?
        self.deaggregation = False
        if self.config.get('deaggregation'):
            self.deaggregation = True
            self.deagg_poes = self.config.get('deaggregation').get('poes')
            self.deagg_invtime = self.config.get('deaggregation').get('inv_time')
            # self.deagg_gtids = self.config.get('deaggregation').get('gtids')
            self.deagg_dimensions = self.config.get('deaggregation').get('dimensions')

    # def _load_deagg(self):
    # ltf = Path(Path(self._config_file).parent, self.config['deaggregation']['gtdata_file'])
    # assert ltf.exists()
    # self.deagg_solutions = json.load(ltf.open('r'))['deagg_solutions']

    # def _load_ltf(self):
    #     ltf = Path(Path(self._config_file).parent, self.config['aggregation']['logic_tree_file'])
    #     assert ltf.exists()
    #     self.logic_tree_permutations = json.load(ltf.open('r'))['logic_tree_permutations']
    #     self.hazard_solutions = json.load(ltf.open('r'))['hazard_solutions']
    #     self.src_correlations = json.load(ltf.open('r')).get('src_correlations')
    #     self.gmm_correlations = json.load(ltf.open('r')).get('gmm_correlations')

    def validate(self):
        """Check the configuration is valid."""
        print(self.config['aggregation'])
        assert self.config['aggregation']['hazard_model_id']
        # assert self.config['aggregation']['stage']  # not used
        assert self.config['aggregation']['imts']
        assert self.config['aggregation']['vs30s']
        assert self.config['aggregation']['aggs']
        assert self.config['aggregation']['locations']
        assert self.config['aggregation']['logic_tree_file']

    def validate_deagg(self):
        """Check the deaggregation configuration is valid."""
        print(self.config['deaggregation'])
        assert self.config['deaggregation']['inv_time']

    def validate_deagg_file(self):
        """Check that the deagg data file exists and load it"""
        # assert self.config['deaggregation']['gtdata_file']
        # ltf = Path(Path(self._config_file).parent, self.config['deaggregation']['gtdata_file'])
        # assert ltf.exists()
        # self._load_deagg()
