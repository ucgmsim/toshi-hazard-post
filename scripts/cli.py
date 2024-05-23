import logging

import click

import toshi_hazard_post.local_config as local_config
from toshi_hazard_post.aggregation import run_aggregation
from toshi_hazard_post.aggregation_args import AggregationArgs

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('toshi_hazard_post').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_post.aggregation_calc').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.aggregation').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.aggregation_calc').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.logic_tree').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.parallel').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post').setLevel(logging.INFO)


@click.group()
def thp():
    pass


@thp.command(name='aggregate', help='aggregate hazard curves')
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--config-file', type=click.Path(exists=True))
def aggregate(input_file, config_file):

    if config_file:
        local_config.config_override_filepath = config_file

    args = AggregationArgs(input_file)
    click.echo("Toshi Hazard Post: hazard curve aggregation")
    click.echo("=================================================")
    run_aggregation(args)


if __name__ == "__main__":
    thp()  # pragma: no cover
