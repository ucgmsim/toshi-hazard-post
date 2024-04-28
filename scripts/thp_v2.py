import logging

import click

from toshi_hazard_post.version2.aggregation import run_aggregation
from toshi_hazard_post.version2.aggregation_config import AggregationConfig

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('toshi_hazard_post').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_post.version2.aggregation_calc').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.version2.aggregation').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.version2.aggregation_calc').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.version2.logic_tree').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_post.version2').setLevel(logging.INFO)


@click.group()
def thp():
    pass


@thp.command(name='aggregate', help='aggregate hazard curves')
@click.argument('config_file', type=click.Path(exists=True))
def aggregate(config_file):

    config = AggregationConfig(config_file)
    click.echo("Toshi Hazard Post: hazard curve aggregation")
    click.echo("=================================================")
    run_aggregation(config)


if __name__ == "__main__":
    thp()  # pragma: no cover
