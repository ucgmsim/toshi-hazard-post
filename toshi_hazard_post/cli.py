"""Console script for toshi_hazard_post."""

import logging
import os

import click

from toshi_hazard_post.hazard_aggregation import (
    AggregationConfig,
    distribute_aggregation,
    process_aggregation,
    push_test_message,
)

log = logging.getLogger()

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('nshm_toshi_client.toshi_client_base').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
# logging.getLogger('git.cmd').setLevel(logging.INFO)
# logging.getLogger('gql.transport').setLevel(logging.WARN)
logging.getLogger('pynamodb').setLevel(logging.INFO)


logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)


@click.command()
@click.option(
    '--mode',
    '-m',
    default=lambda: os.environ.get("NZSHM22_THP_MODE", 'LOCAL'),
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=True),
)
@click.option('--push-test', '-pt', is_flag=True)
@click.argument('config', type=click.Path(exists=True))  # help="path to a valid THP configuration file."
def main(config, push_test, mode):
    """Main entrypoint."""
    click.echo("Hazard post-processing pipeline as serverless AWS infrastructure.")
    click.echo(f"mode: {mode}")

    agconf = AggregationConfig(config)
    # click.echo(agconf)
    log.info("Doit")

    if push_test:
        push_test_message()
        return

    if mode == 'LOCAL':
        process_aggregation(agconf, 'prefix')
        return
    if mode == 'AWS':
        distribute_aggregation(agconf)
        return


if __name__ == "__main__":
    main()  # pragma: no cover
