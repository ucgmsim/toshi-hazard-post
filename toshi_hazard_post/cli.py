"""Console script for toshi_hazard_post."""

import click


@click.command()
def main():
    """Main entrypoint."""
    click.echo("toshi-hazard-post")
    click.echo("=" * len("toshi-hazard-post"))
    click.echo("Hazard post-processing pipeline as serverless AWS infrastructure.")


if __name__ == "__main__":
    main()  # pragma: no cover
