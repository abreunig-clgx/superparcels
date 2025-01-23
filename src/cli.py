import logging
import click
import sp


@click.group(
    help="A CLI tool for the Wildfire Aggregation Products: CNA or GUARD.",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose mode.")
@click.pass_context
def cli(ctx, verbose):
    """Main entry point for the WF-Agg CLI tool."""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["CONFIG"] = 'config.json'

    
    # Set up logging based on the verbose flag
    logging_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=logging_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    )
    
cli.add_command(sp.setup)
cli.add_command(sp.build)


