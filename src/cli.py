import logging
import click
import sp_cmds
from helper import get_config_path
@click.group(
    help="A CLI Tool for Creating Super Parcels",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose mode.")
@click.pass_context
def cli(ctx, verbose):
    """Main entry point for the WF-Agg CLI tool."""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["CONFIG"] = get_config_path()

    
    # Set up logging based on the verbose flag
    logging_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=logging_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    )
    
cli.add_command(sp_cmds.config)
cli.add_command(sp_cmds.build)
cli.add_command(sp_cmds.dt_analysis)


