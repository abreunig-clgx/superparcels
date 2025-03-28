import sys
import logging
import click
import sp_cmds
from helper import get_config_path

logging.basicConfig(
    level=logging.INFO,  # or INFO based on verbose
    format="%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,     # helps avoid Click capturing stdout
    force=True             # wipe any existing handlers
)

@click.group(
    help="A CLI Tool for Creating Super Parcels",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Enable verbose mode.")
@click.pass_context
def cli(ctx, verbose):
    """Main entry point for the WF-Agg CLI tool."""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["CONFIG"] = get_config_path()

    
    # Set up logging based on the verbose flag
    logging_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=logging_level)
    
cli.add_command(sp_cmds.config)
cli.add_command(sp_cmds.build)
cli.add_command(sp_cmds.dt_analysis)


