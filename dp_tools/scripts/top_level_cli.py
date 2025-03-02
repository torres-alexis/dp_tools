"""Connect all sub cli to a top level namespace."""

import sys
import click
from click import Context
from click.exceptions import Exit

from dp_tools.scripts.vv_interface import validation
from dp_tools.scripts.isa_cli import isa
from dp_tools.scripts.osd_api_cli import osd
from dp_tools.scripts.data_assets_cli import data_assets

# Create a custom Click group with better error handling
class BetterClickGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        # Handle 'help' command
        if cmd_name == 'help':
            click.echo(ctx.get_help())
            ctx.exit(0)
            
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
            
        # No such command - provide friendly error
        click.echo(f"Error: '{cmd_name}' is not a valid command.\n", err=True)
        click.echo("Available commands:", err=True)
        for cmd in sorted(self.list_commands(ctx)):
            click.echo(f"  {cmd}", err=True)
        click.echo("\nRun 'dp_tools --help' for more information.", err=True)
        ctx.exit(2)

@click.command(cls=BetterClickGroup, context_settings={
    "help_option_names": ["--help", "-h"],
    "token_normalize_func": lambda x: x.lower(),
})
def cli():
    """dp_tools command line interface.
    
    A toolkit for data processing operations, including:
    - ISA archive handling
    - Validation and verification
    - Data asset management
    - OSD API interactions
    
    This CLI can be accessed using either the 'dp_tools' command (recommended)
    or the shorter 'dpt' alias.
    """
    pass

# Add subcommands to the CLI
cli.add_command(validation)
cli.add_command(isa)
cli.add_command(osd)
cli.add_command(data_assets)

def main():
    """Entry point for the CLI."""
    try:
        cli()
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        click.echo("Please report this issue at https://github.com/torres-alexis/dp_tools/issues", err=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
