"""
Main command-line interface for dp_tools.
"""
import os
import sys
import glob
from pathlib import Path
import click

from dp_tools.utils.move import move_files


@click.group()
def dp_tools():
    """Tools used for DP tasks"""
    pass


class MutuallyExclusiveOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help_text = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = f"{help_text} NOTE: This option is mutually exclusive with {ex_str}."
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        current_opt = self.name in opts
        for mutex_opt in self.mutually_exclusive:
            if mutex_opt in opts:
                if current_opt:
                    raise click.UsageError(
                        f"Illegal usage: {self.name} is mutually exclusive with {mutex_opt}."
                    )
        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


@dp_tools.command()
@click.option("--assay", cls=MutuallyExclusiveOption, mutually_exclusive=["plugin"],
              help="Built-in assay type to use (e.g., rnaseq, atacseq)")
@click.option("--plugin", cls=MutuallyExclusiveOption, mutually_exclusive=["assay"],
              type=click.Path(exists=True, file_okay=False), 
              help="Directory containing assay configs (replaces --assay)")
@click.option("--component", required=True, help="Component (e.g., raw_reads, trimmed_reads)")
@click.option("--outdir", required=True, type=click.Path(), help="Output directory")
@click.option("--input", "-i", multiple=True, help="Input files or directories in format 'key:path'. Can be used multiple times.")
@click.option("--use-symlinks", is_flag=True, default=True, help="Create symbolic links instead of copying files")
@click.option("--dry-run", is_flag=True, help="Only print what would be done")
def move(assay, plugin, component, outdir, input, use_symlinks, dry_run):
    """
    Move files according to assay-specific layouts.
    
    This command organizes files according to the structure defined by either:
    1. A built-in assay type (--assay), or
    2. A custom plugin directory (--plugin)
    
    Examples:
        # Using built-in assay config
        dp_tools move --assay rnaseq --component raw_reads --outdir ./results \\
            -i raw_fastq:./fastq/ -i raw_fastqc:./fastqc/ -i raw_multiqc:./multiqc/
            
        # Using custom plugin directory
        dp_tools move --plugin ./my_assays --component raw_reads --outdir ./results \\
            -i raw_fastq:./fastq/ -i raw_fastqc:./fastqc/
    """
    # Ensure either assay or plugin is provided
    if not assay and not plugin:
        click.echo("Error: Either --assay or --plugin must be specified", err=True)
        sys.exit(1)
    
    # Parse input arguments
    file_paths = {}
    for item in input:
        if ":" not in item:
            click.echo(f"Error: Input '{item}' is not in the format 'key:path'", err=True)
            continue
            
        key, path = item.split(":", 1)
        file_paths[key] = path
    
    if not file_paths:
        click.echo("Error: No valid input files specified", err=True)
        sys.exit(1)
    
    # Process glob patterns in file paths
    for key, value in list(file_paths.items()):
        if "*" in value:
            matching_files = glob.glob(value)
            if matching_files:
                # If it's a glob pattern, expand it
                file_paths[key] = matching_files
            else:
                click.echo(f"Warning: No files found matching '{value}' for {key}", err=True)
                del file_paths[key]
    
    # Collect all files from file paths
    all_files = []
    for key, value in file_paths.items():
        if isinstance(value, list):
            all_files.extend([Path(f) for f in value])
        elif os.path.isdir(value):
            all_files.extend([Path(value) / f for f in os.listdir(value) if os.path.isfile(os.path.join(value, f))])
        elif os.path.isfile(value):
            all_files.append(Path(value))
    
    click.echo(f"Found {len(all_files)} files to move")
    
    try:
        # If using plugin, configure plugin path and assay name
        if plugin:
            plugin_dir = plugin
            # Use the plugin directory name as the assay type
            plugin_assay = os.path.basename(os.path.normpath(plugin))
        else:
            plugin_dir = None
            plugin_assay = assay
            
        # Move files using assay-specific layout
        result = move_files(
            assay_type=plugin_assay,
            component=component,
            files=all_files,
            output_dir=outdir,
            dry_run=dry_run,
            use_symlinks=use_symlinks,
            plugin_dir=plugin_dir
        )
        
        # Print summary
        if dry_run:
            click.echo("\nDry run summary:")
        else:
            action = "Linked" if use_symlinks else "Moved"
            click.echo(f"\n{action} files summary:")
            
        for target_dir, moved_files in result.items():
            if moved_files:
                click.echo(f"  {target_dir}: {len(moved_files)} files")
                # Show a few examples
                if len(moved_files) <= 5:
                    for f in moved_files:
                        click.echo(f"    - {os.path.basename(str(f))}")
    
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@dp_tools.command()
def version():
    """Display the current version of dp_tools."""
    import pkg_resources
    try:
        version = pkg_resources.get_distribution("dp_tools").version
        click.echo(f"dp_tools version {version}")
    except pkg_resources.DistributionNotFound:
        click.echo("dp_tools version unknown (package not installed)")


if __name__ == "__main__":
    dp_tools() 