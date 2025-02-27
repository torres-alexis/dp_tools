"""
Main command-line interface for dp_tools.
"""
import os
import sys
import glob
from pathlib import Path
import click

from dp_tools.utils.mover import move_files


@click.group()
def dpt():
    """Data processing tools for NGS pipelines."""
    pass


@dpt.command()
@click.option("--assay", required=True, help="Assay type (e.g., rnaseq, atacseq)")
@click.option("--component", required=True, help="Component (e.g., raw_reads, trimmed_reads)")
@click.option("--outdir", required=True, type=click.Path(), help="Output directory")
@click.option("--input", "-i", multiple=True, help="Input files or directories in format 'key:path'. Can be used multiple times.")
@click.option("--use-symlinks", is_flag=True, default=True, help="Create symbolic links instead of copying files")
@click.option("--dry-run", is_flag=True, help="Only print what would be done")
def mover(assay, component, outdir, input, use_symlinks, dry_run):
    """
    Move files according to assay-specific layouts.
    
    This command is a CLI wrapper around the move_files function in dp_tools.utils.mover.
    It processes command line arguments, handles file patterns, and calls the core
    implementation to organize files according to component-specific structure definitions.
    
    The component (e.g., raw_reads) defines the directory structure where files will be placed
    based on their type. Each component has a STRUCTURE dictionary that maps file types to 
    output directories.
    
    Examples:
        # Move raw reads files
        dpt mover --assay rnaseq --component raw_reads --outdir ./results \\
            -i raw_fastq:./fastq/ -i raw_fastqc:./fastqc/ -i raw_multiqc:./multiqc/
            
        # Use glob patterns to match files
        dpt mover --assay rnaseq --component raw_reads --outdir ./results \\
            -i raw_fastq:"./*_R?.fastq.gz" -i raw_fastqc:"./*_fastqc.*"
    """
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
        # Move files using assay-specific layout
        result = move_files(
            assay_type=assay,
            component=component,
            files=all_files,
            output_dir=outdir,
            dry_run=dry_run,
            use_symlinks=use_symlinks
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


if __name__ == "__main__":
    dpt() 