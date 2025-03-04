"""ISA-related CLI commands for dp_tools.

Provides commands for:
- Downloading ISA archives
- Converting ISA archives to runsheets
"""

import click
from pathlib import Path
import warnings

# Import the original functions
from dp_tools.glds_api.isa import download_isa_archive
from dp_tools.scripts.convert import convert_isa_to_runsheet

@click.group(name="isa")
def isa():
    """Work with ISA archives.
    
    Commands for downloading and processing ISA archives.
    """
    pass

@isa.command(name="get")
@click.argument("accession")
@click.option("--output-dir", "-o", default=".", help="Directory to save the ISA archive to.")
def get_isa(accession, output_dir):
    """Download an ISA archive for a given accession.
    
    The accession argument should be a GLDS or OSD accession number (e.g., GLDS-168 or OSD-168).
    
    This command handles both traditional ISA archives (GLDS-XXX-ISA.zip) and
    metadata ISA archives (OSD-XXX_metadata_OSD-XXX-ISA.zip).
    """
    download_isa_archive(accession, output_dir)

@isa.command(name="to-runsheet")
@click.option("--accession", required=True, help="Dataset accession (e.g., GLDS-168 or OSD-249)")
@click.option("--config-type", "--assay", "-t", required=True, 
              help="Packaged config type to use (e.g., bulkRNASeq, amplicon)")
@click.option("--config-version", "-v", default="Latest",
              help="Packaged config version to use (default: Latest)")
@click.option("--isa-archive", "--isa", "-a", required=True,
              help="Path to the ISA archive file. Can be downloaded with 'dpt isa get'")
@click.option("--output-dir", "-o", default=".", 
              help="Directory to save the output runsheet to.")
def to_runsheet(accession, config_type, config_version, isa_archive, output_dir):
    """Convert an ISA archive to a runsheet.
    
    This command works with ISA archives from both GLDS and OSD accessions.
    """
    convert_isa_to_runsheet(
        accession=accession,
        config_type=config_type,
        config_version=config_version,
        isa_archive=isa_archive,
        output_dir=output_dir
    )

# Keep convert as an alias for backward compatibility
@isa.command(name="convert", hidden=True)
@click.argument("accession")
@click.option("--config-type", "-t", required=True, 
              help="Packaged config type to use (e.g., bulkRNASeq, microarray)")
@click.option("--config-version", "-v", required=True,
              help="Packaged config version to use (e.g., Latest)")
@click.option("--isa-archive", "-a", required=True,
              help="Path to the ISA archive file. Can be downloaded with 'dpt isa get'")
@click.option("--output-dir", "-o", default=".", 
              help="Directory to save the output runsheet to.")
def convert_isa(accession, config_type, config_version, isa_archive, output_dir):
    """[DEPRECATED] Use 'to-runsheet' instead.
    
    Convert an ISA archive to a runsheet.
    
    The accession argument should be a GLDS or OSD accession number (e.g., GLDS-168 or OSD-168).
    """
    warnings.warn(
        "The 'convert' command is deprecated and will be removed in a future version. "
        "Please use 'to-runsheet' instead.",
        DeprecationWarning, stacklevel=2
    )
    convert_isa_to_runsheet(
        accession=accession,
        config_type=config_type,
        config_version=config_version,
        isa_archive=isa_archive,
        output_dir=output_dir
    )

# Add backward compatibility wrappers with deprecation warnings
def deprecated_get_isa_archive():
    """Deprecated entry point for dpt-get-isa-archive."""
    warnings.warn(
        "The 'dpt-get-isa-archive' command is deprecated and will be removed in a future version. "
        "Please use 'dpt isa get' instead.", 
        DeprecationWarning, stacklevel=2
    )
    from dp_tools.glds_api.isa import main
    main()

def deprecated_isa_to_runsheet():
    """Deprecated entry point for dpt-isa-to-runsheet."""
    warnings.warn(
        "The 'dpt-isa-to-runsheet' command is deprecated and will be removed in a future version. "
        "Please use 'dpt isa to-runsheet' instead.", 
        DeprecationWarning, stacklevel=2
    )
    from dp_tools.scripts.convert import main
    main() 