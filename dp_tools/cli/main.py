"""
Main command-line interface for dp_tools.

This module provides the primary command-line interface for dp_tools,
including commands for moving files according to plugin-specific layouts,
retrieving ISA archives from GeneLab datasets, and displaying version information.
"""
import os
import sys
import glob
import re
from pathlib import Path
import click
import requests
import json
import logging

from dp_tools.utils.move import move_files
from dp_tools.utils.accession import get_osd_and_glds, DEFAULT_API_URL
from dp_tools.utils.logging import Status, ValidationLogger


@click.group()
def dp_tools():
    """Tools used for DP tasks"""
    pass


class MutuallyExclusiveOption(click.Option):
    """
    Custom Click option class that enforces mutual exclusivity with other options.
    
    This class extends Click's Option class to prevent certain options from being
    used together. When instantiated, it takes a list of option names that should
    be considered mutually exclusive with this option.
    """
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help_text = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = f"{help_text} NOTE: This option is mutually exclusive with {ex_str}."
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        """
        Override handle_parse_result to enforce mutual exclusivity.
        
        Args:
            ctx: Click context
            opts: Parsed options
            args: Parsed arguments
            
        Returns:
            Result of parent's handle_parse_result
            
        Raises:
            UsageError: If this option is used with a mutually exclusive option
        """
        current_opt = self.name in opts
        for mutex_opt in self.mutually_exclusive:
            if mutex_opt in opts:
                if current_opt:
                    raise click.UsageError(
                        f"Illegal usage: {self.name} is mutually exclusive with {mutex_opt}."
                    )
        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


@dp_tools.command()
@click.option("--plugin", required=True,
              type=click.Path(exists=True, file_okay=False), 
              help="Directory containing assay configs and components")
@click.option("--component", required=True, help="Component (e.g., raw_reads, trimmed_reads)")
@click.option("--outdir", required=True, type=click.Path(), help="Output directory")
@click.option("--input", "-i", multiple=True, help="Input files or directories in format 'key:path'. Can be used multiple times.")
@click.option("--use-symlinks", is_flag=True, default=True, help="Create symbolic links instead of copying files")
@click.option("--dry-run", is_flag=True, help="Only print what would be done")
def move(plugin, component, outdir, input, use_symlinks, dry_run):
    """
    Move files according to plugin-specific layouts.
    
    This command organizes files according to the structure defined by a plugin directory.
    
    Example:
        dp_tools move --plugin ./plugins/rnaseq --component raw_reads --outdir ./results \\
            -i raw_fastq:./fastq/ -i raw_fastqc:./fastqc/ -i raw_multiqc:./multiqc/
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
        # Move files using plugin-specific layout
        result = move_files(
            plugin_dir=plugin,
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


@dp_tools.command()
def version():
    """Display the current version of dp_tools."""
    import pkg_resources
    try:
        version = pkg_resources.get_distribution("dp_tools").version
        click.echo(f"dp_tools version {version}")
    except pkg_resources.DistributionNotFound:
        click.echo("dp_tools version unknown (package not installed)")


@dp_tools.command()
@click.argument('accession_arg', required=False)
@click.option('--accession', help="Accession in the format 'OSD-###' or 'GLDS-###'")
@click.option('--output-dir', '-o', type=click.Path(file_okay=False), default='.',
              help='Directory to save the ISA archive (default: current directory)')
@click.option('--force', '-f', is_flag=True, help='Overwrite existing files')
@click.option('--api-url', default=DEFAULT_API_URL,
              help='OSDR API URL (default: general OSDR API)')
@click.option('--plugin', type=click.Path(exists=True, file_okay=False), 
              help='Plugin directory containing config.yaml with API settings')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output for debugging')
def get_isa(accession_arg, accession, output_dir, force, api_url, plugin, verbose):
    """
    Download ISA archive for a GeneLab dataset.
    
    Specify the accession either as a positional argument or with --accession option.
    
    This command downloads the ISA archive for the specified GeneLab dataset
    and saves it to the specified output directory. For OSD accessions, it directly
    accesses the OSDR files API to find and download ISA archives. For GLDS accessions,
    it first resolves the OSD mapping via the search API.
    
    Examples:
        dp_tools get-isa GLDS-123
        dp_tools get-isa --accession OSD-456 -o ~/data/
        dp_tools get-isa --accession OSD-456 --plugin ./plugins/rnaseq
    """
    # Determine which accession to use
    if accession_arg and accession:
        click.echo("Error: Please provide the accession either as a positional argument or with --accession, not both.", err=True)
        sys.exit(1)
    
    # Use the positional argument if provided, otherwise use the --accession option
    final_accession = accession_arg if accession_arg else accession
    
    if not final_accession:
        click.echo("Error: No accession provided. Please specify either as a positional argument or with --accession.", err=True)
        sys.exit(1)
    
    # Validate accession format
    pattern = r'^(GLDS|OSD)-(\d+)$'
    match = re.match(pattern, final_accession)
    if not match:
        click.echo(f"Error: Invalid accession format '{final_accession}'. Must be OSD-# or GLDS-# (e.g., OSD-123 or GLDS-456).", err=True)
        sys.exit(1)
        
    accession_type = match.group(1)
    accession_num = match.group(2)
    
    # If plugin is specified, read the config.yaml to get the API URL
    if plugin:
        import yaml
        config_path = os.path.join(plugin, 'config.yaml')
        if not os.path.isfile(config_path):
            click.echo(f"Error: No config.yaml found in plugin directory: {plugin}", err=True)
            sys.exit(1)
            
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Get API URL from config
            if 'api_url' in config:
                api_url = config['api_url']
                if verbose:
                    click.echo(f"Using API URL from plugin config: {api_url}")
            else:
                click.echo(f"Warning: No api_url found in config.yaml. Using default API URL.", err=True)
        except Exception as e:
            click.echo(f"Error reading plugin config: {str(e)}", err=True)
            sys.exit(1)
    
    try:
        # For OSD accessions, go directly to the files endpoint
        if accession_type == "OSD":
            if verbose:
                click.echo(f"Directly accessing OSD files endpoint for {final_accession}")
            
            # Construct the OSD files API URL
            osd_files_url = f"https://osdr.nasa.gov/osdr/data/osd/files/{accession_num}"
            
            if verbose:
                click.echo(f"Using OSD files URL: {osd_files_url}")
            
            # Make the API request
            response = requests.get(osd_files_url)
            if response.status_code != 200:
                click.echo(f"Error: Failed to fetch files for {final_accession}. Status code: {response.status_code}", err=True)
                sys.exit(1)
                
            try:
                data = response.json()
                if not data.get('success'):
                    click.echo(f"Error: API returned unsuccessful response for {final_accession}", err=True)
                    sys.exit(1)
                    
                # Extract file information
                if verbose:
                    click.echo("Successfully retrieved file information")
                
                # Check if we have study files for this OSD
                if final_accession not in data.get('studies', {}):
                    click.echo(f"Error: No files found for {final_accession}", err=True)
                    sys.exit(1)
                
                study_files = data['studies'][final_accession].get('study_files', [])
                
                # Look for ISA archive files
                isa_files = [f for f in study_files if 'ISA' in f.get('file_name', '') and f.get('file_name', '').endswith('.zip')]
                
                if not isa_files:
                    click.echo(f"No ISA archive files found for {final_accession}")
                    # Show all available files
                    click.echo("Available files:")
                    for file in study_files:
                        click.echo(f"  - {file.get('file_name', 'Unknown')}")
                    sys.exit(0)
                
                # Process each ISA file
                click.echo(f"Found {len(isa_files)} ISA archive files for {final_accession}:")
                for isa_file in isa_files:
                    file_name = isa_file.get('file_name')
                    remote_url = isa_file.get('remote_url')
                    click.echo(f"  - {file_name}")
                    
                    # Create the full download URL
                    base_url = "https://osdr.nasa.gov"
                    full_url = f"{base_url}{remote_url}"
                    
                    # Determine the output file path
                    output_file = os.path.join(output_dir, file_name)
                    
                    # Check if file already exists
                    if os.path.exists(output_file) and not force:
                        click.echo(f"    File already exists: {output_file}")
                        click.echo(f"    Use --force to overwrite")
                        continue
                    
                    # Download the file
                    try:
                        if verbose:
                            click.echo(f"    Downloading from: {full_url}")
                            click.echo(f"    Saving to: {output_file}")
                        
                        # Ensure output directory exists
                        os.makedirs(output_dir, exist_ok=True)
                        
                        # Stream the download to handle large files efficiently
                        with requests.get(full_url, stream=True) as response:
                            response.raise_for_status()  # Raise an exception for HTTP errors
                            
                            # Get total file size for progress reporting
                            total_size = int(response.headers.get('content-length', 0))
                            
                            # Open file for writing in binary mode
                            with open(output_file, 'wb') as f:
                                if verbose:
                                    with click.progressbar(length=total_size, 
                                                          label=f'Downloading {file_name}') as bar:
                                        # Download in chunks to handle large files
                                        for chunk in response.iter_content(chunk_size=8192):
                                            if chunk:
                                                f.write(chunk)
                                                bar.update(len(chunk))
                                else:
                                    # Download without progress bar
                                    for chunk in response.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                        
                        click.echo(f"    Downloaded successfully to: {output_file}")
                    
                    except requests.exceptions.RequestException as e:
                        click.echo(f"    Error downloading {file_name}: {str(e)}", err=True)
                    except IOError as e:
                        click.echo(f"    Error writing file {output_file}: {str(e)}", err=True)
                
                click.echo(f"Output directory: {output_dir}")
                
                # No longer needed since we're actually downloading files
                # click.echo("Stub: This command will download ISA archives from the OSD files endpoint in the future.")
            except ValueError:
                click.echo(f"Error: Failed to parse API response for {final_accession}", err=True)
                sys.exit(1)
        else:
            # For GLDS accessions, use the existing approach
            if verbose:
                click.echo(f"Attempting to resolve accession: {final_accession}")
            
            # Get the OSD and GLDS mappings
            osd_accession, glds_accessions = get_osd_and_glds(final_accession, api_url, verbose=verbose)
            
            if not glds_accessions:
                if final_accession.startswith('OSD-'):
                    click.echo(f"Found OSD accession {osd_accession}, but it doesn't have any GLDS mappings.")
                    click.echo("This OSD entry doesn't have ISA archives available.")
                    sys.exit(0)
                else:
                    click.echo(f"Error: Found GLDS accession {final_accession} but couldn't determine its OSD mapping.")
                    sys.exit(1)
            
            click.echo(f"Found mapping: {osd_accession} → {', '.join(glds_accessions)}")
            
            # Process each GLDS accession by getting the files from the OSD accession
            if verbose:
                click.echo(f"Using OSD accession {osd_accession} to access files for {', '.join(glds_accessions)}")
            
            # Construct the OSD files API URL
            osd_num = osd_accession.replace("OSD-", "")
            osd_files_url = f"https://osdr.nasa.gov/osdr/data/osd/files/{osd_num}"
            
            if verbose:
                click.echo(f"Using OSD files URL: {osd_files_url}")
            
            # Make the API request
            response = requests.get(osd_files_url)
            if response.status_code != 200:
                click.echo(f"Error: Failed to fetch files for {osd_accession}. Status code: {response.status_code}", err=True)
                sys.exit(1)
                
            try:
                data = response.json()
                if not data.get('success'):
                    click.echo(f"Error: API returned unsuccessful response for {osd_accession}", err=True)
                    sys.exit(1)
                    
                # Extract file information
                if verbose:
                    click.echo("Successfully retrieved file information")
                
                # Check if we have study files for this OSD
                if osd_accession not in data.get('studies', {}):
                    click.echo(f"Error: No files found for {osd_accession}", err=True)
                    sys.exit(1)
                
                study_files = data['studies'][osd_accession].get('study_files', [])
                
                # Look for ISA archive files
                isa_files = [f for f in study_files if 'ISA' in f.get('file_name', '') and f.get('file_name', '').endswith('.zip')]
                
                if not isa_files:
                    click.echo(f"No ISA archive files found for {final_accession}")
                    # Show all available files
                    click.echo("Available files:")
                    for file in study_files:
                        click.echo(f"  - {file.get('file_name', 'Unknown')}")
                    sys.exit(0)
                
                # Process each ISA file
                click.echo(f"Found {len(isa_files)} ISA archive file(s) for {final_accession}:")
                for isa_file in isa_files:
                    file_name = isa_file.get('file_name')
                    remote_url = isa_file.get('remote_url')
                    click.echo(f"  - {file_name}")
                    
                    # Create the full download URL
                    base_url = "https://osdr.nasa.gov"
                    full_url = f"{base_url}{remote_url}"
                    
                    # Determine the output file path
                    output_file = os.path.join(output_dir, file_name)
                    
                    # Check if file already exists
                    if os.path.exists(output_file) and not force:
                        click.echo(f"    File already exists: {output_file}")
                        click.echo(f"    Use --force to overwrite")
                        continue
                    
                    # Download the file
                    try:
                        if verbose:
                            click.echo(f"    Downloading from: {full_url}")
                            click.echo(f"    Saving to: {output_file}")
                        
                        # Ensure output directory exists
                        os.makedirs(output_dir, exist_ok=True)
                        
                        # Stream the download to handle large files efficiently
                        with requests.get(full_url, stream=True) as response:
                            response.raise_for_status()  # Raise an exception for HTTP errors
                            
                            # Get total file size for progress reporting
                            total_size = int(response.headers.get('content-length', 0))
                            
                            # Open file for writing in binary mode
                            with open(output_file, 'wb') as f:
                                if verbose:
                                    with click.progressbar(length=total_size, 
                                                          label=f'Downloading {file_name}') as bar:
                                        # Download in chunks to handle large files
                                        for chunk in response.iter_content(chunk_size=8192):
                                            if chunk:
                                                f.write(chunk)
                                                bar.update(len(chunk))
                                else:
                                    # Download without progress bar
                                    for chunk in response.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                        
                        click.echo(f"    Downloaded successfully to: {output_file}")
                    
                    except requests.exceptions.RequestException as e:
                        click.echo(f"    Error downloading {file_name}: {str(e)}", err=True)
                    except IOError as e:
                        click.echo(f"    Error writing file {output_file}: {str(e)}", err=True)
                
                click.echo(f"Output directory: {output_dir}")
                
            except ValueError:
                click.echo(f"Error: Failed to parse API response for {osd_accession}", err=True)
                sys.exit(1)
        
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@dp_tools.command()
@click.option("--plugin", required=True,
              type=click.Path(exists=True), 
              help="Path to plugin directory or name of built-in plugin")
@click.option("--runsheet", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Path to runsheet CSV file")
@click.option("--outdir", required=True, 
              type=click.Path(), 
              help="Output directory for validation results")
@click.option("--components", multiple=True,
              help="Components to validate (e.g., raw_reads, star_alignment, bowtie2_alignment). Can be specified multiple times.")
@click.option("--assay-suffix", default="_GLbulkRNAseq",
              help="Suffix for assay files (default: _GLbulkRNAseq)")
@click.option("--log-file", type=click.Path(),
              help="Path to log file (default: <outdir>/vv.log)")
@click.option("--csv-file", type=click.Path(),
              help="Path to CSV results file (default: <outdir>/VV_log.csv)")
def validate(plugin, runsheet, outdir, components, assay_suffix, log_file, csv_file):
    """Validate pipeline outputs according to expectations."""
    # Initialize standard output logging
    log_file = log_file or Path(outdir) / "vv.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Initialize validation logger
    csv_file = csv_file or Path(outdir) / "VV_log.csv"
    logger = ValidationLogger(log_file=csv_file)
    
    # Handle different plugin types
    plugin_path = Path(plugin)
    if not plugin_path.exists():
        # Check if it's a built-in plugin name
        builtin_plugin_path = Path(__file__).parent.parent.parent / "plugins" / plugin
        if builtin_plugin_path.exists():
            plugin_path = builtin_plugin_path
        else:
            raise click.UsageError(f"Plugin '{plugin}' not found")
    
    # Determine which components to validate
    if not components:
        # If no components specified, validate all available components
        config_path = plugin_path / "config.yaml"
        if config_path.exists():
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                if 'components' in config:
                    components = config['components']
                else:
                    # Default to raw_reads if no components defined
                    components = ['raw_reads']
        else:
            # Default to raw_reads if no config
            components = ['raw_reads']
    
    click.echo(f"Starting validation with plugin: {plugin_path.name}")
    click.echo(f"Components to validate: {', '.join(components)}")
    click.echo("Data type (paired-end vs single-end) will be determined from the runsheet")
        
    # Process each component
    for component in components:
        # Import the validation function for the component
        component_module = plugin_path / f"{component}.py"
        if component_module.exists():
            # Dynamically import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                f"{plugin_path.name}.{component}", component_module)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Check if validate_{component} function exists
            validate_func_name = f"validate_{component}"
            if hasattr(module, validate_func_name):
                validate_func = getattr(module, validate_func_name)
                
                # Call the validation function with appropriate arguments
                click.echo(f"Validating {component} component...")
                results = validate_func(
                    outdir=Path(outdir),
                    runsheet=Path(runsheet),
                    assay_suffix=assay_suffix,
                    logger=logger
                )
                
                # Check for any HALT status
                if logger.get_status() == Status.HALT:
                    click.echo(f"Validation halted due to critical errors")
                    sys.exit(1)
            else:
                click.echo(f"Warning: No validation function found for component {component}")
        else:
            click.echo(f"Warning: Component module {component}.py not found in plugin directory")
    
    # Output final validation status
    click.echo(f"Validation completed with status: {logger.get_status().value}")
    click.echo(f"Detailed results written to {csv_file}")


if __name__ == "__main__":
    dp_tools() 