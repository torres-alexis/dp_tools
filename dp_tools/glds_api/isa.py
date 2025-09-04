import argparse
from pathlib import Path
from dp_tools.glds_api.commons import retrieve_file_url, find_matching_filenames, get_table_of_files

import requests
from loguru import logger as log
import sys
import os
import re
from typing import Union


def _parse_args():
    """Parse command line args."""
    parser = argparse.ArgumentParser(
        description=f"Script for downloading latest ISA from GLDS repository"
    )
    parser.add_argument(
        "--accession", metavar="GLDS-001", required=True, 
        help="GLDS or OSD accession number (e.g., GLDS-194 or OSD-194)"
    )

    args = parser.parse_args()
    return args


def download_isa(accession: str) -> Union[str, None]:
    """
    Download the ISA Archive for a given accession number.
    
    This function handles both GLDS and OSD accession numbers.
    
    Args:
        accession: The GLDS or OSD accession number.
        
    Returns:
        The filename of the downloaded file or None if no ISA archive is found.
    """
    log.info(f"Retrieving ISA Archive for {accession}")
    
    # Get all files for this accession
    all_files_df = get_table_of_files(accession)
    all_files = all_files_df['file_name'].tolist()
    log.info(f"All files available for {accession}: {all_files}")
    
    # We need to match both traditional ISA archives and metadata ISA archives
    # Both GLDS-XXX-ISA.zip and OSD-XXX_metadata_OSD-XXX-ISA.zip patterns
    pattern = r".*-ISA\.zip|.*metadata.*-ISA\.zip"
    
    matching_files = [f for f in all_files if re.search(pattern, f)]
    log.info(f"Files matching ISA pattern: {matching_files}")
    
    if not matching_files:
        log.error(f"No ISA Archive found for {accession}!")
        return None
    
    file_to_download = matching_files[0]
    log.info(f"Downloading ISA Archive: {file_to_download}")
    
    # Get the file URL from the DataFrame
    file_row = all_files_df[all_files_df['file_name'] == file_to_download].iloc[0]
    
    # Construct and execute the download URL
    download_url = f"https://osdr.nasa.gov{file_row['remote_url']}"
    log.info(f"Download URL: {download_url}")
    response = requests.get(download_url)
    
    if response.status_code == 200:
        with open(file_to_download, 'wb') as f:
            f.write(response.content)
        log.info(f"Downloaded ISA Archive to {file_to_download}")
        return file_to_download
    else:
        log.error(f"Failed to download ISA Archive: {response.status_code}")
        return None


def download_isa_archive(accession: str, output_dir: str = ".") -> str:
    """Downloads the ISA archive for the given accession.
    
    This is the main function exposed to the CLI.
    
    Args:
        accession: GLDS or OSD accession number, e.g., GLDS-194 or OSD-194
        output_dir: Directory to save the ISA archive to. Defaults to current directory.
        
    Returns:
        The path to the downloaded ISA archive.
    """
    # Configure logging
    log.remove()  # Remove default handler
    log.add(sys.stderr, level="INFO")  # Add stderr handler with INFO level
    
    # Change to output directory
    original_dir = Path.cwd()
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    os.chdir(output_path)
    
    try:
        result = download_isa(accession)
        return result
    finally:
        # Change back to original directory
        os.chdir(original_dir)


def main():
    """Main function for the legacy CLI."""
    # Configure logging
    log.remove()  # Remove default handler
    log.add(sys.stderr, level="INFO")  # Add stderr handler with INFO level
    
    args = _parse_args()
    download_isa(args.accession)


if __name__ == "__main__":
    isazip = main()
