"""
Python functions the retrieve data from GeneLab. Uses the GeneLab public APIs (https://genelab.nasa.gov/genelabAPIs)
"""

import functools
from urllib.request import urlopen
import requests
import json

from loguru import logger as log
import yaml
import pandas as pd

GENELAB_DATASET_FILES = "https://osdr.nasa.gov/osdr/data/osd/files/{accession_number}"
""" Template URL to access json of files for a single GLDS accession ID """

FILE_RETRIEVAL_URL_PREFIX = "https://osdr.nasa.gov{suffix}"
""" Used to retrieve files using remote url suffixes listed in the 'Data Query' API """

@functools.cache
def get_table_of_files(accession: str) -> pd.DataFrame:
    """Retrieve table of filenames associated with a GLDS or OSD accession ID.
    
    This function handles both GLDS and OSD accession types:
    - For OSD accessions, it directly queries the files API
    - For GLDS accessions, it finds the corresponding OSD accession via the search API
    
    Note: This function is cached to prevent extra api calls. This can desync from the repository 
    in the rare case that the accession is updated in between related calls.

    :param accession: Accession ID, e.g. 'GLDS-194' or 'OSD-194'
    :type accession: str
    :return: A dataframe containing each filename including associated metadata like datatype
    :rtype: pd.DataFrame
    """
    # Check accession type
    log.info(f"Retrieving table of files for {accession}")
    
    # Direct access for OSD accessions
    if accession.startswith("OSD-"):
        accession_num = accession.split("-")[1]
        url = GENELAB_DATASET_FILES.format(accession_number=accession_num)
        
        # fetch data
        log.info(f"URL Source: {url}")
        print(url)
        with urlopen(url) as response:
            data = yaml.safe_load(response.read())
            try:
                df = pd.DataFrame(data['studies'][accession]['study_files'])
            except KeyError:
                raise ValueError(f"{accession} is not reachable on OSD website. This study likely does not exist")
        return df
    
    # For GLDS accessions, we MUST use the search API to find the OSD mapping
    elif accession.startswith("GLDS-"):
        log.info(f"Searching for OSD mapping for {accession}")
        search_url = "https://osdr.nasa.gov/osdr/data/search?ffield=Data+Source+Type&fvalue=cgene&size=5000"
        
        try:
            log.info(f"Querying search API: {search_url}")
            with urlopen(search_url) as search_response:
                search_data = json.loads(search_response.read())
                
                # Look for GLDS ID in Identifiers
                found_mapping = False
                for hit in search_data.get("hits", {}).get("hits", []):
                    source = hit.get("_source", {})
                    identifiers = source.get("Identifiers", "")
                    
                    # Check if our GLDS ID is in the identifiers
                    if accession in identifiers.split():
                        # Found the mapping
                        osd_accession = source.get("Accession")  # e.g., "OSD-489"
                        log.info(f"Found mapping: {accession} → {osd_accession}")
                        found_mapping = True
                        
                        # Now get the files for this OSD
                        osd_num = osd_accession.split("-")[1]
                        file_url = GENELAB_DATASET_FILES.format(accession_number=osd_num)
                        log.info(f"Fetching files from: {file_url}")
                        
                        with urlopen(file_url) as file_response:
                            file_data = yaml.safe_load(file_response.read())
                            try:
                                df = pd.DataFrame(file_data['studies'][osd_accession]['study_files'])
                                return df
                            except KeyError:
                                raise ValueError(f"{osd_accession} is not reachable on OSD website after mapping from {accession}")
                
                # If we get here, no mapping was found
                if not found_mapping:
                    raise ValueError(f"Could not find OSD mapping for {accession} in search results")
                    
        except Exception as e:
            raise ValueError(f"Error retrieving files for {accession}: {str(e)}")
    else:
        raise ValueError(f"Invalid accession format: {accession}. Must start with 'OSD-' or 'GLDS'.")

def find_matching_filenames(accession: str, filename_pattern: str) -> list[str]:
    """Returns list of file names that match the provided regex pattern.

    :param accession: GLDS accession ID, e.g. 'GLDS-194'
    :type accession: str
    :param filename_pattern: Regex pattern to query against file names
    :type filename_pattern: str
    :return: List of file names that match the regex
    :rtype: list[str]
    """
    df = get_table_of_files(accession)
    return df.loc[df['file_name'].str.contains(filename_pattern), 'file_name'].to_list()

def retrieve_file_url(accession: str, filename: str) -> str:
    """Retrieve file URL associated with a GLDS accesion ID

    :param accession: GLDS accession ID, e.g. 'GLDS-194'
    :type accession: str
    :param filename: Full filename, e.g. 'GLDS-194_metadata_GLDS-194-ISA.zip'
    :type filename: str
    :return: URL to fetch the most recent version of the file
    :rtype: str
    """
    # Check that the filenames exists
    df = get_table_of_files(accession)
    if filename not in list(df["file_name"]):
        raise ValueError(
            f"Could not find filename: '{filename}'. Here as are found filenames for '{accession}': '{df['file_name'].unique()}'"
        )
    url = FILE_RETRIEVAL_URL_PREFIX.format(suffix=df.loc[df['file_name'] == filename, 'remote_url'].squeeze())
    return url
