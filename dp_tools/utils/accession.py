"""
Utilities for handling accession mapping between OSD and GLDS.
"""
import re
import sys
import json
import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default OSDR API URL - from the official NASA OSDR documentation
DEFAULT_API_URL = "https://osdr.nasa.gov/osdr/data/search?size=2000"

def get_osd_and_glds(accession, api_url=DEFAULT_API_URL, verbose=False):
    """
    Get the corresponding OSD and GLDS accessions from a given accession.
    
    Args:
        accession (str): Accession in the format 'OSD-###' or 'GLDS-###'
        api_url (str): OSDR API URL
        verbose (bool): Print verbose debug information
        
    Returns:
        tuple: (osd_accession, glds_accessions) where glds_accessions is a list
    """
    # Fetch data from the API
    try:
        if verbose:
            logger.info(f"Fetching data from {api_url}")
        
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        if verbose:
            logger.info(f"API response has {len(data.get('hits', {}).get('hits', []))} records")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise RuntimeError(f"Error fetching data from API: {e}")
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON response")
        raise RuntimeError("Error decoding JSON response from API")

    osd_accession = None
    glds_accessions = []

    # Debug: Print structure of the first record to understand the data format
    if verbose and data.get('hits', {}).get('hits', []):
        first_hit = data.get('hits', {}).get('hits', [])[0]
        logger.info(f"Sample record structure: {json.dumps(first_hit.get('_source', {}), indent=2)[:500]}...")

    # Check if the accession is OSD or GLDS
    if accession.startswith('OSD-'):
        osd_accession = accession
        # Find the GLDS identifiers associated with this OSD accession
        found = False
        for hit in data.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            if source.get('Accession') == osd_accession:
                found = True
                identifiers = source.get('Identifiers', '')
                if verbose:
                    logger.info(f"Found OSD: {osd_accession} with identifiers: {identifiers}")
                glds_accessions = re.findall(r'GLDS-\d+', identifiers)
                break
                
        if verbose and not found:
            logger.warning(f"OSD accession {osd_accession} not found in API response")
            
    elif accession.startswith('GLDS-'):
        glds_accessions = [accession]
        # Find the OSD accession associated with this GLDS accession
        found = False
        for hit in data.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            identifiers = source.get('Identifiers', '')
            if accession in identifiers:
                found = True
                osd_accession = source.get('Accession')
                if verbose:
                    logger.info(f"Found GLDS: {accession} with OSD: {osd_accession}")
                break
                
        if verbose and not found:
            logger.warning(f"GLDS accession {accession} not found in any identifiers field")
            
    else:
        raise ValueError("Invalid accession format. Please use 'OSD-###' or 'GLDS-###'.")

    if not osd_accession or (accession.startswith('GLDS-') and not osd_accession):
        # Try the metadata API as a fallback
        if verbose:
            logger.info(f"Trying metadata API fallback for {accession}")
            
        if accession.startswith('OSD-'):
            # Extract number
            osd_num = accession.split('-')[1]
            meta_url = f"https://osdr.nasa.gov/osdr/data/osd/meta/{osd_num}"
            
            try:
                if verbose:
                    logger.info(f"Fetching metadata from {meta_url}")
                    
                meta_response = requests.get(meta_url)
                meta_response.raise_for_status()
                meta_data = meta_response.json()
                
                if verbose:
                    logger.info(f"Metadata response status: {meta_data.get('success', False)}")
                
                if meta_data.get('success', False):
                    osd_accession = accession
                    # Look for GLDS identifiers in the metadata
                    study_data = meta_data.get('study', {}).get(accession, {})
                    comments = []
                    
                    # Navigate the nested structure to find identifiers
                    for study in study_data.get('studies', []):
                        comments.extend(study.get('comments', []))
                        
                    for comment in comments:
                        if comment.get('name') == 'Identifiers':
                            identifiers = comment.get('value', '')
                            if verbose:
                                logger.info(f"Found identifiers in metadata: {identifiers}")
                            glds_accessions = re.findall(r'GLDS-\d+', identifiers)
                            break
            
            except Exception as e:
                if verbose:
                    logger.warning(f"Metadata API fallback failed: {str(e)}")
        
        # If we found an OSD but no GLDS mappings, return it anyway
        if accession.startswith('OSD-') and osd_accession:
            if verbose and not glds_accessions:
                logger.info(f"OSD accession {osd_accession} exists but has no GLDS mappings")
            return osd_accession, glds_accessions
        
        # For GLDS entries or if we didn't find the OSD, raise an error
        if not osd_accession or (accession.startswith('GLDS-') and not osd_accession):
            raise ValueError(f"No data found for {accession}")

    return osd_accession, glds_accessions 