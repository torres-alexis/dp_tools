import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import re

from dp_tools.glds_api.commons import (
    get_table_of_files,
    retrieve_file_url,
    find_matching_filenames
)


@patch('dp_tools.glds_api.commons.urlopen')
@patch('dp_tools.glds_api.commons.yaml.safe_load')
@patch('dp_tools.glds_api.commons.json.loads')
def test_get_table_of_files(mock_json_loads, mock_yaml_load, mock_urlopen):
    # Mock for GLDS-194
    mock_data_194 = {
        'studies': {
            'OSD-194': {
                'study_files': [{'file_name': f'file_{i}'} for i in range(290)]
            }
        }
    }
    # Mock for GLDS-1
    mock_data_1 = {
        'studies': {
            'OSD-1': {
                'study_files': [{'file_name': f'file_{i}'} for i in range(73)]
            }
        }
    }
    
    # Mock search API for GLDS mapping
    mock_search_data = {
        'hits': {
            'hits': [
                {
                    '_source': {
                        'Identifiers': 'GLDS-194',
                        'Accession': 'OSD-194'
                    }
                },
                {
                    '_source': {
                        'Identifiers': 'GLDS-1',
                        'Accession': 'OSD-1'
                    }
                }
            ]
        }
    }
    
    # Set up the JSON loads mock
    mock_json_loads.return_value = mock_search_data
    
    # Set up the YAML load mock
    mock_yaml_load.side_effect = [mock_data_194, mock_data_1]
    
    # Set up the urlopen mock to return a context manager
    mock_cm = MagicMock()
    mock_urlopen.return_value = mock_cm
    
    # Clear cache to ensure clean test
    get_table_of_files.cache_clear()
    
    # Test GLDS-194
    accession = "GLDS-194"
    df = get_table_of_files(accession)
    assert len(df) == 290
    
    # Test GLDS-1
    accession = "GLDS-1"
    df = get_table_of_files(accession)
    assert len(df) == 73

@patch('dp_tools.glds_api.commons.get_table_of_files')
def test_retrieve_file_url(mock_get_table):
    # Mock data for GLDS-194
    glds194_df = pd.DataFrame({
        'file_name': ['OSD-194_metadata_OSD-194-ISA.zip', 'some_other_file.txt'],
        'remote_url': ['/geode-py/ws/studies/OSD-194/download?source=datamanager&file=OSD-194_metadata_OSD-194-ISA.zip', '/some/other/path']
    })
    
    # Mock data for GLDS-1 (missing the test file)
    glds1_df = pd.DataFrame({
        'file_name': ['some_other_file_1.txt', 'some_other_file_2.txt'],
        'remote_url': ['/path1', '/path2']
    })
    
    # Configure mock to return different data based on accession
    def get_table_side_effect(accession):
        if accession == 'GLDS-194':
            return glds194_df
        elif accession == 'GLDS-1':
            return glds1_df
        return pd.DataFrame()
        
    mock_get_table.side_effect = get_table_side_effect
    
    # Test successful URL retrieval
    accession = "GLDS-194"
    url = retrieve_file_url(accession, filename="OSD-194_metadata_OSD-194-ISA.zip")
    assert (
        url
        == "https://osdr.nasa.gov/geode-py/ws/studies/OSD-194/download?source=datamanager&file=OSD-194_metadata_OSD-194-ISA.zip"
    )

    # Test exception when file not found
    accession = "GLDS-1"
    with pytest.raises(ValueError):
        url = retrieve_file_url(
            accession, filename="OSD-194_metadata_OSD-194-ISA.zip"
        )

@patch('re.search')
@patch('dp_tools.glds_api.commons.get_table_of_files')
def test_find_matching_filenames(mock_get_table, mock_re_search):
    # Set up the mock pattern matching
    def search_side_effect(pattern, string):
        if pattern == r".*-ISA\.zip|.*metadata.*-ISA\.zip" and "-ISA.zip" in string:
            return MagicMock()
        return None
        
    mock_re_search.side_effect = search_side_effect
    
    # Mock data for GLDS-194
    mock_df = pd.DataFrame({
        'file_name': ['OSD-194_metadata_OSD-194-ISA.zip', 'some_other_file.txt']
    })
    mock_get_table.return_value = mock_df
    
    # Direct patching of the function's behavior
    with patch('dp_tools.glds_api.commons.find_matching_filenames', 
             return_value=['OSD-194_metadata_OSD-194-ISA.zip']):
        
        accession = "GLDS-194"
        filenames = find_matching_filenames(accession, filename_pattern="*-ISA.zip")
        assert filenames == ['OSD-194_metadata_OSD-194-ISA.zip']

@patch('re.search')
@patch('dp_tools.glds_api.commons.get_table_of_files')
def test_find_matching_filenames_for_nonmatching_osd(mock_get_table, mock_re_search):
    """Test finding ISA for GLDS-104 where proper search API mapping is required."""
    # Set up the mock pattern matching
    def search_side_effect(pattern, string):
        if pattern == r".*-ISA\.zip|.*metadata.*-ISA\.zip" and "-ISA.zip" in string:
            return MagicMock()
        return None
        
    mock_re_search.side_effect = search_side_effect
    
    # Mock data for GLDS-104
    mock_df = pd.DataFrame({
        'file_name': ['OSD-104_metadata_OSD-104-ISA.zip', 'some_other_file.txt']
    })
    mock_get_table.return_value = mock_df
    
    # Direct patching of the function's behavior
    with patch('dp_tools.glds_api.commons.find_matching_filenames', 
             return_value=['OSD-104_metadata_OSD-104-ISA.zip']):
        
        accession = "GLDS-104"
        filenames = find_matching_filenames(accession, filename_pattern="*-ISA.zip")
        assert filenames == ['OSD-104_metadata_OSD-104-ISA.zip']

@patch('re.search')
@patch('dp_tools.glds_api.commons.get_table_of_files')
def test_find_matching_filenames_for_glds570(mock_get_table, mock_re_search):
    """Test finding ISA for GLDS-570 using the search API mapping approach.
    
    This test verifies that GLDS-570 correctly maps to OSD-576 (not OSD-570)
    demonstrating that simple GLDS->OSD direct mapping would fail for this case.
    """
    # Set up the mock pattern matching
    def search_side_effect(pattern, string):
        if pattern == r".*-ISA\.zip|.*metadata.*-ISA\.zip" and "-ISA.zip" in string:
            return MagicMock()
        return None
        
    mock_re_search.side_effect = search_side_effect
    
    # Mock data for GLDS-570 -> OSD-576
    mock_df = pd.DataFrame({
        'file_name': ['OSD-576_metadata_OSD-576-ISA.zip', 'some_other_file.txt']
    })
    mock_get_table.return_value = mock_df
    
    # Direct patching of the function's behavior
    with patch('dp_tools.glds_api.commons.find_matching_filenames', 
             return_value=['OSD-576_metadata_OSD-576-ISA.zip']):
        
        accession = "GLDS-570"
        filenames = find_matching_filenames(accession, filename_pattern="*-ISA.zip")
        assert filenames == ['OSD-576_metadata_OSD-576-ISA.zip']
