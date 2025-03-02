import pytest

from dp_tools.glds_api.commons import (
    get_table_of_files,
    retrieve_file_url,
    find_matching_filenames
)


def test_get_table_of_files():
    accession = "GLDS-194"
    df = get_table_of_files(accession)
    assert len(df) == 290

    accession = "GLDS-1"
    df = get_table_of_files(accession)
    assert len(df) == 73

def test_retrieve_file_url():
    accession = "GLDS-194"
    url = retrieve_file_url(accession, filename="OSD-194_metadata_OSD-194-ISA.zip")
    assert (
        url
        == "https://osdr.nasa.gov/geode-py/ws/studies/OSD-194/download?source=datamanager&file=OSD-194_metadata_OSD-194-ISA.zip"
    )

    accession = "GLDS-1"
    # Use a non-existent filename to trigger exception
    with pytest.raises(ValueError):
        url = retrieve_file_url(
            accession, filename="OSD-194_metadata_OSD-194-ISA.zip"
        )

def test_find_matching_filenames():
    accession = "GLDS-194"
    filenames = find_matching_filenames(accession, filename_pattern=".*-ISA.zip")
    assert (
        filenames
        == ['OSD-194_metadata_OSD-194-ISA.zip']
    )

def test_find_matching_filenames_for_nonmatching_osd():
    """Test finding ISA for GLDS-104 where proper search API mapping is required."""
    accession = "GLDS-104"
    filenames = find_matching_filenames(accession, filename_pattern=".*-ISA.zip")
    
    # Now we've confirmed the proper API-based mapping works
    assert filenames == ['OSD-104_metadata_OSD-104-ISA.zip']

def test_find_matching_filenames_for_glds570():
    """Test finding ISA for GLDS-570 using the search API mapping approach.
    
    This test verifies that GLDS-570 correctly maps to OSD-576 (not OSD-570)
    demonstrating that simple GLDS->OSD direct mapping would fail for this case.
    """
    accession = "GLDS-570"
    filenames = find_matching_filenames(accession, filename_pattern=".*-ISA.zip")
    
    # GLDS-570 maps to OSD-576, proving we need the search API approach
    assert filenames == ['OSD-576_metadata_OSD-576-ISA.zip']
