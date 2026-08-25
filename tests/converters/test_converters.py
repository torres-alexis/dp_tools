import hashlib
import os

import pandas as pd
import pandera.pandas as pa
import pytest
from pathlib import Path

from dp_tools.scripts.convert import isa_to_runsheet


@pytest.fixture(autouse=True)
def _mock_osdr_file_urls(monkeypatch):
    """Avoid live OSDR filelisting (renames / 500s break hash fixtures)."""
    monkeypatch.setattr(
        "dp_tools.scripts.convert.retrieve_file_url",
        lambda accession, filename: f"https://example.invalid/{accession}/{filename}",
    )


# Updated to microarray development version api
def test_paired_isa_to_runsheet(glds194_test_dir, tmpdir):
    """This tests validation as it would be run on dataset after demultiplexing"""
    os.chdir(tmpdir)
    isaPath = glds194_test_dir / "Metadata" / "GLDS-194_metadata_GLDS-194-ISA.zip"
    df_runsheet = isa_to_runsheet("GLDS-194", isaPath, config=("bulkRNASeq", "0"))

    assert df_runsheet.shape == (13, 8)
    assert (
        hashlib.sha1(pd.util.hash_pandas_object(df_runsheet).values).hexdigest()
        == "6b9186d43041aabf94e4a9d448a47b87a2b4f416"
    ), "Hash did not match, the means the contents changed. Manually validation and reset of test hash is in order"


def test_single_isa_to_runsheet(glds48_test_dir, tmpdir):
    """This tests validation as it would be run on dataset after demultiplexing"""
    os.chdir(tmpdir)
    isaPath = glds48_test_dir / "Metadata" / "GLDS-48_metadata_RR1-NASA-ISA.zip"
    df_runsheet = isa_to_runsheet("GLDS-48", isaPath, config=("bulkRNASeq", "0"))

    assert df_runsheet.shape == (14, 7)
    assert (
        hashlib.sha1(pd.util.hash_pandas_object(df_runsheet).values).hexdigest()
        == "5d87fd96304f05a9047cb5e0e25e3cb76e5b24f0"
    ), "Hash did not match, the means the contents changed. Manually validation and reset of test hash is in order"

def test_non_ready_dataset_to_runsheet(glds313_test_dir, tmpdir):
    """This tests that an ISA archive with pending pre processing for FastQ concatenation correctly raises an exception """
    os.chdir(tmpdir)
    isaPath = glds313_test_dir / "Metadata" / "OSD-313_metadata_GLDS-313-ISA.zip"

    with pytest.raises(pa.errors.SchemaError):
        isa_to_runsheet("GLDS-313", isaPath, config=("bulkRNASeq", "1"))
        



def test_methylSeq_glds397_isa_to_runsheet(glds397_isazip_path):
    """This tests isa_to_runsheet on a methylSeq assay, GLDS-397"""
    df_runsheet = isa_to_runsheet(
        "GLDS-397", glds397_isazip_path, config = ("methylSeq", "1")
    )

    assert df_runsheet.shape == (16, 6)  # 1 factor value
    assert (
        hashlib.sha1(pd.util.hash_pandas_object(df_runsheet).values).hexdigest()
        == "5a50791f1b2702b2db4949040268a68156cad028"
    ), "Hash did not match, the means the contents changed. Manually validation and reset of test hash is in order"

# def test_microarray_glds123_isa_to_runsheet(glds123_isazip_path):
#     """This tests validation as it would be run on dataset after demultiplexing"""
#     df_runsheet = isa_to_runsheet(
#         "GLDS-123", glds123_isazip_path, config=("microarray", "0")
#     )

#     assert df_runsheet.shape == (16, 12)  # 1 factor values
#     assert (
#         hashlib.sha1(pd.util.hash_pandas_object(df_runsheet).values).hexdigest()
#         == "0dce69a3de3783856f7f6acc7f1eb9e634d4e2c8"
#     ), "Hash did not match, the means the contents changed. Manually validation and reset of test hash is in order"
