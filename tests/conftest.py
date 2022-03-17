import os
from pathlib import Path
import pytest

from dp_tools.bulkRNASeq.loaders import load_BulkRNASeq_STAGE_00, load_BulkRNASeq_STAGE_01


# set for testing
@pytest.fixture
def root_test_dir():
    """ This should be development machine specific, path should be set by env variable for privacy"""
    return Path(os.environ["TEST_ASSETS_DIR"])


@pytest.fixture
def glds194_sample_names():
    return [
        "Mmus_BAL-TAL_LRTN_BSL_Rep1_B7",
        "Mmus_BAL-TAL_RRTN_BSL_Rep2_B8",
        "Mmus_BAL-TAL_RRTN_BSL_Rep3_B9",
        "Mmus_BAL-TAL_RRTN_BSL_Rep4_B10",
        "Mmus_BAL-TAL_LRTN_GC_Rep1_G6",
        "Mmus_BAL-TAL_LRTN_GC_Rep2_G8",
        "Mmus_BAL-TAL_LRTN_GC_Rep3_G9",
        "Mmus_BAL-TAL_RRTN_GC_Rep4_G10",
        "Mmus_BAL-TAL_LRTN_FLT_Rep1_F6",
        "Mmus_BAL-TAL_LRTN_FLT_Rep2_F7",
        "Mmus_BAL-TAL_LRTN_FLT_Rep3_F8",
        "Mmus_BAL-TAL_LRTN_FLT_Rep4_F9",
        "Mmus_BAL-TAL_LRTN_FLT_Rep5_F10",
    ]


@pytest.fixture
def glds48_sample_names():
    return [
        "Mmus_C57-6J_LVR_GC_I_Rep1_M31",
        "Mmus_C57-6J_LVR_GC_I_Rep2_M32",
        "Mmus_C57-6J_LVR_FLT_I_Rep1_M21",
        "Mmus_C57-6J_LVR_FLT_I_Rep2_M22",
        "Mmus_C57-6J_LVR_GC_C_Rep1_M36",
        "Mmus_C57-6J_LVR_GC_C_Rep2_M37",
        "Mmus_C57-6J_LVR_GC_C_Rep3_M38",
        "Mmus_C57-6J_LVR_GC_C_Rep4_M39",
        "Mmus_C57-6J_LVR_GC_C_Rep5_M40",
        "Mmus_C57-6J_LVR_FLT_C_Rep1_M25",
        "Mmus_C57-6J_LVR_FLT_C_Rep2_M26",
        "Mmus_C57-6J_LVR_FLT_C_Rep3_M27",
        "Mmus_C57-6J_LVR_FLT_C_Rep4_M28",
        "Mmus_C57-6J_LVR_FLT_C_Rep5_M30",
    ]


@pytest.fixture
def glds194_test_dir(root_test_dir):
    return root_test_dir / "GLDS-194_TruncatedProcessed"


@pytest.fixture
def glds194_runsheetPath(glds194_test_dir):
    return (
        glds194_test_dir
        / "Metadata"
        / "AST_autogen_template_RNASeq_RCP_GLDS-194_RNASeq_runsheet.csv"
    )


@pytest.fixture
def glds48_test_dir(root_test_dir):
    return root_test_dir / "GLDS-48_TruncatedProcessed"


@pytest.fixture
def typo_test_dir(root_test_dir):
    return root_test_dir / "GLDS-48_BUTWITHTYPOS"

@pytest.fixture
def glds194_dataSystem_STAGE00(glds194_test_dir):
    return load_BulkRNASeq_STAGE_00(glds194_test_dir, dataSystem_name="GLDS-194")

@pytest.fixture
def glds194_dataSystem_STAGE01(glds194_test_dir):
    return load_BulkRNASeq_STAGE_01(*load_BulkRNASeq_STAGE_00(glds194_test_dir, dataSystem_name="GLDS-194", stack=True))