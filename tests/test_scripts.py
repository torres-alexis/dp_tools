""" Test CLI scripts """


import os


def test_dp_tools_isa_get(script_runner, tmpdir):
    os.chdir(tmpdir)

    ret = script_runner.run(["dp_tools", "isa", "get", "GLDS-194"])
    assert ret.success


def test_dpt_isa_get(script_runner, tmpdir):
    os.chdir(tmpdir)

    ret = script_runner.run(["dpt", "isa", "get", "GLDS-194"])
    assert ret.success


# Tests requiring TEST_ASSETS_DIR are commented out
"""
def test_dp_tools_isa_to_runsheet(script_runner, tmpdir, glds194_test_dir):
    os.chdir(tmpdir)
    isaPath = glds194_test_dir / "Metadata" / "GLDS-194_metadata_GLDS-194-ISA.zip"

    ret = script_runner.run([
        "dp_tools", "isa", "to-runsheet",
        "GLDS-194",
        "--config-type", "bulkRNASeq",
        "--config-version", "Latest",
        "--isa-archive", str(isaPath),
    ])
    assert ret.success


def test_dpt_isa_to_runsheet(script_runner, tmpdir, glds194_test_dir):
    os.chdir(tmpdir)
    isaPath = glds194_test_dir / "Metadata" / "GLDS-194_metadata_GLDS-194-ISA.zip"

    ret = script_runner.run([
        "dpt", "isa", "to-runsheet",
        "GLDS-194",
        "--config-type", "bulkRNASeq",
        "--config-version", "Latest",
        "--isa-archive", str(isaPath),
    ])
    assert ret.success
"""
