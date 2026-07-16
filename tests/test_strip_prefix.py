import pandas as pd

from dp_tools.glds_api import commons


def _glds_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "file_name": [
                "GLDS-240_SRR123_1.fastq.gz",
                "GLDS-240_SRR123_2.fastq.gz",
                "GLDS-240_metadata.zip",
            ],
            "category": ["RNA-Seq", "RNA-Seq", "Study Metadata Files"],
            "subcategory": [
                "Raw sequence data",
                "Raw sequence data",
                "",
            ],
        }
    )


def test_detect_strip_prefix_levels():
    df = _glds_df()
    names = df["file_name"].tolist()
    assert commons.detect_strip_prefix(names, df, 0) is None
    assert commons.detect_strip_prefix(names, df, 1) == "GLDS-240_"
    assert commons.detect_strip_prefix(
        ["GLDS-240_SRR123_1.fastq.gz", "GLDS-240_SRR123_2.fastq.gz"],
        df,
        2,
    ) == "GLDS-240_SRR123_"


def test_apply_strip_prefix_and_output_path():
    assert commons.apply_strip_prefix("GLDS-240_a.fq", "GLDS-240_") == "a.fq"
    assert commons.apply_strip_prefix("other.fq", "GLDS-240_") == "other.fq"
    path = "RNA-Seq/Raw sequence data/GLDS-240_a.fq"
    assert commons.strip_output_path(path, "GLDS-240_") == "RNA-Seq/Raw sequence data/a.fq"


def test_find_strip_collisions_flat():
    assert commons.find_strip_collisions(["GLDS-240_a.fq"], "GLDS-240_") == {}
    collisions = commons.find_strip_collisions(
        ["GLDS-240_a.fq", "a.fq"], "GLDS-240_"
    )
    assert collisions == {"a.fq": ["GLDS-240_a.fq", "a.fq"]}


def test_find_strip_collisions_preserve_dirs():
    df = pd.DataFrame(
        {
            "file_name": ["GLDS-48_foo_counts.csv", "counts.csv"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["Raw sequence data", "Processed counts"],
        }
    )
    names = df["file_name"].tolist()
    assert commons.find_strip_collisions(names, "GLDS-48_foo_") != {}
    assert (
        commons.find_strip_collisions(
            names, "GLDS-48_foo_", df=df, preserve_dirs=True
        )
        == {}
    )

    same_dir = pd.DataFrame(
        {
            "file_name": ["GLDS-48_foo_counts.csv", "counts.csv"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["STAR", "STAR"],
            "subdirectory": ["", ""],
        }
    )
    collisions = commons.find_strip_collisions(
        same_dir["file_name"].tolist(),
        "GLDS-48_foo_",
        df=same_dir,
        preserve_dirs=True,
    )
    assert collisions == {
        "RNA-Seq/STAR/counts.csv": ["GLDS-48_foo_counts.csv", "counts.csv"]
    }


def test_find_download_path_collisions_preserve_dirs_without_strip():
    df = pd.DataFrame(
        {
            "file_name": ["nested/foo.txt", "nested_foo.txt"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["STAR", "STAR"],
            "subdirectory": ["", ""],
        }
    )
    collisions = commons.find_download_path_collisions(
        df["file_name"].tolist(),
        df=df,
        preserve_dirs=True,
    )
    assert collisions == {
        "RNA-Seq/STAR/nested_foo.txt": ["nested/foo.txt", "nested_foo.txt"]
    }
