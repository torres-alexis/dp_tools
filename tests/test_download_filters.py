from unittest.mock import patch

import pandas as pd

from dp_tools.glds_api.commons import (
    apply_name_filters,
    expected_file_size,
    filenames_for_assay_types,
    filter_filenames,
    is_genelab_processed_category,
    verify_downloaded_sizes,
)


def test_apply_name_filters_ext_and_search():
    names = [
        "OSD-1_counts.csv",
        "OSD-1_raw.fastq.gz",
        "OSD-1_archive.tar.gz",
        "OSD-1_temp_counts.csv",
    ]
    assert apply_name_filters(names, extensions=["csv"]) == [
        "OSD-1_counts.csv",
        "OSD-1_temp_counts.csv",
    ]
    assert apply_name_filters(names, exclude_extensions=["tar.gz", "fastq.gz"]) == [
        "OSD-1_counts.csv",
        "OSD-1_temp_counts.csv",
    ]
    assert apply_name_filters(names, search=["counts"], exclude_search=["temp"]) == [
        "OSD-1_counts.csv"
    ]


def test_filenames_for_assay_types_substring_and_unique():
    records = pd.DataFrame(
        {
            "file.file_name": [
                "rna_raw.fastq.gz",
                "rna_counts.csv",
                "rna_counts.csv",
                "rrbs.fastq.gz",
            ],
            "investigation.study assays.study assay measurement type": [
                "transcription profiling",
                "transcription profiling",
                "transcription profiling",
                "DNA methylation profiling",
            ],
            "investigation.study assays.study assay technology type": [
                "RNA Sequencing (RNA-Seq)",
                "RNA Sequencing (RNA-Seq)",
                "RNA Sequencing (RNA-Seq)",
                "Reduced-Representation Bisulfite Sequencing",
            ],
        }
    )
    assert filenames_for_assay_types(
        "OSD-397",
        measurements=["transcription profiling"],
        technologies=["RNA-Seq"],
        assay_records=records,
    ) == ["rna_raw.fastq.gz", "rna_counts.csv"]
    assert filenames_for_assay_types(
        "OSD-397",
        measurements=["methylation"],
        assay_records=records,
    ) == ["rrbs.fastq.gz"]


@patch("dp_tools.glds_api.commons.filenames_for_assay_types")
@patch("dp_tools.glds_api.commons.get_table_of_files")
def test_filter_filenames_intersects_assay_and_excludes(mock_table, mock_assay):
    mock_table.return_value = pd.DataFrame(
        {
            "file_name": [
                "keep.csv",
                "drop.tar.gz",
                "other_assay.csv",
            ],
            "category": ["RNA-Seq", "RNA-Seq", "Methyl-Seq"],
            "subcategory": ["", "", ""],
        }
    )
    mock_assay.return_value = ["keep.csv", "drop.tar.gz"]
    names = filter_filenames(
        "OSD-397",
        measurements=["transcription profiling"],
        exclude_extensions=["tar.gz"],
    )
    assert names == ["keep.csv"]
    mock_assay.assert_called_once()


def test_genelab_only_category_shortcut():
    assert is_genelab_processed_category("GeneLab Processed RNA-Seq Files")
    assert not is_genelab_processed_category("RNA-Seq")


@patch("dp_tools.glds_api.commons.get_table_of_files")
def test_filter_filenames_genelab_only(mock_table):
    mock_table.return_value = pd.DataFrame(
        {
            "file_name": ["counts.csv", "raw.fastq.gz"],
            "category": [
                "GeneLab Processed RNA-Seq Files",
                "RNA-Seq",
            ],
            "subcategory": ["Raw counts data", "Raw sequence data"],
        }
    )
    assert filter_filenames("OSD-1", genelab_only=True) == ["counts.csv"]


def test_verify_downloaded_sizes(tmp_path):
    df = pd.DataFrame(
        {
            "file_name": ["ok.txt", "short.txt", "unknown.txt"],
            "file_size": [4, 10, None],
        }
    )
    (tmp_path / "ok.txt").write_bytes(b"abcd")
    (tmp_path / "short.txt").write_bytes(b"no")
    (tmp_path / "unknown.txt").write_bytes(b"whatever")
    mismatches = verify_downloaded_sizes(
        tmp_path,
        [("ok.txt", "ok.txt"), ("short.txt", "short.txt"), ("unknown.txt", "unknown.txt")],
        df,
    )
    assert mismatches == ["short.txt: expected 10 B, got 2 B"]
    assert expected_file_size(df, "ok.txt") == 4
