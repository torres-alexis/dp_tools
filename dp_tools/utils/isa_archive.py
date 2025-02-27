"""
Functions that deal directly with GLDS ISA Archives.

This module provides utilities to extract and parse ISA archive files,
particularly for retrieving investigation files and parsing their contents.
"""

from pathlib import Path
import tempfile
import zipfile
from loguru import logger as log

import pandas as pd

ISA_INVESTIGATION_HEADERS = {
    "ONTOLOGY SOURCE REFERENCE",
    "INVESTIGATION",
    "INVESTIGATION PUBLICATIONS",
    "INVESTIGATION CONTACTS",
    "STUDY",
    "STUDY DESIGN DESCRIPTORS",
    "STUDY PUBLICATIONS",
    "STUDY FACTORS",
    "STUDY ASSAYS",
    "STUDY PROTOCOLS",
    "STUDY CONTACTS",
}


def fetch_isa_files(ISAarchive: Path) -> set[Path]:
    """
    Extract all files from an ISA archive to a temporary directory.
    
    Args:
        ISAarchive: Path to the ISA archive zip file
        
    Returns:
        Set of Path objects pointing to the extracted files
    """
    temp_dir = tempfile.mkdtemp()
    log.debug(f"Extracting ISA Archive to temp directory: {temp_dir}")
    with zipfile.ZipFile(ISAarchive, "r") as zip_ref:
        zip_ref.extractall(temp_dir)

    return {f for f in Path(temp_dir).rglob("*") if f.is_file()}


def isa_investigation_subtables(isaArchive: Path) -> dict[str, pd.DataFrame]:
    """
    Parse an ISA investigation file into subtables.
    
    This function extracts and processes the investigation file from an ISA archive,
    splitting it into separate dataframes for each section defined in ISA_INVESTIGATION_HEADERS.
    
    Args:
        isaArchive: Path to the ISA archive zip file
        
    Returns:
        Dictionary mapping header names to pandas DataFrames containing the corresponding subtables
        
    Raises:
        AssertionError: If not all expected subtables are present in the investigation file
    """
    tables: dict[str, pd.DataFrame] = dict()

    # track sub table lines
    table_lines: list[list] = list()
    key: str = None  # type: ignore

    [i_file] = (
        f for f in fetch_isa_files(isaArchive) if f.name.startswith("i_")
    )
    # Default to 'utf-8'
    try:
        log.trace("Decoding ISA with 'utf-8")
        with open(i_file, "r", encoding = "utf-8") as f:
            lines = f.readlines()
    # Fallback to "ISO-8859-1" if 'utf-8' fails
    except UnicodeDecodeError:
        log.warning("Failed using 'utf-8'. Decoding ISA with 'ISO-8859-1'")
        with open(i_file, "r", encoding = "ISO-8859-1") as f:
            lines = f.readlines()
    for line in lines:
        line = line.rstrip()
        # search for header
        if line in ISA_INVESTIGATION_HEADERS:
            if key != None:
                tables[key] = pd.DataFrame(
                    table_lines
                ).T  # each subtable is transposed in the i_file
                table_lines = list()
            key = line  # set next table key
        else:
            tokens = line.split("\t")  # tab separated
            table_lines.append(tokens)
    tables[key] = pd.DataFrame(
        table_lines
    ).T  # each subtable is transposed in the i_file

    # reformat each table
    def clean_quotes(string: str) -> str:
        """Remove single or double quotes from the beginning and end of a string."""
        SINGLE_OR_DOUBLE_QUOTES = "\"'"
        # don't perform on non-string elements
        if not isinstance(string, str):
            return string
        else:
            return string.lstrip(SINGLE_OR_DOUBLE_QUOTES).rstrip(
                SINGLE_OR_DOUBLE_QUOTES
            )

    df: pd.DataFrame
    for key, df in tables.items():
        # note: as a ref, no reassign needed
        tables[key] = (
            df.rename(columns=df.iloc[0]).drop(df.index[0]).applymap(clean_quotes)
        )

    # ensure all expected subtables present
    assert set(tables.keys()) == ISA_INVESTIGATION_HEADERS

    return tables 