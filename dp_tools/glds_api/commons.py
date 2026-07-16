"""
Python functions the retrieve data from GeneLab. Uses the GeneLab public APIs (https://genelab.nasa.gov/genelabAPIs)
"""

import functools
from pathlib import Path
from urllib.request import urlopen
import requests
import json

from loguru import logger as log
import yaml
import pandas as pd

GENELAB_DATASET_FILES = "https://osdr.nasa.gov/osdr/data/osd/files/{accession_number}"
"""Template URL to access json of files for a single GLDS accession ID."""

FILE_RETRIEVAL_URL_PREFIX = "https://osdr.nasa.gov{suffix}"
"""Used to retrieve files using remote url suffixes listed in the 'Data Query' API."""


def _load_osd_files_table(osd_id: str) -> pd.DataFrame:
    osd_num = osd_id.split("-", 1)[1]
    url = GENELAB_DATASET_FILES.format(accession_number=osd_num)
    log.info(f"URL Source: {url}")
    with urlopen(url) as response:
        data = yaml.safe_load(response.read())
    studies = data.get("studies") or {}
    if osd_id not in studies:
        raise ValueError(
            f"{osd_id} is not reachable on OSD website. This study likely does not exist"
        )
    return pd.DataFrame(studies[osd_id]["study_files"])


@functools.cache
def get_table_of_files(accession: str) -> pd.DataFrame:
    """Retrieve table of filenames associated with a GLDS or OSD accession ID."""
    log.info(f"Retrieving table of files for {accession}")

    if accession.startswith("OSD-"):
        return _load_osd_files_table(accession)

    # For GLDS accessions, we MUST use the search API to find the OSD mapping
    if accession.startswith("GLDS-"):
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
                        return _load_osd_files_table(osd_accession)
                
                # If we get here, no mapping was found
                if not found_mapping:
                    raise ValueError(f"Could not find OSD mapping for {accession} in search results")
                    
        except Exception as e:
            raise ValueError(f"Error retrieving files for {accession}: {str(e)}") from e
    raise ValueError(
        f"Invalid accession format: {accession}. Must start with 'OSD-' or 'GLDS-'."
    )


def find_matching_filenames(accession: str, filename_pattern: str) -> list[str]:
    """Returns list of file names that match the provided pattern.

    :param accession: GLDS or OSD accession ID, e.g. 'GLDS-194' or 'OSD-123'
    :type accession: str
    :param filename_pattern: Glob pattern to query against file names (e.g. '*.fastq.gz')
    :type filename_pattern: str
    :return: List of file names that match the pattern
    :rtype: list[str]
    """
    return filter_filenames(
        accession=accession,
        filename_pattern=filename_pattern,
    )


def filter_filenames(
    accession: str,
    filename_pattern: str | None = None,
    categories: list[str] | None = None,
    subcategories: list[str] | None = None,
) -> list[str]:
    """Return filenames filtered by OSDR category metadata and/or glob pattern."""
    import fnmatch

    df = get_table_of_files(accession)

    if categories:
        df = df[df["category"].isin(categories)]

    if subcategories:
        df = df[df["subcategory"].isin(subcategories)]

    if filename_pattern:
        regex_pattern = fnmatch.translate(filename_pattern)
        df = df[df["file_name"].str.contains(regex_pattern, regex=True)]

    return df["file_name"].tolist()


def _safe_path_segment(segment: str) -> str:
    segment = segment.strip()
    for sep in ("/", "\\"):
        segment = segment.replace(sep, "_")
    while ".." in segment:
        segment = segment.replace("..", "_")
    if segment == ".":
        return "_"
    return segment or "_"


def relative_download_path(row: pd.Series) -> str:
    """Build a relative output path from OSDR category metadata."""
    from pathlib import PurePosixPath

    parts: list[str] = []
    for col in ("category", "subcategory", "subdirectory"):
        val = row.get(col, "")
        if pd.notna(val) and str(val).strip():
            parts.append(_safe_path_segment(str(val)))
    parts.append(_safe_path_segment(str(row["file_name"])))
    return str(PurePosixPath(*parts))


_RAW_SUBCATEGORIES = frozenset(
    {
        "Raw sequence data",
        "Raw Sequence Data",
        "Raw Data",
    }
)


def _longest_common_prefix_trimmed(names: list[str]) -> str:
    if not names:
        return ""
    if len(names) == 1:
        return _study_glds_prefix(names)
    prefix = names[0]
    for name in names[1:]:
        while not name.startswith(prefix):
            prefix = prefix[:-1]
            if not prefix:
                return ""
    if prefix and not prefix.endswith("_"):
        cut = prefix.rfind("_")
        prefix = prefix[: cut + 1] if cut >= 0 else ""
    return prefix


def _study_glds_prefix(filenames: list[str]) -> str:
    for name in filenames:
        if name.startswith("GLDS-"):
            acc, _, rest = name.partition("_")
            if rest or name.endswith("_"):
                return f"{acc}_"
    return ""


def raw_data_filenames(filenames: list[str], df: pd.DataFrame) -> list[str]:
    """Return selected filenames that look like raw sequencing inputs."""
    if not filenames or "file_name" not in df.columns:
        return []
    lookup = df.set_index("file_name", drop=False)
    raw: list[str] = []
    for name in filenames:
        if name not in lookup.index:
            continue
        row = lookup.loc[name]
        sub = str(row.get("subcategory", "") or "").strip()
        if sub in _RAW_SUBCATEGORIES or "raw" in sub.lower():
            raw.append(name)
            continue
        lower = name.lower()
        if "raw" in lower and (".fastq" in lower or ".fq" in lower):
            raw.append(name)
    return raw


def detect_strip_prefix(
    filenames: list[str], df: pd.DataFrame, level: int
) -> str | None:
    """Suggest a filename prefix to strip (cycle 1=study, 2=raw LCP, 3=all LCP)."""
    if level <= 0 or not filenames:
        return None
    glds = [name for name in filenames if name.startswith("GLDS-")]
    if not glds:
        return None
    floor = _study_glds_prefix(glds)
    if level == 1:
        return floor or None
    pool = raw_data_filenames(filenames, df) if level == 2 else glds
    if len(pool) < 2:
        pool = glds
    prefix = _longest_common_prefix_trimmed(pool)
    if floor and (not prefix or len(prefix) < len(floor)):
        prefix = floor
    return prefix or None


def apply_strip_prefix(filename: str, prefix: str | None) -> str:
    if prefix and filename.startswith(prefix):
        return filename[len(prefix) :]
    return filename


def count_strip_affected(filenames: list[str], prefix: str | None) -> int:
    """How many filenames would change when applying strip_prefix."""
    if not prefix:
        return 0
    return sum(1 for name in filenames if name.startswith(prefix))


def strip_output_path(relative_path: str, prefix: str | None) -> str:
    """Apply strip prefix to the basename of a relative output path."""
    from pathlib import PurePosixPath

    if not prefix:
        return relative_path
    path = PurePosixPath(relative_path)
    stripped = apply_strip_prefix(path.name, prefix)
    if stripped == path.name:
        return relative_path
    if path.parent.parts:
        return str(path.parent / stripped)
    return stripped


def find_download_path_collisions(
    filenames: list[str],
    *,
    df: pd.DataFrame | None = None,
    preserve_dirs: bool = False,
    strip_prefix: str | None = None,
) -> dict[str, list[str]]:
    """Map output path -> original filenames when multiple inputs share one destination."""
    if not filenames:
        return {}

    by_name = df.set_index("file_name", drop=False) if df is not None else None
    buckets: dict[str, list[str]] = {}
    for name in filenames:
        if preserve_dirs and by_name is not None and name in by_name.index:
            output_path = relative_download_path(by_name.loc[name])
        else:
            output_path = name
        if strip_prefix:
            output_path = strip_output_path(output_path, strip_prefix)
        buckets.setdefault(output_path, []).append(name)
    return {path: orig for path, orig in buckets.items() if len(orig) > 1}


def find_strip_collisions(
    filenames: list[str],
    prefix: str | None,
    *,
    df: pd.DataFrame | None = None,
    preserve_dirs: bool = False,
) -> dict[str, list[str]]:
    """Map output path -> original filenames when stripping would collide on disk."""
    if not prefix:
        return {}
    return find_download_path_collisions(
        filenames,
        df=df,
        preserve_dirs=preserve_dirs,
        strip_prefix=prefix,
    )


def filenames_from_isa_assay(
    assay_path: Path, column: str = "Raw Data File"
) -> list[str]:
    """Read filenames from an ISA assay table column (comma-separated values supported)."""
    assay_tab = pd.read_csv(assay_path, sep="\t")
    if column not in assay_tab.columns:
        raise ValueError(
            f"Column '{column}' not found in {assay_path.name}. "
            f"Available columns: {list(assay_tab.columns)}"
        )

    filenames: list[str] = []
    for entry in assay_tab[column].dropna():
        entry = str(entry).strip()
        if not entry:
            continue
        for part in entry.split(","):
            part = part.strip()
            if part:
                filenames.append(part)

    return list(dict.fromkeys(filenames))


def resolve_filenames_on_osdr(
    accession: str, target_filenames: list[str]
) -> tuple[list[str], list[str]]:
    """Split target filenames into those available on OSDR and those missing."""
    available = set(get_table_of_files(accession)["file_name"])
    found = [name for name in target_filenames if name in available]
    missing = [name for name in target_filenames if name not in available]
    return found, missing


def format_file_hierarchy(accession: str) -> str:
    """Format OSDR file categories/subcategories as a hierarchy (matches repository UI)."""
    df = get_table_of_files(accession)
    lines = [accession]
    for category, cat_df in df.groupby("category", sort=True):
        lines.append(f"  {category} ({len(cat_df)})")
        for subcategory, sub_df in cat_df.groupby("subcategory", sort=True):
            if pd.isna(subcategory) or subcategory == "":
                continue
            lines.append(f"    {subcategory} ({len(sub_df)})")
    return "\n".join(lines)

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
