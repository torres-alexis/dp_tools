import enum
from pathlib import Path
import sys
import requests
import os
import shlex
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
from loguru import logger
import pandas as pd

from dp_tools.glds_api import commons, isa
from dp_tools.core.files import isa_archive

@click.group()
def osd():
    pass


def _download_with_requests(filename: str, url: str, output_dir: Path) -> None:
    logger.info(f"Downloading {filename}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(output_dir / filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def _download_with_parallel(
    downloads: list[tuple[str, str]], output_dir: Path, jobs: int
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    commands = [
        "curl -L -s -o "
        f"{shlex.quote(str(output_dir / filename))} "
        f"{shlex.quote(url)}"
        for filename, url in downloads
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as cmd_file:
        cmd_file.write("\n".join(commands) + "\n")
        cmd_path = cmd_file.name

    try:
        subprocess.run(
            f"parallel --xapply -j {jobs} < {shlex.quote(cmd_path)}",
            shell=True,
            check=True,
        )
    finally:
        os.unlink(cmd_path)


def _download_with_threads(
    downloads: list[tuple[str, str]], output_dir: Path, jobs: int
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [
            executor.submit(_download_with_requests, filename, url, output_dir)
            for filename, url in downloads
        ]
        for future in as_completed(futures):
            future.result()


def _run_downloads(
    osd_id: str,
    downloads: list[tuple[str, str]],
    dry_run: bool,
    non_interactive: bool,
    output_dir: str,
    jobs: int,
) -> None:
    if not downloads:
        raise click.ClickException("No files matched the selection.")

    if dry_run:
        logger.info("Output pairs of 'url' 'filename'")
        for filename, url in downloads:
            click.echo(f"{url} {filename}")
        return

    if not non_interactive:
        input(f"Ready to download {len(downloads)} file(s)... Press enter to continue.")

    output_path = Path(output_dir)
    if shutil.which("parallel") and shutil.which("curl"):
        logger.info(f"Downloading {len(downloads)} file(s) with GNU parallel (-j {jobs})")
        _download_with_parallel(downloads, output_path, jobs)
    else:
        if not shutil.which("parallel"):
            logger.warning("GNU parallel not found; falling back to threaded downloads")
        if not shutil.which("curl"):
            logger.warning("curl not found; falling back to threaded downloads")
        logger.info(f"Downloading {len(downloads)} file(s) with {jobs} worker(s)")
        _download_with_threads(downloads, output_path, jobs)


def _build_downloads(osd_id: str, filenames: list[str]) -> list[tuple[str, str]]:
    return [
        (filename, commons.retrieve_file_url(accession=osd_id, filename=filename))
        for filename in filenames
    ]


@click.command()
@click.argument("osd-id")
@click.argument("file-pattern", required=False)
@click.option("--dry-run", default=False, is_flag=True, help="Output urls and filenames only.")
@click.option("-y", "--y", "non_interactive", default=False, is_flag=True, help="Download files without a prompt")
@click.option("-o", "--output-dir", default=".", show_default=True, type=click.Path(), help="Directory for downloaded files.")
@click.option("-j", "--jobs", default=10, show_default=True, type=int, help="Number of parallel downloads.")
@click.option("-a", "--isa-assay", type=click.Path(exists=True, dir_okay=False), help="ISA assay table (a_*.txt); downloads filenames from its data-file column.")
@click.option("--data-file-column", default="Raw Data File", show_default=True, help="ISA assay column listing files to download (used with --isa-assay).")
@click.option("-c", "--category", multiple=True, help="OSDR repository category (repeatable). Use --list-categories to see options.")
@click.option("--subcategory", multiple=True, help="OSDR repository subcategory (repeatable; narrows --category selection).")
@click.option("--list-categories", is_flag=True, help="List file categories for this study and exit.")
def download_files(
    osd_id,
    file_pattern,
    dry_run,
    non_interactive,
    output_dir,
    jobs,
    isa_assay,
    data_file_column,
    category,
    subcategory,
    list_categories,
):
    if list_categories:
        click.echo(commons.format_file_hierarchy(osd_id))
        return

    if isa_assay:
        if category or subcategory or file_pattern:
            raise click.UsageError(
                "--isa-assay cannot be combined with FILE-PATTERN or --category."
            )
    elif not category and not file_pattern:
        raise click.UsageError(
            "Specify FILE-PATTERN, --isa-assay / -a, or --category / -c. "
            "Use --list-categories to inspect repository categories."
        )

    logger.info(f"Fetching file list for {osd_id}")

    if isa_assay:
        target_filenames = commons.filenames_from_isa_assay(
            Path(isa_assay), column=data_file_column
        )
        if not target_filenames:
            raise click.ClickException(
                f"No filenames found in column '{data_file_column}' of {isa_assay}."
            )
        filenames, missing = commons.resolve_filenames_on_osdr(osd_id, target_filenames)
        if missing:
            logger.warning(
                f"{len(missing)} file(s) from the assay table are not on OSDR: {missing[:5]}"
                + (" ..." if len(missing) > 5 else "")
            )
        if not filenames:
            raise click.ClickException(
                "None of the assay-table filenames are available on OSDR for this accession."
            )
        logger.info(
            f"Resolved {len(filenames)} file(s) from {Path(isa_assay).name} "
            f"column '{data_file_column}'"
        )
    else:
        categories = list(category) if category else None
        subcategories = list(subcategory) if subcategory else None
        filenames = commons.filter_filenames(
            accession=osd_id,
            filename_pattern=file_pattern,
            categories=categories,
            subcategories=subcategories,
        )
        if category:
            logger.info(
                f"Found {len(filenames)} file(s) in categories {list(category)}"
                + (f" subcategories {list(subcategory)}" if subcategory else "")
                + (f" matching pattern {file_pattern!r}" if file_pattern else "")
            )
        else:
            logger.info(
                f"Found {len(filenames)} file(s) matching glob pattern: {file_pattern}"
            )

    _run_downloads(
        osd_id,
        _build_downloads(osd_id, filenames),
        dry_run,
        non_interactive,
        output_dir,
        jobs,
    )


@click.command()
@click.argument("osd-id")
@click.option("-o", "--output", default="samples.txt", show_default=True, help="File to write sample names.")
@click.option("-t", "--table-index", type=int, default=None, help="ISA table index to use when multiple a_*/s_* tables exist.")
@click.option("-i", "--interactive", is_flag=True, help="Prompt to select an ISA table when multiple exist.")
def get_samples(osd_id, output, table_index, interactive):
    logger.info(f"Fetching information for {osd_id}")

    isa_path = isa.download_isa(accession=osd_id)

    if isa_path is None:
        logger.error(f"No ISA archive found for {osd_id}. Cannot extract samples.")
        sys.exit(1)

    isa_tables = sorted(
        [
            f
            for f in isa_archive.fetch_isa_files(Path(isa_path))
            if f.name.startswith("a_") or f.name.startswith("s_")
        ],
        key=lambda p: p.name,
    )
    isa_tables_by_index = {i: f for i, f in enumerate(isa_tables)}

    logger.info(f"Found ISA tables: {[f.name for f in isa_tables]}")

    assay_files = [f for f in isa_tables if f.name.startswith("a_")]

    if len(assay_files) == 1:
        target_file = assay_files[0]
        logger.info(f"Automatically selected the only assay file: {target_file.name}")
    elif table_index is not None:
        if table_index not in isa_tables_by_index:
            raise click.ClickException(
                f"Invalid --table-index {table_index}. "
                f"Valid indices: 0–{len(isa_tables) - 1}."
            )
        target_file = isa_tables_by_index[table_index]
        logger.info(f"Selected table {table_index}: {target_file.name}")
    elif interactive:
        for i, f in isa_tables_by_index.items():
            click.echo(f"{i}: {f.name}", err=True)
        selection = click.prompt("Select a table file by number", type=int, err=True)
        if selection not in isa_tables_by_index:
            raise click.ClickException(f"Invalid choice: {selection}")
        target_file = isa_tables_by_index[selection]
        logger.info(f"Selected {target_file.name}")
    else:
        click.echo("Multiple ISA tables found. Choose one with --table-index:", err=True)
        for i, f in isa_tables_by_index.items():
            click.echo(f"  {i}: {f.name}", err=True)
        raise click.ClickException(
            f"Re-run with: dpt osd get-samples {osd_id} --table-index <N> "
            f"(or -i / --interactive to choose at a prompt)."
        )

    samples = [s.strip() for s in pd.read_csv(target_file, sep="\t")["Sample Name"]]

    logger.info(f"Found {len(samples)} samples. Outputting to {output}.")
    with open(output, "w") as f:
        for s in samples:
            f.write(s + "\n")


@click.command()
@click.argument("osd-id")
@click.option("--includes-assay-type", default=None)
@click.option("--includes-assay-type-on-platform", default=None)
@click.option("--includes-file-pattern", default=None)
@click.option("--excludes-file-pattern", default=None)
def check_if(osd_id, includes_assay_type, includes_assay_type_on_platform, includes_file_pattern, excludes_file_pattern):
    failed_criteria = None
    logger.info(f"Fetching information for {osd_id}")


    files = commons.get_table_of_files(osd_id)
    logger.info(f"Found {len(files)} files associated to {osd_id}")
    isa_path = isa.download_isa(accession = osd_id)

    try: # Try here is ensure isa path is deleted regardless of success
        ### Describe/Filter on Assay Types
        if includes_assay_type and failed_criteria == None:
            query_measurement, query_technology = includes_assay_type.split(",")
            assays_subtable = isa_archive.isa_investigation_subtables(Path(isa_path))["STUDY ASSAYS"]

            logger.trace(assays_subtable)
            # Add OSD ID
            assays_subtable["OSD_ID"] = osd_id

            # Add GLDS-ID
            # Assumes all files have GLDS number as prefix
            assays_subtable["GLDS_ID"] = files.iloc[0]['file_name'].split("_")[0]

            assays_subtable.set_index(["OSD_ID","GLDS_ID"], inplace = True)

            logger.trace(f"Found assay types: {assays_subtable}")

            # Check if desired assay type exists
            if assays_subtable.loc[
                            (assays_subtable["Study Assay Measurement Type"] == query_measurement) & 
                            (assays_subtable["Study Assay Technology Type"] == query_technology)
                            ].empty:
                failed_criteria = f"Could not find {query_measurement},{query_technology} in:\n {assays_subtable.to_dict(orient='records')}"
        
        ### Describe/Filter on Assay Types AND platform
        if includes_assay_type_on_platform and failed_criteria == None:
            query_measurement, query_technology, query_platform = includes_assay_type_on_platform.split(",")
            assays_subtable = isa_archive.isa_investigation_subtables(Path(isa_path))["STUDY ASSAYS"]

            logger.trace(assays_subtable)
            # Add OSD ID
            assays_subtable["OSD_ID"] = osd_id

            # Add GLDS-ID
            # Assumes all files have GLDS number as prefix
            assays_subtable["GLDS_ID"] = files.iloc[0]['file_name'].split("_")[0]

            assays_subtable.set_index(["OSD_ID","GLDS_ID"], inplace = True)

            logger.trace(f"Found assay types: {assays_subtable}")

            # Check if desired assay type exists
            if assays_subtable.loc[
                            (assays_subtable["Study Assay Measurement Type"].str.match(query_measurement)) & 
                            (assays_subtable["Study Assay Technology Type"].str.match(query_technology)) &
                            (assays_subtable["Study Assay Technology Platform"].str.match(query_platform))
                            ].empty:
                failed_criteria = f"Could not find {includes_assay_type_on_platform} in:\n {assays_subtable.to_dict(orient='records')}"


        ### Describe/Filter on files
        if includes_file_pattern and failed_criteria == None:
            logger.info(f"Searching for {len(files)} files")

            logger.trace(files)

            if len(commons.find_matching_filenames(
                            accession = osd_id, 
                            filename_pattern = includes_file_pattern
                            )
                    ) == 0:
                failed_criteria = (f"No files matching pattern '{includes_file_pattern}' could be located.")

        ### Describe/Filter on files
        if excludes_file_pattern and failed_criteria == None:
            logger.info(f"Searching for {len(files)} files")

            logger.trace(files)

            if len(commons.find_matching_filenames(
                            accession = osd_id, 
                            filename_pattern = excludes_file_pattern
                            )
                    ) != 0:
                failed_criteria = (f"Found files matching pattern '{excludes_file_pattern}' could be located.")

        # Check if any failed critera
        if failed_criteria is None:
            logger.success(f"{osd_id} matches supplied criteria!")
        else:
            logger.error(failed_criteria)
            sys.exit(-1)
    finally:
        # Teardown isa path regardless
        if isa_path is not None:
            logger.info(f"Clean up: Removing {isa_path}")
            try:
                os.remove(isa_path)
            except Exception as e:
                logger.warning(f"Failed to clean up {isa_path}: {e}")


osd.add_command(download_files)
osd.add_command(check_if)
osd.add_command(get_samples)
