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
from dp_tools.glds_api.interactive_browser import (
    BrowserAction,
    run_file_browser,
)
from dp_tools.core.files import isa_archive

@click.group()
def osd():
    pass


def _download_with_requests(relative_path: str, url: str, output_dir: Path) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    dest = output_dir / relative_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return relative_path


def _download_with_parallel(
    downloads: list[tuple[str, str]], output_dir: Path, jobs: int
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    commands = []
    for relative_path, url in downloads:
        dest = output_dir / relative_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        commands.append(
            "curl -L -s -o "
            f"{shlex.quote(str(dest))} "
            f"{shlex.quote(url)}"
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as cmd_file:
        cmd_file.write("\n".join(commands) + "\n")
        cmd_path = cmd_file.name

    total = len(downloads)
    try:
        logger.info(f"Running GNU parallel (-j {jobs}) for {total} file(s)")
        subprocess.run(
            f"parallel --bar --xapply -j {jobs} < {shlex.quote(cmd_path)}",
            shell=True,
            check=True,
        )
        logger.success(f"Finished {total} file(s) in {output_dir.resolve()}")
    finally:
        os.unlink(cmd_path)


def _download_with_threads(
    downloads: list[tuple[str, str]], output_dir: Path, jobs: int
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    total = len(downloads)
    done = 0
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_to_name = {
            executor.submit(_download_with_requests, relative_path, url, output_dir): relative_path
            for relative_path, url in downloads
        }
        for future in as_completed(future_to_name):
            filename = future_to_name[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"[failed] {filename}: {exc}")
                raise
            done += 1
            logger.info(f"[{done}/{total}] {filename}")
    logger.success(f"Finished {total} file(s) in {output_dir.resolve()}")


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
        logger.info("Output pairs of 'url' 'relative_output_path'")
        for relative_path, url in downloads:
            click.echo(f"{url} {relative_path}")
        return

    if not non_interactive:
        input(f"Ready to download {len(downloads)} file(s)... Press enter to continue.")

    output_path = Path(output_dir).resolve()
    logger.info(f"Output directory: {output_path}")
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


def _build_downloads(
    osd_id: str,
    filenames: list[str] | None = None,
    *,
    file_ids: list[str] | None = None,
    preserve_dirs: bool = False,
    strip_prefix: str | None = None,
    df: pd.DataFrame | None = None,
) -> list[tuple[str, str]]:
    total = len(file_ids) if file_ids is not None else len(filenames or [])
    if total == 0:
        return []
    if df is None:
        df = commons.get_table_of_files(osd_id)
    if preserve_dirs:
        logger.info("Preserving OSDR category/subdirectory layout under output dir")
    if strip_prefix:
        logger.info(f"Stripping output basename prefix: {strip_prefix!r}")

    if file_ids is not None:
        from dp_tools.glds_api.browser.files import (
            build_download_plan_from_ids,
            build_file_maps,
            find_selection_path_collisions,
        )

        maps = build_file_maps(df)
        collisions = find_selection_path_collisions(
            file_ids,
            maps,
            preserve_dirs=preserve_dirs,
            strip_prefix=strip_prefix,
        )
        if collisions:
            output_path, originals = next(iter(collisions.items()))
            label = "output path" if preserve_dirs else "output name"
            reason = (
                f"Strip prefix {strip_prefix!r} would collide"
                if strip_prefix
                else "Duplicate"
            )
            raise click.ClickException(
                f"{reason} for {label} {output_path!r} "
                f"({originals[0]!r} vs {originals[1]!r})."
            )
        plan = build_download_plan_from_ids(
            file_ids,
            maps,
            preserve_dirs=preserve_dirs,
            strip_prefix=strip_prefix,
        )
    else:
        by_name = df.set_index("file_name", drop=False)
        collisions = commons.find_download_path_collisions(
            filenames or [],
            df=df,
            preserve_dirs=preserve_dirs,
            strip_prefix=strip_prefix,
        )
        if collisions:
            output_path, originals = next(iter(collisions.items()))
            label = "output path" if preserve_dirs else "output name"
            reason = (
                f"Strip prefix {strip_prefix!r} would collide"
                if strip_prefix
                else "Duplicate"
            )
            raise click.ClickException(
                f"{reason} for {label} {output_path!r} "
                f"({originals[0]!r} vs {originals[1]!r})."
            )
        plan = []
        for filename in filenames or []:
            row = by_name.loc[filename]
            if isinstance(row, pd.DataFrame):
                raise click.ClickException(
                    f"Ambiguous filename {filename!r} (multiple repository paths). "
                    "Use `dpt osd browse` to pick files by path."
                )
            relative_path = (
                commons.relative_download_path(row)
                if preserve_dirs
                else filename
            )
            if strip_prefix:
                relative_path = commons.strip_output_path(relative_path, strip_prefix)
            plan.append((relative_path, filename))

    if total > 1:
        logger.info(f"Resolving URLs for {total} file(s)...")
    downloads: list[tuple[str, str]] = []
    for i, (relative_path, filename) in enumerate(plan, start=1):
        downloads.append(
            (relative_path, commons.retrieve_file_url(accession=osd_id, filename=filename))
        )
        if total > 10 and i % max(1, total // 10) == 0:
            logger.info(f"Resolved {i}/{total} URLs...")
    if total > 1:
        logger.info(f"Resolved {total} URL(s)")
    return downloads


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
@click.option("--file", "exact_files", multiple=True, help="Exact filename(s) to download (repeatable).")
@click.option(
    "--preserve-dirs",
    is_flag=True,
    help="Write files under category/subcategory/subdirectory paths from OSDR metadata (default: flat output dir).",
)
@click.option(
    "--strip-prefix",
    default=None,
    help="Remove this prefix from each output basename (API lookup still uses full OSDR names).",
)
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
    exact_files,
    preserve_dirs,
    strip_prefix,
    list_categories,
):
    if list_categories:
        click.echo(commons.format_file_hierarchy(osd_id))
        return

    if exact_files:
        if isa_assay or category or subcategory or file_pattern:
            raise click.UsageError(
                "--file cannot be combined with FILE-PATTERN, --isa-assay, or --category."
            )
        filenames = list(exact_files)
        logger.info(f"Downloading {len(filenames)} explicitly listed file(s)")
    elif isa_assay:
        if category or subcategory or file_pattern:
            raise click.UsageError(
                "--isa-assay cannot be combined with FILE-PATTERN or --category."
            )
    elif not category and not file_pattern:
        raise click.UsageError(
            "Specify FILE-PATTERN, --file, --isa-assay / -a, or --category / -c. "
            "Use --list-categories to inspect repository categories, or "
            "`dpt osd browse` for an interactive picker."
        )

    logger.info(f"Fetching file list for {osd_id}")

    if exact_files:
        pass
    elif isa_assay:
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

    if exact_files:
        available = set(commons.get_table_of_files(osd_id)["file_name"])
        missing = [f for f in filenames if f not in available]
        if missing:
            raise click.ClickException(
                f"File(s) not on OSDR for {osd_id}: {missing[:5]}"
                + (" ..." if len(missing) > 5 else "")
            )

    _run_downloads(
        osd_id,
        _build_downloads(
            osd_id,
            filenames,
            preserve_dirs=preserve_dirs,
            strip_prefix=strip_prefix,
        ),
        dry_run,
        non_interactive,
        output_dir,
        jobs,
    )


@click.command()
@click.argument("osd-id")
@click.option("-o", "--output-dir", default=".", show_default=True, type=click.Path(), help="Directory for downloaded files.")
@click.option("-j", "--jobs", default=10, show_default=True, type=int, help="Number of parallel downloads.")
@click.option(
    "--preserve-dirs",
    is_flag=True,
    help="Write files under category/subcategory/subdirectory paths from OSDR metadata (default: flat output dir).",
)
def browse(osd_id, output_dir, jobs, preserve_dirs):
    """Browse OSDR files interactively (checkbox tree + confirm menu)."""
    try:
        result = run_file_browser(
            osd_id,
            preserve_dirs=preserve_dirs,
            output_dir=output_dir,
            jobs=jobs,
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    if result.action == BrowserAction.EXIT or not result.selected_files:
        return

    _run_downloads(
        osd_id,
        _build_downloads(
            osd_id,
            file_ids=result.selected_file_ids,
            preserve_dirs=result.preserve_dirs,
            strip_prefix=result.strip_prefix,
            df=result.file_table,
        ),
        dry_run=False,
        non_interactive=True,
        output_dir=output_dir,
        jobs=jobs,
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
osd.add_command(browse)
osd.add_command(check_if)
osd.add_command(get_samples)
