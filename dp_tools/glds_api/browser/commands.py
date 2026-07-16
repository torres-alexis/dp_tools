"""Download command builder and clipboard helpers for the interactive browser."""

from __future__ import annotations

import shlex
import shutil
import subprocess

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.files import FileMaps, ambiguous_basenames


def _norm_subcategory(value: object) -> str:
    if pd.isna(value) or not str(value).strip():
        return ""
    return str(value).strip()


def flags_for_selection(df: pd.DataFrame, selected: set[str]) -> dict | None:
    if not selected:
        return None

    for category in sorted(df["category"].dropna().unique()):
        cat_df = df[df["category"] == category]
        cat_files = set(cat_df["file_name"])
        if selected == cat_files:
            return {"categories": [category], "subcategories": []}

        grouped = cat_df.groupby(cat_df["subcategory"].map(_norm_subcategory))
        for subcategory, sub_df in grouped:
            sub_files = set(sub_df["file_name"])
            if selected == sub_files:
                return {
                    "categories": [category],
                    "subcategories": [subcategory] if subcategory else [],
                }

    return None


def format_download_command(
    osd_id: str,
    filenames: list[str],
    *,
    df: pd.DataFrame | None = None,
    file_ids: list[str] | None = None,
    maps: FileMaps | None = None,
    output_dir: str = ".",
    jobs: int = 10,
    dry_run: bool = False,
    non_interactive: bool = True,
    preserve_dirs: bool = False,
    strip_prefix: str | None = None,
) -> str:
    """Build a pasteable dpt osd download-files command for the selection."""
    parts = ["dpt", "osd", "download-files", osd_id]
    if df is None:
        df = commons.get_table_of_files(osd_id)
    if file_ids and maps is None:
        from dp_tools.glds_api.browser.files import build_file_maps

        maps = build_file_maps(df)
    selected_names = set(filenames)
    flags = flags_for_selection(df, selected_names)

    if flags:
        for category in flags["categories"]:
            parts.extend(["-c", shlex.quote(category)])
        for subcategory in flags["subcategories"]:
            parts.extend(["--subcategory", shlex.quote(subcategory)])
    else:
        for name in filenames:
            parts.extend(["--file", shlex.quote(name)])

    parts.extend(["-o", shlex.quote(output_dir), "-j", str(jobs)])
    if preserve_dirs:
        parts.append("--preserve-dirs")
    if strip_prefix:
        parts.extend(["--strip-prefix", shlex.quote(strip_prefix)])
    if dry_run:
        parts.append("--dry-run")
    if non_interactive:
        parts.append("-y")
    command = " ".join(parts)
    if file_ids and maps:
        dupes = ambiguous_basenames(maps, file_ids)
        if dupes:
            names = ", ".join(sorted(dupes))
            command = (
                f"# Same basename in multiple paths ({names}); "
                f"prefer browse download (y) over this command.\n{command}"
            )
    return command


def executable_download_command(command: str) -> str:
    """Drop leading comment lines from a formatted command string."""
    lines = command.splitlines()
    while lines and lines[0].startswith("#"):
        lines.pop(0)
    return lines[0].strip() if lines else command.strip()


def copy_download_command(command: str) -> bool:
    """Copy a shell-ready download command to the clipboard."""
    return copy_to_clipboard(executable_download_command(command))


def copy_to_clipboard(text: str) -> bool:
    """Copy text to the system clipboard."""
    payload = text.encode("utf-8")
    for argv in (
        ["wl-copy"],
        ["xclip", "-selection", "clipboard"],
        ["xsel", "--clipboard", "--input"],
        ["pbcopy"],
        ["clip.exe"],
    ):
        if not shutil.which(argv[0]):
            continue
        try:
            subprocess.run(
                argv,
                input=payload,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except (OSError, subprocess.CalledProcessError):
            continue
    return False
