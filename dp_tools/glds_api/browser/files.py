"""File identity helpers for the interactive browser."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from dp_tools.glds_api import commons


@dataclass
class FileMaps:
    id_to_name: dict[str, str]
    id_to_row: dict[str, pd.Series]
    name_to_ids: dict[str, list[str]]
    ordered_ids: list[str]


def build_file_maps(df: pd.DataFrame) -> FileMaps:
    """Index study files by relative download path (unique selection key)."""
    id_to_name: dict[str, str] = {}
    id_to_row: dict[str, pd.Series] = {}
    name_to_ids: dict[str, list[str]] = {}
    ordered_ids: list[str] = []
    for _, row in df.iterrows():
        file_id = commons.relative_download_path(row)
        name = str(row["file_name"])
        id_to_name[file_id] = name
        id_to_row[file_id] = row
        name_to_ids.setdefault(name, []).append(file_id)
        ordered_ids.append(file_id)
    return FileMaps(
        id_to_name=id_to_name,
        id_to_row=id_to_row,
        name_to_ids=name_to_ids,
        ordered_ids=ordered_ids,
    )


def sorted_file_ids(file_ids: set[str] | list[str]) -> list[str]:
    return sorted(file_ids)


def file_ids_to_names(file_ids: set[str] | list[str], maps: FileMaps) -> list[str]:
    """One display/API filename per selected file_id (order follows sorted file_ids)."""
    return [
        maps.id_to_name[fid]
        for fid in sorted_file_ids(file_ids)
        if fid in maps.id_to_name
    ]


def output_path_for_row(
    row: pd.Series,
    *,
    preserve_dirs: bool,
    strip_prefix: str | None,
) -> str:
    relative = (
        commons.relative_download_path(row)
        if preserve_dirs
        else str(row["file_name"])
    )
    if strip_prefix:
        return commons.strip_output_path(relative, strip_prefix)
    return relative


def find_selection_path_collisions(
    file_ids: list[str],
    maps: FileMaps,
    *,
    preserve_dirs: bool = False,
    strip_prefix: str | None = None,
) -> dict[str, list[str]]:
    """Map output path -> API filenames when multiple selections share one destination."""
    buckets: dict[str, list[str]] = {}
    for file_id in file_ids:
        row = maps.id_to_row.get(file_id)
        if row is None:
            continue
        name = str(row["file_name"])
        output_path = output_path_for_row(
            row, preserve_dirs=preserve_dirs, strip_prefix=strip_prefix
        )
        buckets.setdefault(output_path, []).append(name)
    return {path: names for path, names in buckets.items() if len(names) > 1}


def build_download_plan_from_ids(
    file_ids: list[str],
    maps: FileMaps,
    *,
    preserve_dirs: bool,
    strip_prefix: str | None,
) -> list[tuple[str, str]]:
    """Return (output_relative_path, api_filename) pairs in file_id order."""
    plan: list[tuple[str, str]] = []
    for file_id in file_ids:
        row = maps.id_to_row[file_id]
        name = str(row["file_name"])
        plan.append(
            (
                output_path_for_row(
                    row, preserve_dirs=preserve_dirs, strip_prefix=strip_prefix
                ),
                name,
            )
        )
    return plan


def ambiguous_basenames(maps: FileMaps, file_ids: list[str]) -> dict[str, list[str]]:
    """Basenames shared by more than one selected file_id."""
    grouped: dict[str, list[str]] = {}
    for file_id in file_ids:
        name = maps.id_to_name.get(file_id)
        if name is None:
            continue
        grouped.setdefault(name, []).append(file_id)
    return {name: ids for name, ids in grouped.items() if len(ids) > 1}


def selected_size_bytes(maps: FileMaps, checked: set[str]) -> int:
    if not checked:
        return 0
    from dp_tools.glds_api.browser.preview import coerce_file_size

    total = 0
    for file_id in checked:
        row = maps.id_to_row.get(file_id)
        if row is not None:
            total += coerce_file_size(row.get("file_size"))
    return total
