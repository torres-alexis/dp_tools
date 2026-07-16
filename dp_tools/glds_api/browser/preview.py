"""Confirm-screen previews and scroll helpers."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.files import FileMaps
from dp_tools.glds_api.browser.models import PreviewRow

PATH_TREE_FILES_KEY = "__files__"


def format_file_size(num_bytes: int | float | None) -> str:
    """Format API file_size (bytes) for display."""
    if num_bytes is None or pd.isna(num_bytes):
        return ""
    size = max(0.0, float(num_bytes))
    if size < 1024:
        return f"{int(size)} B"
    for unit in ("KB", "MB", "GB", "TB"):
        size /= 1024
        if size < 1024:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} PB"


def coerce_file_size(value: object) -> int:
    if value is None or pd.isna(value):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def columnize_filenames(filenames: list[str], width: int) -> list[str]:
    """Pack filenames into terminal lines using as many columns as fit."""
    width = max(1, width - 1)
    if not filenames:
        return []

    max_len = max(len(name) for name in filenames)
    if max_len >= width:
        return filenames

    col_width = max_len + 2
    ncols = max(1, width // col_width)
    nrows = math.ceil(len(filenames) / ncols)
    lines: list[str] = []
    for row in range(nrows):
        parts: list[str] = []
        for col in range(ncols):
            idx = row * ncols + col
            if idx >= len(filenames):
                break
            parts.append(filenames[idx].ljust(col_width))
        lines.append("".join(parts).rstrip())
    return lines


def df_by_filename(df: pd.DataFrame) -> pd.DataFrame:
    return df.set_index("file_name", drop=False)


def _insert_path(tree: dict, parts: tuple[str, ...], filename: str) -> None:
    node = tree
    for part in parts:
        node = node.setdefault(part, {})
    node.setdefault(PATH_TREE_FILES_KEY, []).append(filename)


def _flatten_path_tree(
    node: dict, depth: int, next_rank: list[int]
) -> list[PreviewRow]:
    rows: list[PreviewRow] = []
    indent = "  " * depth
    for dirname in sorted(k for k in node if k != PATH_TREE_FILES_KEY):
        rows.append(PreviewRow(f"{indent}{dirname}/"))
        rows.extend(_flatten_path_tree(node[dirname], depth + 1, next_rank))
    for filename in sorted(node.get(PATH_TREE_FILES_KEY, [])):
        rows.append(PreviewRow(f"{indent}{filename}", next_rank[0]))
        next_rank[0] += 1
    return rows


def build_path_preview_rows_from_ids(
    file_ids: list[str],
    maps: FileMaps,
    *,
    strip_prefix: str | None = None,
) -> list[PreviewRow]:
    if not file_ids:
        return []
    tree: dict = {}
    for file_id in file_ids:
        row = maps.id_to_row[file_id]
        rel = Path(commons.relative_download_path(row))
        parts = rel.parts
        if len(parts) == 1:
            leaf = commons.apply_strip_prefix(parts[0], strip_prefix)
            _insert_path(tree, (), leaf)
        else:
            leaf = commons.apply_strip_prefix(parts[-1], strip_prefix)
            _insert_path(tree, parts[:-1], leaf)
    return _flatten_path_tree(tree, 0, [0])


def build_path_preview_rows(
    filenames: list[str],
    df: pd.DataFrame,
    *,
    strip_prefix: str | None = None,
) -> list[PreviewRow]:
    if not filenames:
        return []
    by_name = df_by_filename(df)
    sorted_names = sorted(filenames)
    tree: dict = {}
    for name in sorted_names:
        rel = Path(commons.relative_download_path(by_name.loc[name]))
        parts = rel.parts
        if len(parts) == 1:
            leaf = commons.apply_strip_prefix(parts[0], strip_prefix)
            _insert_path(tree, (), leaf)
        else:
            leaf = commons.apply_strip_prefix(parts[-1], strip_prefix)
            _insert_path(tree, parts[:-1], leaf)
    return _flatten_path_tree(tree, 0, [0])


def path_preview_lines(
    filenames: list[str],
    df: pd.DataFrame,
    *,
    strip_prefix: str | None = None,
) -> list[str]:
    """Indented output paths for confirm preview when preserve dirs is on."""
    return [
        row.text
        for row in build_path_preview_rows(filenames, df, strip_prefix=strip_prefix)
    ]


def visible_file_range_from_preview_rows(
    rows: list[PreviewRow], scroll: int, max_rows: int, total_files: int
) -> tuple[int, int]:
    if total_files == 0:
        return 0, 0
    scroll = clamp_list_scroll(scroll, len(rows), max_rows)

    def _range_from(start: int) -> tuple[int, int] | None:
        visible = [
            row.display_rank
            for row in rows[start : start + max_rows]
            if row.display_rank is not None
        ]
        if not visible:
            return None
        return min(visible) + 1, max(visible) + 1

    found = _range_from(scroll)
    if found:
        return found

    start = scroll
    while start < len(rows) and rows[start].display_rank is None:
        start += 1
    found = _range_from(start)
    if found:
        return found

    prior = next(
        (
            row.display_rank
            for row in reversed(rows[:scroll])
            if row.display_rank is not None
        ),
        None,
    )
    if prior is not None:
        return prior + 1, prior + 1

    return 1, min(total_files, max_rows)


def visible_filename_range(
    filenames: list[str], width: int, scroll: int, max_rows: int
) -> tuple[int, int]:
    """Return 1-based inclusive indices of filenames visible in the viewport."""
    n = len(filenames)
    if n == 0:
        return 0, 0

    usable_width = max(1, width - 1)
    max_len = max(len(name) for name in filenames)
    if max_len >= usable_width:
        start = max(0, min(scroll, n - 1))
        end = min(start + max(1, max_rows), n)
        return start + 1, end

    col_width = max_len + 2
    ncols = max(1, usable_width // col_width)
    nrows = math.ceil(n / ncols)
    scroll = max(0, min(scroll, max(0, nrows - max_rows)))

    visible: list[int] = []
    for line in range(scroll, min(scroll + max_rows, nrows)):
        for col in range(ncols):
            idx = line * ncols + col
            if idx < n:
                visible.append(idx)
    if not visible:
        return 1, min(n, max_rows)
    return visible[0] + 1, visible[-1] + 1


def clamp_list_scroll(scroll: int, line_count: int, max_rows: int) -> int:
    if line_count <= 0 or max_rows < 1:
        return 0
    return max(0, min(scroll, max(0, line_count - max_rows)))
