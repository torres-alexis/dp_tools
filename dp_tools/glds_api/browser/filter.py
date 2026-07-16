"""Filename filter matching for the interactive browser."""

from __future__ import annotations

import fnmatch
import re

FILTER_TYPE_LABELS = {1: "contains", 2: "glob", 3: "regex"}


def filter_type_label(level: int, *, invert: bool = False) -> str:
    base = FILTER_TYPE_LABELS.get(level, "contains")
    if not invert:
        return base
    if level == 1:
        return "excludes"
    return f"not {base}"


def filter_mode_hint(invert: bool) -> str:
    return "exclude" if invert else "include"


def file_matches_filter(
    name: str, pattern: str, level: int, *, invert: bool = False
) -> bool:
    if not pattern:
        return True
    if level == 1:
        matched = pattern.lower() in name.lower()
    elif level == 2:
        matched = fnmatch.fnmatch(name, pattern)
    else:
        try:
            matched = re.search(pattern, name) is not None
        except re.error:
            matched = False
    return (not matched) if invert else matched


def filter_pattern_valid(pattern: str, level: int) -> bool:
    if level != 3 or not pattern:
        return True
    try:
        re.compile(pattern)
    except re.error:
        return False
    return True


def prune_checked_for_filter(
    checked: set[str],
    pattern: str,
    level: int,
    *,
    invert: bool = False,
    names_by_id: dict[str, str] | None = None,
) -> int:
    """Drop checked files that do not match the filter. Returns count removed."""
    if not pattern:
        return 0
    to_remove = [
        file_id
        for file_id in checked
        if not file_matches_filter(
            (names_by_id or {}).get(file_id, file_id),
            pattern,
            level,
            invert=invert,
        )
    ]
    for file_id in to_remove:
        checked.discard(file_id)
    return len(to_remove)


def count_filter_matches(
    filenames: list[str],
    pattern: str,
    level: int,
    *,
    invert: bool = False,
    active: bool = True,
) -> tuple[int, int]:
    total = len(filenames)
    if not active or not pattern:
        return total, total
    matches = sum(
        1
        for name in filenames
        if file_matches_filter(name, pattern, level, invert=invert)
    )
    return matches, total
