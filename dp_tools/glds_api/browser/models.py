"""Shared datatypes for the interactive file browser."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

import pandas as pd

BrowserMode = Literal["browse", "filter", "confirm"]


class BrowserAction(str, Enum):
    EXIT = "exit"
    DOWNLOAD = "download"


@dataclass
class BrowserResult:
    action: BrowserAction
    selected_files: list[str] = field(default_factory=list)
    selected_file_ids: list[str] = field(default_factory=list)
    preserve_dirs: bool = False
    strip_prefix: str | None = None
    file_table: pd.DataFrame | None = None


@dataclass
class FlattenCache:
    key: tuple | None = None
    rows: list[tuple["TreeNode", str]] = field(default_factory=list)


@dataclass
class TreeNode:
    kind: str
    label: str
    depth: int
    filename: str | None = None
    file_id: str | None = None
    category: str | None = None
    subcategory: str | None = None
    file_count: int = 0
    size_bytes: int = 0
    expanded: bool = True
    children: list["TreeNode"] = field(default_factory=list)


@dataclass
class PreviewRow:
    text: str
    display_rank: int | None = None
