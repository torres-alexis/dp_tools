"""Mutable session state for the interactive file browser."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.files import (
    FileMaps,
    build_file_maps,
    file_ids_to_names,
    find_selection_path_collisions,
    selected_size_bytes,
    sorted_file_ids,
)
from dp_tools.glds_api.browser.filter import (
    count_filter_matches,
    filter_pattern_valid,
    prune_checked_for_filter,
)
from dp_tools.glds_api.browser.help_text import (
    DEFAULT_STATUS,
    STATUS_FLASH_SECONDS,
)
from dp_tools.glds_api.browser.models import (
    BrowserMode,
    FlattenCache,
    PreviewRow,
    TreeNode,
)
from dp_tools.glds_api.browser.preview import (
    build_path_preview_rows_from_ids,
    columnize_filenames,
)
from dp_tools.glds_api.browser.tree import build_tree


@dataclass
class BrowserSession:
    osd_id: str
    df: pd.DataFrame
    tree: list[TreeNode]
    maps: FileMaps
    checked: set[str] = field(default_factory=set)
    preserve_dirs: bool = False
    output_dir: str = "."
    jobs: int = 10
    mode: BrowserMode = "browse"
    cursor: int = 0
    scroll: int = 0
    status: str = DEFAULT_STATUS
    status_until: float = 0.0
    confirm_idx: int = 1  # default highlight: download
    confirm_scroll: int = 0
    confirm_labels: list[tuple[str, str]] = field(
        default_factory=lambda: [("q", "exit"), ("y", "download"), ("c", "copy cmd")]
    )
    confirm_status: str = ""
    confirm_status_until: float = 0.0
    strip_enabled: bool = False
    strip_level: int = 2
    strip_prefixes: dict[int, str | None] = field(default_factory=dict)
    strip_custom_levels: set[int] = field(default_factory=set)
    strip_editing: bool = False
    strip_edit_buffer: str = ""
    strip_edit_cursor: int = 0
    confirm_preview_key: tuple | None = None
    confirm_names: list[str] = field(default_factory=list)
    confirm_path_rows: list[PreviewRow] = field(default_factory=list)
    confirm_preview_lines: list[str] = field(default_factory=list)
    confirm_flat_lines: list[str] = field(default_factory=list)
    confirm_display_names: list[str] = field(default_factory=list)
    browse_flatten_cache: FlattenCache = field(default_factory=FlattenCache)
    filter_flatten_cache: FlattenCache = field(default_factory=FlattenCache)
    tree_epoch: int = 0
    filter_enabled: bool = False
    filter_level: int = 1
    filter_patterns: dict[int, str] = field(
        default_factory=lambda: {1: "", 2: "", 3: ""}
    )
    filter_inverted: bool = False
    filter_draft_level: int = 1
    filter_draft_invert: bool = False
    filter_draft_buffer: str = ""
    filter_draft_cursor: int = 0
    filter_draft_patterns: dict[int, str] = field(
        default_factory=lambda: {1: "", 2: "", 3: ""}
    )
    filter_cursor: int = 0
    filter_scroll: int = 0
    filter_status: str = ""
    show_help: bool = False
    help_scroll: int = 0
    _confirm_selection_key: frozenset[str] | None = field(default=None, repr=False)

    @classmethod
    def create(
        cls,
        osd_id: str,
        df: pd.DataFrame,
        *,
        preserve_dirs: bool = False,
        output_dir: str = ".",
        jobs: int = 10,
    ) -> BrowserSession:
        return cls(
            osd_id=osd_id,
            df=df,
            tree=build_tree(df),
            maps=build_file_maps(df),
            preserve_dirs=preserve_dirs,
            output_dir=output_dir,
            jobs=jobs,
        )

    def selected_filenames(self) -> list[str]:
        return file_ids_to_names(self.checked, self.maps)

    def selected_bytes(self) -> int:
        return selected_size_bytes(self.maps, self.checked)

    def tick_status(self) -> None:
        if self.status != DEFAULT_STATUS and time.monotonic() >= self.status_until:
            self.status = DEFAULT_STATUS
        if self.confirm_status and time.monotonic() >= self.confirm_status_until:
            self.confirm_status = ""

    def needs_status_poll(self) -> bool:
        if self.status != DEFAULT_STATUS and time.monotonic() < self.status_until:
            return True
        if self.confirm_status and time.monotonic() < self.confirm_status_until:
            return True
        return False

    def flash_status(self, message: str) -> None:
        self.status = message
        self.status_until = time.monotonic() + STATUS_FLASH_SECONDS

    def flash_confirm_status(self, message: str) -> None:
        self.confirm_status = message
        self.confirm_status_until = time.monotonic() + STATUS_FLASH_SECONDS

    def reset_strip_state(self) -> None:
        self.strip_enabled = False
        self.strip_level = 2
        self.strip_prefixes.clear()
        self.strip_custom_levels.clear()
        self.strip_editing = False
        self.strip_edit_buffer = ""
        self.strip_edit_cursor = 0

    def current_filter_pattern(self) -> str:
        return self.filter_patterns.get(self.filter_level, "")

    def filter_is_active(self) -> bool:
        return self.filter_enabled and bool(self.current_filter_pattern())

    def all_display_names(self) -> list[str]:
        return [self.maps.id_to_name[fid] for fid in self.maps.ordered_ids]

    def filter_match_counts(
        self,
        pattern: str | None = None,
        level: int | None = None,
        *,
        invert: bool | None = None,
        active: bool | None = None,
    ) -> tuple[int, int]:
        pat = self.current_filter_pattern() if pattern is None else pattern
        lvl = self.filter_level if level is None else level
        inv = self.filter_inverted if invert is None else invert
        is_active = self.filter_is_active() if active is None else active
        return count_filter_matches(
            self.all_display_names(), pat, lvl, invert=inv, active=is_active
        )

    def draft_filter_active(self) -> bool:
        return bool(self.filter_draft_buffer)

    def draft_match_counts(self) -> tuple[int, int]:
        return count_filter_matches(
            self.all_display_names(),
            self.filter_draft_buffer,
            self.filter_draft_level,
            invert=self.filter_draft_invert,
            active=self.draft_filter_active(),
        )

    def enter_filter_mode(self) -> None:
        self.mode = "filter"
        self.filter_draft_patterns = dict(self.filter_patterns)
        self.filter_draft_level = self.filter_level
        self.filter_draft_invert = self.filter_inverted
        self.filter_draft_buffer = self.filter_draft_patterns.get(self.filter_level, "")
        self.filter_draft_cursor = len(self.filter_draft_buffer)
        self.filter_cursor = 0
        self.filter_scroll = 0
        self.filter_status = ""

    def cancel_filter_mode(self) -> None:
        self.mode = "browse"
        self.filter_status = ""

    def commit_filter(self) -> str | None:
        if not filter_pattern_valid(self.filter_draft_buffer, self.filter_draft_level):
            return "Invalid regex pattern."
        self.filter_draft_patterns[self.filter_draft_level] = self.filter_draft_buffer
        self.filter_patterns.update(self.filter_draft_patterns)
        self.filter_level = self.filter_draft_level
        self.filter_inverted = self.filter_draft_invert
        self.filter_enabled = bool(self.filter_draft_buffer)
        removed = 0
        if self.filter_enabled:
            removed = prune_checked_for_filter(
                self.checked,
                self.filter_draft_buffer,
                self.filter_draft_level,
                invert=self.filter_draft_invert,
                names_by_id=self.maps.id_to_name,
            )
            if removed:
                self.invalidate_confirm_preview()
        self.mode = "browse"
        self.filter_cursor = 0
        self.filter_scroll = 0
        self.cursor = 0
        self.scroll = 0
        if removed:
            self.flash_status(f"Filter applied. Removed {removed} hidden selection(s).")
        elif self.filter_enabled:
            self.flash_status("Filter applied.")
        else:
            self.flash_status("Filter cleared.")
        return None

    def current_strip_prefix(self) -> str | None:
        if not self.strip_enabled:
            return None
        return self.strip_prefixes.get(self.strip_level)

    def detect_strip_for_level(self, names: list[str], level: int) -> str | None:
        return commons.detect_strip_prefix(names, self.df, level)

    def ensure_strip_level(self, names: list[str], level: int) -> None:
        if level in self.strip_custom_levels:
            return
        self.strip_prefixes[level] = self.detect_strip_for_level(names, level)

    def fill_all_strip_levels(self, names: list[str]) -> None:
        for level in (1, 2, 3):
            self.ensure_strip_level(names, level)

    def invalidate_confirm_preview(self) -> None:
        self.confirm_preview_key = None

    def ensure_confirm_preview(self, term_width: int, active_prefix: str | None) -> None:
        key = (frozenset(self.checked), self.preserve_dirs, active_prefix, term_width)
        if key == self.confirm_preview_key:
            return
        self.confirm_names = self.selected_filenames()
        if self.preserve_dirs:
            self.confirm_path_rows = build_path_preview_rows_from_ids(
                sorted_file_ids(self.checked),
                self.maps,
                strip_prefix=active_prefix,
            )
            self.confirm_preview_lines = [row.text for row in self.confirm_path_rows]
            self.confirm_flat_lines = []
            self.confirm_display_names = []
        else:
            self.confirm_path_rows = []
            self.confirm_preview_lines = []
            self.confirm_display_names = [
                commons.apply_strip_prefix(name, active_prefix)
                for name in self.confirm_names
            ]
            self.confirm_flat_lines = columnize_filenames(
                self.confirm_display_names, term_width
            )
        self.confirm_preview_key = key

    def strip_collision_message(self) -> str | None:
        prefix = self.current_strip_prefix()
        if not prefix:
            return None
        collisions = find_selection_path_collisions(
            sorted(self.checked),
            self.maps,
            preserve_dirs=self.preserve_dirs,
            strip_prefix=prefix,
        )
        if not collisions:
            return None
        output_path, originals = next(iter(collisions.items()))
        extra = len(originals) - 2
        suffix = f" (+{extra} more)" if extra > 0 else ""
        return (
            f"Strip prefix collision for output path {output_path!r}: "
            f"{originals[0]!r}, {originals[1]!r}{suffix}"
        )

    def enter_confirm_mode(self) -> None:
        selection_key = frozenset(self.checked)
        same_selection = selection_key == self._confirm_selection_key
        self.mode = "confirm"
        self.confirm_idx = 0
        self.confirm_scroll = 0
        if not same_selection:
            self.reset_strip_state()
            self.invalidate_confirm_preview()
        self._confirm_selection_key = selection_key
