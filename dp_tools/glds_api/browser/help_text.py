"""Help screen copy for the interactive browser."""

from __future__ import annotations

DEFAULT_STATUS = "Space toggle  g confirm  f filter  ? help  q quit"
STATUS_FLASH_SECONDS = 2.0

BROWSE_HELP_LINES = [
    "Browse - keyboard shortcuts",
    "",
    "Navigation",
    "  j/k, Up/Down    move cursor",
    "  PgUp/PgDn      page up/down",
    "  Enter, l, ->    expand/collapse folder",
    "",
    "Selection",
    "  Space          toggle file or folder",
    "  a              select/deselect all (visible when filter on)",
    "",
    "Filter",
    "  f              open filter screen (preview, then apply)",
    "",
    "Actions",
    "  g              confirm download",
    "  ?              this help",
    "  q, Esc         quit",
]

FILTER_HELP_LINES = [
    "Filter - keyboard shortcuts",
    "",
    "  Type           edit pattern (live preview below)",
    "  F              cycle type: contains -> glob -> regex",
    "  i              toggle include / exclude",
    "  Enter          apply filter and update selection",
    "  Esc, b         cancel (keep current filter/selection)",
    "",
    "Preview",
    "  j/k, Up/Down    move in preview tree",
    "  l, ->          expand/collapse folder",
    "  PgUp/PgDn      page preview",
    "",
    "  ?              this help",
]

CONFIRM_HELP_LINES = [
    "Confirm download - keyboard shortcuts",
    "",
    "  y              download selection",
    "  c              copy download-files command",
    "  p              toggle preserve-dirs",
    "  s              strip prefix on/off",
    "  S              cycle strip level (1/2/3)",
    "  e              edit strip prefix (Enter apply, Esc cancel)",
    "",
    "  Up/Down, j/k   scroll file list",
    "  Left/Right, h/l move between action buttons",
    "  Enter          activate highlighted action",
    "",
    "  Esc, b         back to browse",
    "  q              quit",
    "  ?              this help",
]


def browse_help_lines() -> list[str]:
    return list(BROWSE_HELP_LINES)


def confirm_help_lines() -> list[str]:
    return list(CONFIRM_HELP_LINES)


def filter_help_lines() -> list[str]:
    return list(FILTER_HELP_LINES)
