"""Terminal file browser for OSDR study files (category tree + checkboxes)."""

from __future__ import annotations

import curses
import os
import sys

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.commands import format_download_command
from dp_tools.glds_api.browser.loop import run_browser_loop
from dp_tools.glds_api.browser.models import BrowserAction, BrowserResult
from dp_tools.glds_api.browser.session import BrowserSession

__all__ = [
    "BrowserAction",
    "BrowserResult",
    "format_download_command",
    "run_file_browser",
]


def run_file_browser(
    osd_id: str,
    df: pd.DataFrame | None = None,
    *,
    preserve_dirs: bool = False,
    output_dir: str = ".",
    jobs: int = 10,
) -> BrowserResult:
    """Run the interactive OSDR file browser. Requires a TTY."""
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise RuntimeError("Interactive browser requires a terminal (TTY).")

    if df is None:
        df = commons.get_table_of_files(osd_id)

    os.environ.setdefault("ESCDELAY", "25")

    session = BrowserSession.create(
        osd_id,
        df,
        preserve_dirs=preserve_dirs,
        output_dir=output_dir,
        jobs=jobs,
    )
    return curses.wrapper(lambda stdscr: run_browser_loop(stdscr, session))
