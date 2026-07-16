"""Main event loop for the interactive file browser."""

from __future__ import annotations

import curses

from dp_tools.glds_api.browser.help_text import (
    browse_help_lines,
    confirm_help_lines,
    filter_help_lines,
)
from dp_tools.glds_api.browser.models import BrowserAction, BrowserResult
from dp_tools.glds_api.browser.modes import browse, confirm, filter
from dp_tools.glds_api.browser.session import BrowserSession
from dp_tools.glds_api.browser.tui_draw import draw_help_screen, init_colors
from dp_tools.glds_api.browser.tui_keys import ignore_key, read_key


def run_browser_loop(stdscr: curses.window, session: BrowserSession) -> BrowserResult:
    colors = init_colors()
    curses.curs_set(0)
    stdscr.keypad(True)
    try:
        curses.set_escdelay(25)
    except curses.error:
        pass

    browse_view = browse.BrowseView(rows=[], view_h=1, pattern="", filter_active=False)
    filter_view = filter.FilterView(draft_rows=[], view_h=1)
    confirm_view = confirm.ConfirmView(
        preview_rows=1,
        preview_top=4,
        status_row=0,
        action_row=0,
        list_scrollable=False,
        confirm_line_count=0,
        active_strip_prefix=None,
    )

    while True:
        session.tick_status()
        stdscr.timeout(200 if session.needs_status_poll() else -1)
        stdscr.erase()
        height, width = stdscr.getmaxyx()

        if height < 8 or width < 40:
            msg = "Terminal too small (need 40x8). Resize or press q."
            stdscr.addnstr(0, 0, msg, max(0, width - 1), curses.A_NORMAL)
            stdscr.refresh()
            key = read_key(stdscr)
            if ignore_key(key):
                continue
            if key in (ord("q"), 27):
                return BrowserResult(BrowserAction.EXIT)
            continue

        if session.show_help:
            if session.mode == "browse":
                help_lines = browse_help_lines()
            elif session.mode == "filter":
                help_lines = filter_help_lines()
            else:
                help_lines = confirm_help_lines()
            session.help_scroll = draw_help_screen(
                stdscr, height, width, help_lines, session.help_scroll, colors
            )
        elif session.mode == "browse":
            if len(session.df) == 0:
                stdscr.addstr(0, 0, f"{session.osd_id}: no files found. Press any key.")
                stdscr.refresh()
                stdscr.getch()
                return BrowserResult(BrowserAction.EXIT)
            browse_view = browse.draw_browse(session, stdscr, height, width, colors)
        elif session.mode == "filter":
            filter_view = filter.draw_filter(session, stdscr, height, width, colors)
        else:
            confirm_view = confirm.draw_confirm(session, stdscr, height, width, colors)

        stdscr.refresh()
        key = read_key(stdscr)
        if ignore_key(key):
            continue

        if session.show_help:
            if key in (curses.KEY_UP, ord("k")):
                session.help_scroll = max(0, session.help_scroll - 1)
            elif key in (curses.KEY_DOWN, ord("j")):
                session.help_scroll += 1
            elif key in (curses.KEY_PPAGE,):
                session.help_scroll = max(0, session.help_scroll - max(1, height - 4))
            elif key in (curses.KEY_NPAGE,):
                session.help_scroll += max(1, height - 4)
            else:
                session.show_help = False
            continue

        result: BrowserResult | None = None
        if session.mode == "browse":
            result = browse.handle_browse_key(session, key, browse_view, stdscr)
        elif session.mode == "filter":
            result = filter.handle_filter_key(session, key, filter_view)
        else:
            result = confirm.handle_confirm_key(session, key, confirm_view, stdscr)
        if result is not None:
            return result
