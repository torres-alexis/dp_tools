"""Filter mode for the interactive file browser."""

from __future__ import annotations

from dataclasses import dataclass

import curses

from dp_tools.glds_api.browser.models import BrowserAction, BrowserResult, TreeNode
from dp_tools.glds_api.browser.session import BrowserSession
from dp_tools.glds_api.browser.tree import flatten_cached
from dp_tools.glds_api.browser.tui_draw import (
    Colors,
    draw_filter_edit_line,
    draw_filter_footer,
    draw_filter_page_header,
    draw_tree_row,
)
from dp_tools.glds_api.browser.tui_keys import apply_text_edit_key


@dataclass
class FilterView:
    draft_rows: list[tuple[TreeNode, str]]
    view_h: int


def draw_filter(
    session: BrowserSession,
    stdscr: curses.window,
    height: int,
    width: int,
    colors: Colors,
) -> FilterView:
    draft_active = session.draft_filter_active()
    draft_pattern = session.filter_draft_buffer
    flatten_key = (
        session.tree_epoch,
        draft_pattern,
        session.filter_draft_level,
        draft_active,
        session.filter_draft_invert,
        frozenset(session.checked),
    )
    draft_rows = flatten_cached(
        session.filter_flatten_cache,
        flatten_key,
        session.tree,
        session.checked,
        pattern=draft_pattern,
        filter_level=session.filter_draft_level,
        filter_active=draft_active,
        filter_invert=session.filter_draft_invert,
    )
    tree_top = 4
    view_h = max(1, height - 6)
    if draft_rows:
        session.filter_cursor = max(
            0, min(session.filter_cursor, len(draft_rows) - 1)
        )
    else:
        session.filter_cursor = 0
    if session.filter_cursor < session.filter_scroll:
        session.filter_scroll = session.filter_cursor
    if session.filter_cursor >= session.filter_scroll + view_h:
        session.filter_scroll = session.filter_cursor - (view_h - 1)

    match_count, total_count = session.draft_match_counts()
    draw_filter_page_header(
        stdscr,
        width,
        len(session.checked),
        match_count,
        total_count,
        colors,
    )
    curses.curs_set(1)
    draw_filter_edit_line(
        stdscr,
        width,
        session.filter_draft_buffer,
        session.filter_draft_cursor,
        session.filter_draft_level,
        match_count,
        total_count,
        colors,
        filter_invert=session.filter_draft_invert,
    )
    stdscr.hline(3, 0, "-", min(width - 1, 60))
    if draft_rows:
        for i in range(view_h):
            row_idx = session.filter_scroll + i
            if row_idx >= len(draft_rows):
                break
            node, line = draft_rows[row_idx]
            draw_tree_row(
                stdscr,
                tree_top + i,
                width,
                node,
                row_idx == session.filter_cursor,
                colors,
                display_line=line,
            )
    elif draft_active:
        stdscr.addnstr(tree_top, 0, "No matches.", width - 1, colors.warn)

    from dp_tools.glds_api.browser.help_text import DEFAULT_STATUS

    status_line = session.filter_status or session.status
    status_attr = (
        colors.warn if status_line not in ("", DEFAULT_STATUS) else colors.status
    )
    stdscr.addnstr(height - 2, 0, status_line, width - 1, status_attr)
    draw_filter_footer(stdscr, height - 1, width, colors)
    return FilterView(draft_rows=draft_rows, view_h=view_h)


def handle_filter_key(
    session: BrowserSession,
    key: int,
    view: FilterView,
) -> BrowserResult | None:
    if key == ord("?"):
        session.show_help = True
        session.help_scroll = 0
        return None
    if key in (ord("q"),):
        return BrowserResult(BrowserAction.EXIT)
    if key in (27, ord("b")):
        session.cancel_filter_mode()
        return None
    if key == ord("F"):
        session.filter_draft_patterns[session.filter_draft_level] = (
            session.filter_draft_buffer
        )
        session.filter_draft_level = session.filter_draft_level % 3 + 1
        session.filter_draft_buffer = session.filter_draft_patterns.get(
            session.filter_draft_level, ""
        )
        session.filter_draft_cursor = len(session.filter_draft_buffer)
        session.filter_cursor = 0
        session.filter_scroll = 0
        session.filter_status = ""
        return None
    if key == ord("i"):
        session.filter_draft_invert = not session.filter_draft_invert
        session.filter_cursor = 0
        session.filter_scroll = 0
        session.filter_status = ""
        return None
    if key in (curses.KEY_UP, ord("k")):
        if view.draft_rows:
            session.filter_cursor -= 1
        return None
    if key in (curses.KEY_DOWN, ord("j")):
        if view.draft_rows:
            session.filter_cursor += 1
        return None
    if key in (curses.KEY_PPAGE,):
        if view.draft_rows:
            session.filter_cursor -= max(1, view.view_h - 1)
        return None
    if key in (curses.KEY_NPAGE,):
        if view.draft_rows:
            session.filter_cursor += max(1, view.view_h - 1)
        return None
    if key in (curses.KEY_RIGHT, ord("l")):
        if view.draft_rows:
            node, _ = view.draft_rows[session.filter_cursor]
            if node.kind != "file":
                node.expanded = not node.expanded
                session.tree_epoch += 1
        return None

    buffer, cur, action = apply_text_edit_key(
        key, session.filter_draft_buffer, session.filter_draft_cursor
    )
    session.filter_draft_buffer = buffer
    session.filter_draft_cursor = cur
    if action == "done":
        error = session.commit_filter()
        if error:
            session.filter_status = error
        return None
    if action == "cancel":
        session.cancel_filter_mode()
        return None
    session.filter_cursor = 0
    session.filter_scroll = 0
    session.filter_status = ""
    return None
