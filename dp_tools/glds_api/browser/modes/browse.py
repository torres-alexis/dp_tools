"""Browse mode for the interactive file browser."""

from __future__ import annotations

from dataclasses import dataclass

import curses

from dp_tools.glds_api.browser.models import BrowserAction, BrowserResult, TreeNode
from dp_tools.glds_api.browser.session import BrowserSession
from dp_tools.glds_api.browser.tree import (
    flatten_cached,
    node_files_filtered,
    toggle_node,
    visible_file_ids,
)
from dp_tools.glds_api.browser.tui_draw import (
    Colors,
    draw_browse_filter_line,
    draw_header,
    draw_tree_row,
)


@dataclass
class BrowseView:
    rows: list[tuple[TreeNode, str]]
    view_h: int
    pattern: str
    filter_active: bool


def draw_browse(
    session: BrowserSession,
    stdscr: curses.window,
    height: int,
    width: int,
    colors: Colors,
) -> BrowseView:
    pattern = session.current_filter_pattern()
    filter_active = session.filter_is_active()
    flatten_key = (
        session.tree_epoch,
        pattern,
        session.filter_level,
        filter_active,
        session.filter_inverted,
        frozenset(session.checked),
    )
    rows = flatten_cached(
        session.browse_flatten_cache,
        flatten_key,
        session.tree,
        session.checked,
        pattern=pattern,
        filter_level=session.filter_level,
        filter_active=filter_active,
        filter_invert=session.filter_inverted,
    )

    filter_row = 1
    tree_top = filter_row + 2
    view_h = max(1, height - tree_top - 2)

    if rows:
        session.cursor = max(0, min(session.cursor, len(rows) - 1))
    else:
        session.cursor = 0
    if session.cursor < session.scroll:
        session.scroll = session.cursor
    if session.cursor >= session.scroll + view_h:
        session.scroll = session.cursor - (view_h - 1)

    draw_header(
        stdscr,
        width,
        session.osd_id,
        len(session.checked),
        colors,
        session.selected_bytes(),
    )
    match_count, total_count = session.filter_match_counts()
    curses.curs_set(0)
    draw_browse_filter_line(
        stdscr,
        width,
        session.filter_enabled,
        session.filter_level,
        pattern if session.filter_enabled else None,
        match_count,
        total_count,
        colors,
        filter_invert=session.filter_inverted,
        row=filter_row,
    )
    stdscr.hline(filter_row + 1, 0, "-", min(width - 1, 60))
    if rows:
        for i in range(view_h):
            row_idx = session.scroll + i
            if row_idx >= len(rows):
                break
            node, line = rows[row_idx]
            draw_tree_row(
                stdscr,
                tree_top + i,
                width,
                node,
                row_idx == session.cursor,
                colors,
                display_line=line,
            )
    elif filter_active:
        stdscr.addnstr(tree_top, 0, "No matches.", width - 1, colors.warn)

    from dp_tools.glds_api.browser.help_text import DEFAULT_STATUS

    status_attr = colors.warn if session.status != DEFAULT_STATUS else colors.status
    stdscr.addnstr(height - 2, 0, session.status, width - 1, status_attr)
    return BrowseView(
        rows=rows,
        view_h=view_h,
        pattern=pattern,
        filter_active=filter_active,
    )


def handle_browse_key(
    session: BrowserSession,
    key: int,
    view: BrowseView,
    stdscr: curses.window,
) -> BrowserResult | None:
    from dp_tools.glds_api.browser.help_text import DEFAULT_STATUS

    if key == ord("?"):
        session.show_help = True
        session.help_scroll = 0
        return None
    if key in (ord("q"), 27):
        return BrowserResult(BrowserAction.EXIT)
    if key in (ord("g"), ord("G")):
        if not session.checked:
            session.flash_status("Nothing selected. Space to check files, q to quit.")
            return None
        session.enter_confirm_mode()
        return None
    if key == ord("f"):
        session.enter_filter_mode()
        return None
    if session.status != DEFAULT_STATUS:
        session.status = DEFAULT_STATUS
    if key in (curses.KEY_UP, ord("k")):
        session.cursor -= 1
    elif key in (curses.KEY_DOWN, ord("j")):
        session.cursor += 1
    elif key in (curses.KEY_PPAGE,):
        session.cursor -= max(1, view.view_h - 1)
    elif key in (curses.KEY_NPAGE,):
        session.cursor += max(1, view.view_h - 1)
    elif key in (10, curses.KEY_RIGHT, ord("l")):
        if not view.rows:
            return None
        node, _ = view.rows[session.cursor]
        if node.kind != "file":
            node.expanded = not node.expanded
            session.tree_epoch += 1
    elif key == ord(" "):
        if not view.rows:
            return None
        node, _ = view.rows[session.cursor]
        visible_files = node_files_filtered(
            node,
            view.pattern,
            session.filter_level,
            view.filter_active,
            invert=session.filter_inverted,
        )
        toggle_node(
            node,
            session.checked,
            visible_files=visible_files if view.filter_active else None,
        )
        session.invalidate_confirm_preview()
    elif key in (ord("a"), ord("A")):
        if view.filter_active:
            visible = visible_file_ids(
                session.tree,
                view.pattern,
                session.filter_level,
                True,
                invert=session.filter_inverted,
            )
            if visible and all(file_id in session.checked for file_id in visible):
                for file_id in visible:
                    session.checked.discard(file_id)
            else:
                session.checked.update(visible)
        else:
            if len(session.checked) == len(session.maps.ordered_ids):
                session.checked.clear()
            else:
                session.checked.update(session.maps.ordered_ids)
        session.invalidate_confirm_preview()
    return None
