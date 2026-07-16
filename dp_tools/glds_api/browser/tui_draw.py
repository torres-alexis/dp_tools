"""Curses drawing helpers for the interactive browser."""

from __future__ import annotations

import curses
from dataclasses import dataclass

from dp_tools.glds_api.browser.filter import filter_mode_hint, filter_type_label
from dp_tools.glds_api.browser.preview import clamp_list_scroll, format_file_size
from dp_tools.glds_api.browser.tree import TreeNode


@dataclass
class Colors:
    enabled: bool = False
    header: int = curses.A_BOLD
    header_count: int = curses.A_BOLD
    category: int = curses.A_BOLD
    subcategory: int = curses.A_NORMAL
    file: int = curses.A_DIM
    check_on: int = curses.A_BOLD
    check_partial: int = curses.A_NORMAL
    check_off: int = curses.A_DIM
    cursor: int = curses.A_REVERSE
    status: int = curses.A_DIM
    warn: int = curses.A_BOLD
    confirm_title: int = curses.A_BOLD


def init_colors() -> Colors:
    colors = Colors(enabled=bool(curses.has_colors()))
    if not colors.enabled:
        return colors

    curses.start_color()
    try:
        curses.use_default_colors()
    except curses.error:
        pass

    curses.init_pair(1, curses.COLOR_CYAN, -1)
    curses.init_pair(2, curses.COLOR_GREEN, -1)
    curses.init_pair(3, curses.COLOR_BLUE, -1)
    curses.init_pair(4, curses.COLOR_WHITE, -1)
    curses.init_pair(5, curses.COLOR_GREEN, -1)
    curses.init_pair(6, curses.COLOR_YELLOW, -1)
    curses.init_pair(7, curses.COLOR_BLACK, curses.COLOR_CYAN)
    curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(9, curses.COLOR_YELLOW, -1)

    colors.header = curses.color_pair(1) | curses.A_BOLD
    colors.header_count = curses.color_pair(2) | curses.A_BOLD
    colors.category = curses.color_pair(1) | curses.A_BOLD
    colors.subcategory = curses.color_pair(3) | curses.A_NORMAL
    colors.file = curses.color_pair(4) | curses.A_DIM
    colors.check_on = curses.color_pair(5) | curses.A_BOLD
    colors.check_partial = curses.color_pair(6) | curses.A_BOLD
    colors.check_off = curses.color_pair(4) | curses.A_DIM
    colors.cursor = curses.color_pair(8) | curses.A_BOLD
    colors.status = curses.color_pair(4) | curses.A_DIM
    colors.warn = curses.color_pair(9) | curses.A_BOLD
    colors.confirm_title = curses.color_pair(1) | curses.A_BOLD
    return colors


def addnstr_clipped(
    stdscr: curses.window, y: int, x: int, text: str, width: int, attr: int
) -> None:
    remaining = max(0, width - x)
    if remaining <= 0:
        return
    stdscr.addnstr(y, x, text, remaining, attr)


def draw_preview_lines(
    stdscr: curses.window,
    lines: list[str],
    *,
    top_row: int,
    max_rows: int,
    width: int,
    scroll: int,
    attr: int,
) -> int:
    if max_rows < 1:
        return 0
    scroll = clamp_list_scroll(scroll, len(lines), max_rows)
    for i in range(max_rows):
        row_idx = scroll + i
        if row_idx >= len(lines):
            break
        stdscr.addnstr(top_row + i, 0, lines[row_idx], max(0, width - 1), attr)
    return scroll


def draw_help_screen(
    stdscr: curses.window,
    height: int,
    width: int,
    lines: list[str],
    scroll: int,
    colors: Colors,
) -> int:
    title = "Help (? or any key to close)"
    addnstr_clipped(stdscr, 0, 0, title, width, colors.header)
    stdscr.hline(1, 0, "-", min(max(0, width - 1), 60))
    body_rows = max(1, height - 3)
    scroll = draw_preview_lines(
        stdscr,
        lines,
        top_row=2,
        max_rows=body_rows,
        width=width,
        scroll=scroll,
        attr=colors.status,
    )
    footer = "Up/Down scroll  any key close"
    addnstr_clipped(stdscr, height - 1, 0, footer, width, colors.status)
    return scroll


def _node_attr(node: TreeNode, colors: Colors, on_cursor: bool) -> int:
    if on_cursor:
        return colors.cursor
    if node.kind == "category":
        return colors.category
    if node.kind == "subcategory":
        return colors.subcategory
    return colors.file


def draw_tree_row(
    stdscr: curses.window,
    y: int,
    width: int,
    node: TreeNode,
    is_cursor: bool,
    colors: Colors,
    *,
    display_line: str,
) -> None:
    attr = colors.cursor if is_cursor else _node_attr(node, colors, False)
    stdscr.addnstr(y, 0, display_line, max(0, width - 1), attr)


def draw_header(
    stdscr: curses.window,
    width: int,
    osd_id: str,
    selected: int,
    colors: Colors,
    selected_bytes: int = 0,
) -> None:
    count = f"{selected} selected"
    if selected_bytes > 0:
        count = f"{count}, {format_file_size(selected_bytes)}"
    title = f"{osd_id}  ("
    suffix = ")"
    addnstr_clipped(stdscr, 0, 0, title, width, colors.header)
    x = len(title)
    count_attr = colors.header_count if selected else colors.header
    addnstr_clipped(stdscr, 0, x, count, width, count_attr)
    x += len(count)
    addnstr_clipped(stdscr, 0, x, suffix, width, colors.header)


def draw_browse_filter_line(
    stdscr: curses.window,
    width: int,
    filter_enabled: bool,
    filter_level: int,
    pattern: str | None,
    match_count: int,
    total_count: int,
    colors: Colors,
    *,
    filter_invert: bool = False,
    row: int = 1,
) -> None:
    if filter_enabled:
        type_label = filter_type_label(filter_level, invert=filter_invert)
        pat_label = repr(pattern) if pattern else "(none)"
        if len(pat_label) > max(12, width // 8):
            pat_label = pat_label[: max(9, width // 8 - 3)] + "..."
        hint = f"filter {type_label}: {pat_label} {match_count}/{total_count}  (f edit)"
    else:
        hint = "filter off  (f edit)"
    addnstr_clipped(stdscr, row, 0, hint, width, colors.status)


def draw_filter_page_header(
    stdscr: curses.window,
    width: int,
    checked_count: int,
    match_count: int,
    total_count: int,
    colors: Colors,
) -> None:
    title = f"Filter preview: {match_count}/{total_count} matches"
    sel_hint = f"{checked_count} selected (updated on apply)"
    sel_x = max(0, width - len(sel_hint))
    addnstr_clipped(stdscr, 0, 0, title, max(0, sel_x - 1), colors.confirm_title)
    addnstr_clipped(stdscr, 0, sel_x, sel_hint, width, colors.status)


def draw_filter_edit_line(
    stdscr: curses.window,
    width: int,
    buffer: str,
    cursor: int,
    filter_level: int,
    match_count: int,
    total_count: int,
    colors: Colors,
    *,
    filter_invert: bool = False,
) -> None:
    type_label = filter_type_label(filter_level, invert=filter_invert)
    counter = f"{match_count}/{total_count}"
    counter_x = max(0, width - len(counter))
    prompt = f"Pattern ({type_label}, Enter apply, Esc cancel): "
    field_width = max(0, counter_x - len(prompt) - 1)
    addnstr_clipped(stdscr, 1, 0, prompt, width, colors.status)
    visible = buffer[:field_width]
    addnstr_clipped(stdscr, 1, len(prompt), visible, width, colors.file)
    cursor_x = len(prompt) + min(cursor, field_width)
    if cursor_x < counter_x:
        stdscr.chgat(1, cursor_x, 1, colors.cursor)
    addnstr_clipped(stdscr, 1, counter_x, counter, width, colors.status)
    controls = f"F type  i {filter_mode_hint(filter_invert)}"
    addnstr_clipped(stdscr, 2, 0, controls, width, colors.status)


def draw_filter_footer(
    stdscr: curses.window,
    y: int,
    width: int,
    colors: Colors,
) -> None:
    hint = "Enter apply  Esc cancel  Up/Down preview  PgUp/Dn page  ? help"
    addnstr_clipped(stdscr, y, 0, hint, width, colors.status)


def draw_strip_edit_line(
    stdscr: curses.window,
    width: int,
    buffer: str,
    cursor: int,
    colors: Colors,
    *,
    strip_affected: int,
    strip_total: int,
) -> None:
    counter = f"{strip_affected}/{strip_total}"
    counter_x = max(0, width - len(counter))
    prompt = "Edit strip prefix (Enter apply, Esc cancel): "
    field_width = max(0, counter_x - len(prompt) - 1)
    addnstr_clipped(stdscr, 1, 0, prompt, width, colors.status)
    visible = buffer[:field_width]
    addnstr_clipped(stdscr, 1, len(prompt), visible, width, colors.file)
    cursor_x = len(prompt) + min(cursor, field_width)
    if cursor_x < counter_x:
        stdscr.chgat(1, cursor_x, 1, colors.cursor)
    addnstr_clipped(stdscr, 1, counter_x, counter, width, colors.status)


def draw_confirm_header(
    stdscr: curses.window,
    width: int,
    checked_count: int,
    selected_bytes: int,
    preserve_dirs: bool,
    strip_enabled: bool,
    strip_prefix: str | None,
    strip_affected: int,
    strip_total: int,
    colors: Colors,
) -> None:
    title = f"Confirm: {checked_count} file(s) selected"
    if selected_bytes > 0:
        title = f"{title}, {format_file_size(selected_bytes)}"
    dirs_label = "on" if preserve_dirs else "off"
    dirs_hint = f"preserve dirs: {dirs_label} (p)"
    hint_len = len(dirs_hint)
    dirs_x = max(0, width - 1 - hint_len)
    title_width = max(0, dirs_x - 2)
    addnstr_clipped(stdscr, 0, 0, title, title_width, colors.confirm_title)
    addnstr_clipped(stdscr, 0, dirs_x, dirs_hint, width, colors.status)

    if strip_enabled:
        prefix_label = repr(strip_prefix) if strip_prefix else "(none)"
        if len(prefix_label) > max(20, width // 5):
            prefix_label = prefix_label[: max(17, width // 5 - 3)] + "..."
        counter = f"{strip_affected}/{strip_total}"
        strip_hint = (
            f"strip prefix: {prefix_label} {counter} (s off, e edit, S cycle)"
        )
    else:
        strip_hint = "strip prefix: off (s, e edit)"
    addnstr_clipped(stdscr, 1, 0, strip_hint, width, colors.status)


def draw_confirm_footer(
    stdscr: curses.window,
    y: int,
    width: int,
    confirm_idx: int,
    confirm_labels: list[tuple[str, str]],
    colors: Colors,
) -> None:
    sep = "  "
    x = 0
    for i, (hotkey, label) in enumerate(confirm_labels):
        option = f"{hotkey} {label}"
        attr = colors.cursor if i == confirm_idx else colors.status
        addnstr_clipped(stdscr, y, x, option, width, attr)
        x += len(option)
        if i < len(confirm_labels) - 1:
            addnstr_clipped(stdscr, y, x, sep, width, colors.status)
            x += len(sep)

    addnstr_clipped(stdscr, y, x, sep, width, colors.status)
    x += len(sep)
    suffix = "Enter  Esc back"
    addnstr_clipped(stdscr, y, x, suffix, width, colors.status)
