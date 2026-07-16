"""Confirm mode for the interactive file browser."""

from __future__ import annotations

from dataclasses import dataclass

import curses

from dp_tools.glds_api.browser.commands import (
    copy_download_command,
    format_download_command,
)
from dp_tools.glds_api.browser.models import BrowserAction, BrowserResult
from dp_tools.glds_api.browser.preview import (
    clamp_list_scroll,
    visible_file_range_from_preview_rows,
    visible_filename_range,
)
from dp_tools.glds_api.browser.session import BrowserSession
from dp_tools.glds_api.browser.tui_draw import (
    Colors,
    draw_confirm_footer,
    draw_confirm_header,
    draw_preview_lines,
    draw_strip_edit_line,
)
from dp_tools.glds_api.browser.tui_keys import apply_text_edit_key


@dataclass
class ConfirmView:
    preview_rows: int
    preview_top: int
    status_row: int
    action_row: int
    list_scrollable: bool
    confirm_line_count: int
    active_strip_prefix: str | None


def draw_confirm(
    session: BrowserSession,
    stdscr: curses.window,
    height: int,
    width: int,
    colors: Colors,
) -> ConfirmView:
    preview_top = 4
    action_row = height - 2
    status_row = action_row - 1
    list_scrollable = False
    confirm_line_count = 0

    if session.strip_editing:
        active_strip_prefix = session.strip_edit_buffer or None
    elif session.strip_enabled:
        active_strip_prefix = session.strip_prefixes.get(session.strip_level)
    else:
        active_strip_prefix = None

    from dp_tools.glds_api import commons

    strip_affected = (
        commons.count_strip_affected(session.selected_filenames(), active_strip_prefix)
        if session.strip_enabled or session.strip_editing
        else 0
    )
    if session.strip_editing:
        draw_strip_edit_line(
            stdscr,
            width,
            session.strip_edit_buffer,
            session.strip_edit_cursor,
            colors,
            strip_affected=strip_affected,
            strip_total=len(session.checked),
        )
        curses.curs_set(1)
    else:
        curses.curs_set(0)
        draw_confirm_header(
            stdscr,
            width,
            len(session.checked),
            session.selected_bytes(),
            session.preserve_dirs,
            session.strip_enabled,
            active_strip_prefix,
            strip_affected,
            len(session.checked),
            colors,
        )

    stdscr.hline(2, 0, "-", min(width - 1, 60))
    preview_rows = max(1, status_row - preview_top)
    if not session.checked:
        stdscr.addstr(preview_top, 0, "Nothing selected.", colors.warn)
    else:
        session.ensure_confirm_preview(width, active_strip_prefix)
        if session.preserve_dirs:
            preview_lines = session.confirm_preview_lines
            confirm_line_count = len(preview_lines)
            list_scrollable = confirm_line_count > preview_rows
            session.confirm_scroll = draw_preview_lines(
                stdscr,
                preview_lines,
                top_row=preview_top,
                max_rows=preview_rows,
                width=width,
                scroll=session.confirm_scroll,
                attr=colors.file,
            )
        else:
            preview_lines = session.confirm_flat_lines
            confirm_line_count = len(preview_lines)
            list_scrollable = confirm_line_count > preview_rows
            session.confirm_scroll = draw_preview_lines(
                stdscr,
                preview_lines,
                top_row=preview_top,
                max_rows=preview_rows,
                width=width,
                scroll=session.confirm_scroll,
                attr=colors.file,
            )

    if session.confirm_status:
        status_line = session.confirm_status
    elif list_scrollable:
        if session.preserve_dirs:
            first, last = visible_file_range_from_preview_rows(
                session.confirm_path_rows,
                session.confirm_scroll,
                preview_rows,
                len(session.confirm_names),
            )
        else:
            first, last = visible_filename_range(
                session.confirm_display_names,
                width,
                session.confirm_scroll,
                preview_rows,
            )
        status_line = f"↑↓ scroll  {first}-{last} of {len(session.confirm_names)}"
    else:
        status_line = ""
    if status_line:
        stdscr.addnstr(status_row, 0, status_line, width - 1, colors.status)
    draw_confirm_footer(
        stdscr,
        action_row,
        width,
        session.confirm_idx,
        session.confirm_labels,
        colors,
    )
    return ConfirmView(
        preview_rows=preview_rows,
        preview_top=preview_top,
        status_row=status_row,
        action_row=action_row,
        list_scrollable=list_scrollable,
        confirm_line_count=confirm_line_count,
        active_strip_prefix=active_strip_prefix,
    )


def _download_result(session: BrowserSession) -> BrowserResult:
    file_ids = sorted(session.checked)
    return BrowserResult(
        BrowserAction.DOWNLOAD,
        session.selected_filenames(),
        selected_file_ids=file_ids,
        preserve_dirs=session.preserve_dirs,
        strip_prefix=session.current_strip_prefix(),
        file_table=session.df,
    )


def _flash_copy_command(session: BrowserSession) -> None:
    command = format_download_command(
        session.osd_id,
        session.selected_filenames(),
        df=session.df,
        file_ids=sorted(session.checked),
        maps=session.maps,
        output_dir=session.output_dir,
        jobs=session.jobs,
        preserve_dirs=session.preserve_dirs,
        strip_prefix=session.current_strip_prefix(),
    )
    if copy_download_command(command):
        session.flash_confirm_status("Command copied to clipboard.")
    else:
        session.flash_confirm_status(
            "Clipboard unavailable. Install wl-copy or xclip."
        )


def handle_confirm_key(
    session: BrowserSession,
    key: int,
    view: ConfirmView,
    stdscr: curses.window,
) -> BrowserResult | None:
    if key == ord("?"):
        session.show_help = True
        session.help_scroll = 0
        return None

    def try_scroll(scroll_key: int) -> bool:
        if not view.list_scrollable:
            return False
        if scroll_key in (curses.KEY_UP, ord("k")):
            session.confirm_scroll = clamp_list_scroll(
                session.confirm_scroll - 1,
                view.confirm_line_count,
                view.preview_rows,
            )
        elif scroll_key in (curses.KEY_DOWN, ord("j")):
            session.confirm_scroll = clamp_list_scroll(
                session.confirm_scroll + 1,
                view.confirm_line_count,
                view.preview_rows,
            )
        elif scroll_key in (curses.KEY_PPAGE,):
            session.confirm_scroll = clamp_list_scroll(
                session.confirm_scroll - max(1, view.preview_rows - 1),
                view.confirm_line_count,
                view.preview_rows,
            )
        elif scroll_key in (curses.KEY_NPAGE,):
            session.confirm_scroll = clamp_list_scroll(
                session.confirm_scroll + max(1, view.preview_rows - 1),
                view.confirm_line_count,
                view.preview_rows,
            )
        else:
            return False
        session.confirm_status = ""
        return True

    if session.strip_editing:
        if try_scroll(key):
            return None
        buffer, cur, action = apply_text_edit_key(
            key, session.strip_edit_buffer, session.strip_edit_cursor
        )
        session.strip_edit_buffer = buffer
        session.strip_edit_cursor = cur
        if action == "done":
            session.strip_prefixes[session.strip_level] = (
                session.strip_edit_buffer or None
            )
            session.strip_custom_levels.add(session.strip_level)
            session.strip_enabled = True
            session.strip_editing = False
            session.confirm_status = ""
            session.invalidate_confirm_preview()
        elif action == "cancel":
            session.strip_editing = False
            session.confirm_status = ""
            session.invalidate_confirm_preview()
        else:
            session.invalidate_confirm_preview()
        return None

    if key in (27, ord("b")):
        session.mode = "browse"
        session.confirm_status = ""
        session.strip_editing = False
        session.invalidate_confirm_preview()
        return None
    if try_scroll(key):
        return None
    if key in (ord("q"),):
        return BrowserResult(BrowserAction.EXIT)
    if key in (ord("p"), ord("P")):
        session.preserve_dirs = not session.preserve_dirs
        session.confirm_scroll = 0
        session.confirm_status = ""
        session.invalidate_confirm_preview()
        return None
    if key == ord("s"):
        session.strip_editing = False
        session.strip_enabled = not session.strip_enabled
        selected = session.selected_filenames()
        if session.strip_enabled:
            session.fill_all_strip_levels(selected)
            prefix = session.strip_prefixes.get(session.strip_level)
            label = repr(prefix) if prefix else "(none)"
            if len(label) > 36:
                label = label[:33] + "..."
            session.flash_confirm_status(f"Strip on, S cycle {session.strip_level}: {label}")
        else:
            session.confirm_status = ""
        session.confirm_scroll = 0
        session.invalidate_confirm_preview()
        return None
    if key == ord("S"):
        if session.strip_enabled:
            session.strip_editing = False
            session.strip_level = 1 if session.strip_level >= 3 else session.strip_level + 1
            session.ensure_strip_level(session.selected_filenames(), session.strip_level)
            prefix = session.strip_prefixes.get(session.strip_level)
            label = repr(prefix) if prefix else "(none)"
            if len(label) > 36:
                label = label[:33] + "..."
            session.flash_confirm_status(f"S cycle {session.strip_level}: {label}")
            session.confirm_scroll = 0
            session.invalidate_confirm_preview()
        return None
    if key == ord("e"):
        session.strip_enabled = True
        session.ensure_strip_level(session.selected_filenames(), session.strip_level)
        session.strip_edit_buffer = session.strip_prefixes.get(session.strip_level) or ""
        session.strip_edit_cursor = len(session.strip_edit_buffer)
        session.strip_editing = True
        session.confirm_status = ""
        session.invalidate_confirm_preview()
        return None
    if key in (ord("y"),):
        collision_msg = session.strip_collision_message()
        if collision_msg:
            session.flash_confirm_status(collision_msg)
            return None
        return _download_result(session)
    if key in (ord("c"),):
        _flash_copy_command(session)
        return None
    if key in (curses.KEY_LEFT, ord("h")):
        session.confirm_idx = (session.confirm_idx - 1) % len(session.confirm_labels)
        session.confirm_status = ""
        return None
    if key in (curses.KEY_RIGHT, ord("l")):
        session.confirm_idx = (session.confirm_idx + 1) % len(session.confirm_labels)
        session.confirm_status = ""
        return None
    if key in (10,):
        if session.confirm_idx == 0:
            return BrowserResult(BrowserAction.EXIT)
        if session.confirm_idx == 1:
            collision_msg = session.strip_collision_message()
            if collision_msg:
                session.flash_confirm_status(collision_msg)
                return None
            return _download_result(session)
        _flash_copy_command(session)
    return None
