"""Keyboard input helpers for the interactive browser TUI."""

from __future__ import annotations

import curses


def read_key(stdscr: curses.window) -> int:
    """Read a key; resolve ESC-prefixed sequences, ignore bare CSI garbage."""
    ch = stdscr.getch()
    if ch != 27:
        return ch

    stdscr.nodelay(True)
    try:
        follow = stdscr.getch()
        if follow == -1:
            return 27
        if follow == ord("["):
            code = stdscr.getch()
            if code == ord("A"):
                return curses.KEY_UP
            if code == ord("B"):
                return curses.KEY_DOWN
            if code == ord("C"):
                return curses.KEY_RIGHT
            if code == ord("D"):
                return curses.KEY_LEFT
            if code == ord("5"):
                next_ch = stdscr.getch()
                if next_ch == ord("~"):
                    return curses.KEY_PPAGE
            if code == ord("6"):
                next_ch = stdscr.getch()
                if next_ch == ord("~"):
                    return curses.KEY_NPAGE
            while True:
                extra = stdscr.getch()
                if extra == -1:
                    break
                if extra in range(64, 91) or extra in range(97, 123) or extra == ord("~"):
                    break
            return -1
        if follow == ord("O"):
            code = stdscr.getch()
            if code == ord("A"):
                return curses.KEY_UP
            if code == ord("B"):
                return curses.KEY_DOWN
            if code == ord("C"):
                return curses.KEY_RIGHT
            if code == ord("D"):
                return curses.KEY_LEFT
            return -1
        return -1
    finally:
        stdscr.nodelay(False)


def ignore_key(key: int) -> bool:
    """Drop accidental Ctrl/IDE shortcuts (e.g. Ctrl+N = 14)."""
    if key == -1:
        return True
    if key == curses.KEY_RESIZE:
        return True
    if key > 31:
        return False
    if key in (10, curses.KEY_ENTER, 27):
        return False
    return True


def apply_text_edit_key(
    key: int,
    buffer: str,
    cursor: int,
    *,
    max_len: int = 200,
) -> tuple[str, int, str | None]:
    """Edit a single-line text field. action is None, 'done', or 'cancel'."""
    if key in (10, curses.KEY_ENTER):
        return buffer, cursor, "done"
    if key == 27:
        return buffer, cursor, "cancel"
    if key in (curses.KEY_BACKSPACE, 127, 8):
        if cursor > 0:
            buffer = buffer[: cursor - 1] + buffer[cursor:]
            cursor -= 1
        return buffer, cursor, None
    if key == curses.KEY_DC:
        if cursor < len(buffer):
            buffer = buffer[:cursor] + buffer[cursor + 1 :]
        return buffer, cursor, None
    if key == curses.KEY_HOME:
        return buffer, 0, None
    if key == curses.KEY_END:
        return buffer, len(buffer), None
    if key == curses.KEY_LEFT:
        return buffer, max(0, cursor - 1), None
    if key == curses.KEY_RIGHT:
        return buffer, min(len(buffer), cursor + 1), None
    if 32 <= key <= 126 and len(buffer) < max_len:
        buffer = buffer[:cursor] + chr(key) + buffer[cursor:]
        cursor += 1
    return buffer, cursor, None
