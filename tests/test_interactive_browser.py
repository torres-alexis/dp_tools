from unittest.mock import MagicMock, patch

import pandas as pd

from dp_tools.glds_api.browser.commands import (
    copy_download_command,
    executable_download_command,
    flags_for_selection,
    format_download_command,
)
from dp_tools.glds_api.browser.files import (
    build_file_maps,
    build_download_plan_from_ids,
    find_selection_path_collisions,
)
from dp_tools.glds_api.browser.filter import (
    count_filter_matches as _count_filter_matches,
    file_matches_filter as _file_matches_filter,
    filter_pattern_valid as _filter_pattern_valid,
    prune_checked_for_filter as _prune_checked_for_filter,
)
from dp_tools.glds_api.browser.help_text import (
    browse_help_lines as _browse_help_lines,
    confirm_help_lines as _confirm_help_lines,
    filter_help_lines as _filter_help_lines,
)
from dp_tools.glds_api.browser.preview import (
    build_path_preview_rows_from_ids,
    path_preview_lines,
)
from dp_tools.glds_api.browser.session import BrowserSession
from dp_tools.glds_api.browser.tui_keys import apply_text_edit_key as _apply_strip_edit_key
from dp_tools.glds_api.browser.tui_keys import ignore_key as _ignore_key


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "file_name": ["a.fq", "b.fq", "c.fq", "meta.txt"],
            "category": ["RNA-Seq", "RNA-Seq", "RNA-Seq", "Study Metadata Files"],
            "subcategory": [
                "Raw sequence data",
                "Raw sequence data",
                "Processed counts",
                "",
            ],
        }
    )


@patch("dp_tools.glds_api.browser.commands.copy_to_clipboard")
def test_copy_download_command_strips_warning_comment(mock_copy):
    command = "# warning\n dpt osd download-files OSD-240 -y"
    mock_copy.return_value = True
    assert copy_download_command(command) is True
    mock_copy.assert_called_once_with("dpt osd download-files OSD-240 -y")


def test_executable_download_command():
    raw = "# note\n dpt osd download-files OSD-1 -y"
    assert executable_download_command(raw) == "dpt osd download-files OSD-1 -y"
    assert executable_download_command("dpt osd download-files OSD-1 -y") == (
        "dpt osd download-files OSD-1 -y"
    )


def test_path_preview_rows_from_ids_duplicate_basenames():
    df = pd.DataFrame(
        {
            "file_name": ["counts.csv", "counts.csv"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["STAR", "Processed counts"],
            "subdirectory": ["", ""],
        }
    )
    maps = build_file_maps(df)
    rows = build_path_preview_rows_from_ids(maps.ordered_ids, maps)
    text = "\n".join(row.text for row in rows)
    assert text.count("counts.csv") == 2
    assert "STAR" in text
    assert "Processed counts" in text


@patch("dp_tools.glds_api.browser.commands.commons.get_table_of_files")
def test_format_download_command_uses_passed_df(mock_table):
    df = _sample_df()
    format_download_command("OSD-240", ["a.fq"], df=df)
    mock_table.assert_not_called()


def test_flags_for_selection_exact_subcategory():
    df = _sample_df()
    flags = flags_for_selection(df, {"a.fq", "b.fq"})
    assert flags == {
        "categories": ["RNA-Seq"],
        "subcategories": ["Raw sequence data"],
    }


def test_flags_for_selection_mixed_uses_file_flags():
    df = _sample_df()
    assert flags_for_selection(df, {"a.fq", "meta.txt"}) is None


@patch("dp_tools.glds_api.browser.commands.commons.get_table_of_files")
def test_format_download_command_subcategory(mock_table):
    mock_table.return_value = _sample_df()
    cmd = format_download_command("OSD-240", ["a.fq", "b.fq"])
    assert "-c RNA-Seq" in cmd
    assert "--subcategory 'Raw sequence data'" in cmd
    assert "--file" not in cmd


@patch("dp_tools.glds_api.browser.commands.commons.get_table_of_files")
def test_format_download_command_exact_files(mock_table):
    mock_table.return_value = _sample_df()
    cmd = format_download_command("OSD-240", ["a.fq", "meta.txt"])
    assert "--file a.fq" in cmd
    assert "--file meta.txt" in cmd


@patch("dp_tools.glds_api.browser.commands.commons.get_table_of_files")
def test_format_download_command_preserve_dirs_and_strip(mock_table):
    mock_table.return_value = _sample_df()
    cmd = format_download_command(
        "OSD-240",
        ["a.fq"],
        preserve_dirs=True,
        strip_prefix="GLDS-240_",
    )
    assert "--preserve-dirs" in cmd
    assert "--strip-prefix GLDS-240_" in cmd


def test_path_preview_lines_nested():
    df = pd.DataFrame(
        {
            "file_name": ["a.fq", "b.fq"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["Raw sequence data", "Raw sequence data"],
            "subdirectory": ["FastQC Reports", "FastQC Reports"],
        }
    )
    lines = path_preview_lines(["a.fq", "b.fq"], df)
    assert lines == [
        "RNA-Seq/",
        "  Raw sequence data/",
        "    FastQC Reports/",
        "      a.fq",
        "      b.fq",
    ]


def test_ignore_accidental_ctrl_keys():
    assert _ignore_key(14) is True  # Ctrl+N
    assert _ignore_key(12) is True  # Ctrl+L
    assert _ignore_key(ord("q")) is False
    assert _ignore_key(10) is False
    assert _ignore_key(27) is False  # Esc must work


def test_apply_strip_edit_key_insert_and_backspace():
    buf, cur, action = _apply_strip_edit_key(ord("G"), "", 0)
    assert action is None
    assert buf == "G"
    buf, cur, action = _apply_strip_edit_key(ord("L"), buf, cur)
    buf, cur, action = _apply_strip_edit_key(8, buf, cur)
    assert buf == "G"
    assert cur == 1


def test_file_matches_filter_contains_glob_regex():
    assert _file_matches_filter("GLDS-48_sample.fq", "sample", 1)
    assert not _file_matches_filter("GLDS-48_sample.fq", "bam", 1)
    assert _file_matches_filter("a.fq.gz", "*.fq.gz", 2)
    assert not _file_matches_filter("a.fq", "*.fq.gz", 2)
    assert _file_matches_filter("sample_R1.fq", r"R[12]", 3)
    assert not _file_matches_filter("sample_R1.fq", r"[", 3)


def test_file_matches_filter_invert():
    assert not _file_matches_filter("a.fq", "fq", 1, invert=True)
    assert _file_matches_filter("meta.txt", "fq", 1, invert=True)
    assert not _file_matches_filter("a.fq.gz", "*.fq.gz", 2, invert=True)
    assert _file_matches_filter("a.fq", "*.fq.gz", 2, invert=True)


def test_filter_pattern_valid_regex():
    assert _filter_pattern_valid("abc", 1)
    assert _filter_pattern_valid(r"R[12]", 3)
    assert not _filter_pattern_valid(r"[", 3)


def test_prune_checked_for_filter():
    checked = {"a.fq", "b.fq", "meta.txt"}
    removed = _prune_checked_for_filter(
        checked, "fq", 1, names_by_id={k: k for k in checked}
    )
    assert removed == 1
    assert checked == {"a.fq", "b.fq"}
    assert _prune_checked_for_filter(checked, "", 1) == 0


def test_prune_checked_for_filter_by_file_id():
    names_by_id = {
        "RNA-Seq/Raw/a.fq": "a.fq",
        "RNA-Seq/Raw/b.fq": "b.fq",
        "Study Metadata Files/meta.txt": "meta.txt",
    }
    checked = set(names_by_id)
    removed = _prune_checked_for_filter(
        checked, "fq", 1, names_by_id=names_by_id
    )
    assert removed == 1
    assert checked == {"RNA-Seq/Raw/a.fq", "RNA-Seq/Raw/b.fq"}


def test_prune_checked_for_filter_invert():
    checked = {"a.fq", "b.fq", "meta.txt"}
    removed = _prune_checked_for_filter(
        checked, "fq", 1, invert=True, names_by_id={k: k for k in checked}
    )
    assert removed == 2
    assert checked == {"meta.txt"}


def test_help_lines_cover_main_keys():
    browse = _browse_help_lines()
    confirm = _confirm_help_lines()
    filter_help = _filter_help_lines()
    browse_text = "\n".join(browse).lower()
    confirm_text = "\n".join(confirm).lower()
    filter_text = "\n".join(filter_help).lower()
    assert "filter" in browse_text
    assert "open filter" in browse_text
    assert "g " in browse_text or "  g" in browse_text
    assert "strip" in confirm_text
    assert "preserve-dirs" in confirm_text
    assert "copy" in confirm_text
    assert "include" in filter_text or "exclude" in filter_text
    assert "apply" in filter_text


def test_filter_type_label_invert():
    from dp_tools.glds_api.browser.filter import filter_type_label as _filter_type_label

    assert _filter_type_label(1) == "contains"
    assert _filter_type_label(1, invert=True) == "excludes"
    assert _filter_type_label(2, invert=True) == "not glob"
    assert _filter_type_label(3, invert=True) == "not regex"


def test_count_filter_matches():
    names = ["a.fq", "b.fq", "meta.txt"]
    assert _count_filter_matches(names, "fq", 1) == (2, 3)
    assert _count_filter_matches(names, "fq", 1, invert=True) == (1, 3)
    assert _count_filter_matches(names, "", 1, active=False) == (3, 3)


def test_flags_for_selection_empty_subcategory():
    df = pd.DataFrame(
        {
            "file_name": ["meta.txt", "readme.txt"],
            "category": ["Study Metadata Files", "Study Metadata Files"],
            "subcategory": ["", ""],
        }
    )
    flags = flags_for_selection(df, {"meta.txt", "readme.txt"})
    assert flags == {
        "categories": ["Study Metadata Files"],
        "subcategories": [],
    }


def test_download_plan_from_ids_preserves_paths():
    df = pd.DataFrame(
        {
            "file_name": ["counts.csv", "counts.csv"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["STAR", "Processed counts"],
            "subdirectory": ["", ""],
        }
    )
    maps = build_file_maps(df)
    file_ids = maps.ordered_ids
    plan = build_download_plan_from_ids(
        file_ids, maps, preserve_dirs=True, strip_prefix=None
    )
    assert len(plan) == 2
    assert plan[0][0] != plan[1][0]
    assert find_selection_path_collisions(file_ids, maps, preserve_dirs=True) == {}


def test_download_plan_flat_mode_detects_basename_collision():
    df = pd.DataFrame(
        {
            "file_name": ["counts.csv", "counts.csv"],
            "category": ["RNA-Seq", "RNA-Seq"],
            "subcategory": ["STAR", "Processed counts"],
            "subdirectory": ["", ""],
        }
    )
    maps = build_file_maps(df)
    collisions = find_selection_path_collisions(
        maps.ordered_ids, maps, preserve_dirs=False
    )
    assert collisions == {"counts.csv": ["counts.csv", "counts.csv"]}


def test_enter_confirm_preserves_strip_on_same_selection():
    session = BrowserSession.create("OSD-240", _sample_df())
    session.checked = {session.maps.ordered_ids[0]}
    session.enter_confirm_mode()
    session.strip_enabled = True
    session.strip_level = 1
    session.strip_prefixes[1] = "GLDS-240_"
    session.mode = "browse"
    session.enter_confirm_mode()
    assert session.strip_enabled is True
    assert session.strip_prefixes[1] == "GLDS-240_"


def test_esc_back_from_confirm_preserves_strip():
    from dp_tools.glds_api.browser.modes import confirm as confirm_mode

    session = BrowserSession.create("OSD-240", _sample_df())
    session.checked = {session.maps.ordered_ids[0]}
    session.enter_confirm_mode()
    session.strip_enabled = True
    session.strip_level = 1
    session.strip_prefixes[1] = "GLDS-240_"
    view = confirm_mode.ConfirmView(
        preview_rows=1,
        preview_top=4,
        status_row=0,
        action_row=0,
        list_scrollable=False,
        confirm_line_count=0,
        active_strip_prefix="GLDS-240_",
    )
    confirm_mode.handle_confirm_key(session, 27, view, MagicMock())
    assert session.mode == "browse"
    assert session.strip_enabled is True
    assert session.strip_prefixes[1] == "GLDS-240_"
    session.enter_confirm_mode()
    assert session.strip_enabled is True
    assert session.strip_prefixes[1] == "GLDS-240_"


def test_enter_confirm_resets_strip_on_new_selection():
    session = BrowserSession.create("OSD-240", _sample_df())
    session.checked = {session.maps.ordered_ids[0]}
    session.strip_enabled = True
    session.strip_prefixes[2] = "GLDS-240_"
    session.enter_confirm_mode()
    session.mode = "browse"
    session.checked.add(session.maps.ordered_ids[1])
    session.enter_confirm_mode()
    assert session.strip_enabled is False
    assert session.strip_prefixes == {}
