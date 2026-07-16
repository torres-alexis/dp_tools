"""Tests for browser tree building and file identity."""

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.files import build_file_maps
from dp_tools.glds_api.browser.tree import build_tree, node_files, toggle_node


def _row(name, *, category="RNA-Seq", subcategory="Raw sequence data", subdirectory="", size=100):
    return {
        "file_name": name,
        "file_size": size,
        "category": category,
        "subcategory": subcategory,
        "subdirectory": subdirectory,
    }


def test_build_tree_includes_subdirectory_level():
    df = pd.DataFrame(
        [
            _row("a.fq", subdirectory="FastQC Reports"),
            _row("b.fq", subdirectory="FastQC Reports"),
            _row("c.fq", subdirectory=""),
        ]
    )
    roots = build_tree(df)
    assert len(roots) == 1
    cat = roots[0]
    assert cat.kind == "category"
    sub = cat.children[0]
    assert sub.kind == "subcategory"
    dir_nodes = [c for c in sub.children if c.kind == "subdirectory"]
    assert len(dir_nodes) == 1
    assert dir_nodes[0].label == "FastQC Reports"
    assert len(dir_nodes[0].children) == 2
    loose_files = [c for c in sub.children if c.kind == "file"]
    assert len(loose_files) == 1
    assert loose_files[0].filename == "c.fq"


def test_file_id_is_relative_download_path():
    df = pd.DataFrame([_row("sample.fq", subdirectory="FastQC Reports")])
    roots = build_tree(df)
    file_node = roots[0].children[0].children[0].children[0]
    expected = commons.relative_download_path(df.iloc[0])
    assert file_node.file_id == expected
    assert file_node.file_id != file_node.filename


def test_toggle_node_uses_file_id_keys():
    df = pd.DataFrame([_row("a.fq"), _row("b.fq")])
    roots = build_tree(df)
    maps = build_file_maps(df)
    checked: set[str] = set()
    toggle_node(roots[0].children[0], checked)
    assert checked == set(maps.ordered_ids)
