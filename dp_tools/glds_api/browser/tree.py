"""Category tree construction and flattening for the interactive browser."""

from __future__ import annotations

import pandas as pd

from dp_tools.glds_api import commons
from dp_tools.glds_api.browser.filter import file_matches_filter
from dp_tools.glds_api.browser.models import FlattenCache, TreeNode
from dp_tools.glds_api.browser.preview import coerce_file_size, format_file_size


def node_size_label(node: TreeNode) -> str:
    if node.size_bytes <= 0:
        return ""
    return format_file_size(node.size_bytes)


def _folder_label(value: object) -> str:
    if pd.isna(value) or not str(value).strip():
        return ""
    return str(value).strip()


def _file_node(
    row: pd.Series,
    *,
    depth: int,
    category: str,
    subcategory: str,
) -> TreeNode:
    file_id = commons.relative_download_path(row)
    name = str(row["file_name"])
    return TreeNode(
        kind="file",
        label=name,
        depth=depth,
        filename=name,
        file_id=file_id,
        category=category,
        subcategory=subcategory,
        size_bytes=coerce_file_size(row.get("file_size")),
    )


def _add_files_with_subdirs(
    parent: TreeNode,
    df: pd.DataFrame,
    *,
    depth: int,
    category: str,
    subcategory: str,
) -> None:
    if "subdirectory" not in df.columns:
        for _, row in df.sort_values("file_name").iterrows():
            parent.children.append(
                _file_node(row, depth=depth, category=category, subcategory=subcategory)
            )
        return

    for subdir, dir_df in df.groupby("subdirectory", sort=True):
        label = _folder_label(subdir)
        if label:
            dir_node = TreeNode(
                kind="subdirectory",
                label=label,
                depth=depth,
                category=category,
                subcategory=subcategory,
                file_count=len(dir_df),
                expanded=False,
            )
            for _, row in dir_df.sort_values("file_name").iterrows():
                dir_node.children.append(
                    _file_node(
                        row,
                        depth=depth + 1,
                        category=category,
                        subcategory=subcategory,
                    )
                )
            dir_node.size_bytes = sum(c.size_bytes for c in dir_node.children)
            parent.children.append(dir_node)
        else:
            for _, row in dir_df.sort_values("file_name").iterrows():
                parent.children.append(
                    _file_node(
                        row, depth=depth, category=category, subcategory=subcategory
                    )
                )


def build_tree(df: pd.DataFrame) -> list[TreeNode]:
    roots: list[TreeNode] = []
    for category, cat_df in df.groupby("category", sort=True):
        cat_node = TreeNode(
            kind="category",
            label=str(category),
            depth=0,
            category=str(category),
            file_count=len(cat_df),
            expanded=True,
        )
        for subcategory, sub_df in cat_df.groupby("subcategory", sort=True):
            sub_label = _folder_label(subcategory)
            if not sub_label:
                _add_files_with_subdirs(
                    cat_node,
                    sub_df,
                    depth=1,
                    category=str(category),
                    subcategory="",
                )
                continue
            sub_node = TreeNode(
                kind="subcategory",
                label=sub_label,
                depth=1,
                category=str(category),
                subcategory=sub_label,
                file_count=len(sub_df),
                expanded=False,
            )
            _add_files_with_subdirs(
                sub_node,
                sub_df,
                depth=2,
                category=str(category),
                subcategory=sub_label,
            )
            sub_node.size_bytes = sum(c.size_bytes for c in sub_node.children)
            cat_node.children.append(sub_node)
        cat_node.size_bytes = sum(c.size_bytes for c in cat_node.children)
        roots.append(cat_node)
    return roots


def node_files(node: TreeNode) -> list[str]:
    if node.kind == "file":
        return [node.file_id] if node.file_id else []
    names: list[str] = []
    for child in node.children:
        names.extend(node_files(child))
    return names


def node_files_filtered(
    node: TreeNode,
    pattern: str,
    level: int,
    filter_active: bool,
    *,
    invert: bool = False,
) -> list[str]:
    if not filter_active or not pattern:
        return node_files(node)
    matched: list[str] = []
    for file_id, display in _descendant_file_entries(node):
        if file_matches_filter(display, pattern, level, invert=invert):
            matched.append(file_id)
    return matched


def _descendant_file_entries(node: TreeNode) -> list[tuple[str, str]]:
    if node.kind == "file" and node.file_id:
        return [(node.file_id, node.filename or "")]
    entries: list[tuple[str, str]] = []
    for child in node.children:
        entries.extend(_descendant_file_entries(child))
    return entries


def subtree_has_filter_match(
    node: TreeNode,
    pattern: str,
    level: int,
    filter_active: bool,
    *,
    invert: bool = False,
) -> bool:
    if not filter_active or not pattern:
        return True
    if node.kind == "file":
        return file_matches_filter(node.filename or "", pattern, level, invert=invert)
    return any(
        subtree_has_filter_match(child, pattern, level, filter_active, invert=invert)
        for child in node.children
    )


def flatten_tree(
    nodes: list[TreeNode],
    checked: set[str],
    *,
    pattern: str = "",
    filter_level: int = 1,
    filter_active: bool = False,
    filter_invert: bool = False,
) -> list[tuple[TreeNode, str]]:
    """Visible rows as (node, display_line)."""
    rows: list[tuple[TreeNode, str]] = []

    def walk(node: TreeNode) -> None:
        if filter_active and pattern:
            if node.kind == "file":
                if not file_matches_filter(
                    node.filename or "", pattern, filter_level, invert=filter_invert
                ):
                    return
            elif not subtree_has_filter_match(
                node, pattern, filter_level, True, invert=filter_invert
            ):
                return

        visible_files = node_files_filtered(
            node,
            pattern,
            filter_level,
            filter_active and bool(pattern),
            invert=filter_invert,
        )
        mark = node_mark(node, checked, visible_files)
        indent = "  " * node.depth
        if node.kind == "file":
            line = f"{indent}{mark} {node.label}"
        elif node.expanded:
            if filter_active and pattern and node.kind != "file":
                count = len(visible_files)
                line = f"{indent}{mark} ▾ {node.label} ({count})"
            else:
                line = f"{indent}{mark} ▾ {node.label} ({node.file_count})"
        else:
            if filter_active and pattern and node.kind != "file":
                count = len(visible_files)
                line = f"{indent}{mark} ▸ {node.label} ({count})"
            else:
                line = f"{indent}{mark} ▸ {node.label} ({node.file_count})"
        size_label = node_size_label(node)
        if size_label:
            line = f"{line}  {size_label}"
        rows.append((node, line))
        if node.expanded:
            for child in node.children:
                walk(child)

    for root in nodes:
        walk(root)
    return rows


def flatten_cached(
    cache: FlattenCache,
    key: tuple,
    nodes: list[TreeNode],
    checked: set[str],
    **kwargs,
) -> list[tuple[TreeNode, str]]:
    if cache.key == key:
        return cache.rows
    rows = flatten_tree(nodes, checked, **kwargs)
    cache.key = key
    cache.rows = rows
    return rows


def visible_file_ids(
    nodes: list[TreeNode],
    pattern: str,
    filter_level: int,
    filter_active: bool,
    *,
    invert: bool = False,
) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for root in nodes:
        for file_id in node_files_filtered(
            root, pattern, filter_level, filter_active, invert=invert
        ):
            if file_id not in seen:
                seen.add(file_id)
                ids.append(file_id)
    return ids


def node_mark(
    node: TreeNode, checked: set[str], visible_files: list[str] | None = None
) -> str:
    if node.kind == "file":
        return "[x]" if node.file_id in checked else "[ ]"
    files = visible_files if visible_files is not None else node_files(node)
    if not files:
        return "[ ]"
    selected = sum(1 for file_id in files if file_id in checked)
    if selected == 0:
        return "[ ]"
    if selected == len(files):
        return "[x]"
    return "[-]"


def toggle_node(
    node: TreeNode,
    checked: set[str],
    *,
    visible_files: list[str] | None = None,
) -> None:
    files = visible_files if visible_files is not None else node_files(node)
    if not files:
        return
    if all(file_id in checked for file_id in files):
        checked.difference_update(files)
    else:
        checked.update(files)
