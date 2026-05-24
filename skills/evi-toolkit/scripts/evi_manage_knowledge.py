#!/usr/bin/env python3
"""Sirius Knowledge Base Manager — 数据目录的增删查改工具。

供 Agent 调用，管理 knowledge/catalog.json 目录索引。

用法：
    # 列出所有知识数据
    python scripts/manage_knowledge.py --symbol 0700.HK --action list

    # 列出某类数据
    python scripts/manage_knowledge.py --symbol 0700.HK --action list --category research

    # 搜索
    python scripts/manage_knowledge.py --symbol 0700.HK --action search --query "goldman"

    # 删除某条记录（同时删文件）
    python scripts/manage_knowledge.py --symbol 0700.HK --action delete --category research --filename "2024-12-15_Goldman_Sachs.pdf"

    # 获取摘要统计
    python scripts/manage_knowledge.py --symbol 0700.HK --action stats

    # 导出 Markdown 格式的目录清单（供 Agent 读取）
    python scripts/manage_knowledge.py --symbol 0700.HK --action export-md

    # 验证目录完整性（检查文件是否存在）
    python scripts/manage_knowledge.py --symbol 0700.HK --action verify
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

SKILL_DIR = Path(__file__).resolve().parent.parent

# 数据输出目录：优先使用 workspace 根目录的 data/（沙箱环境）
_in_sandbox = ".agents/skills" in str(SKILL_DIR)
if _in_sandbox:
    DATA_DIR = SKILL_DIR.parent.parent.parent / "data"
else:
    DATA_DIR = SKILL_DIR / "data"


def _catalog_path(knowledge_dir: Path) -> Path:
    """Resolve catalog file: prefer EVI's _dl_catalog.json, fall back to legacy catalog.json."""
    new_p = knowledge_dir / "_dl_catalog.json"
    legacy_p = knowledge_dir / "catalog.json"
    if new_p.exists():
        return new_p
    if legacy_p.exists():
        return legacy_p
    return new_p  # default: write the new name


def _load_catalog(knowledge_dir: Path) -> dict[str, Any]:
    catalog_path = _catalog_path(knowledge_dir)
    if catalog_path.exists():
        return json.loads(catalog_path.read_text(encoding="utf-8"))
    return {"version": "1.0.0", "categories": {}, "stats": {}}


def _save_catalog(knowledge_dir: Path, catalog: dict[str, Any]):
    catalog["last_updated"] = datetime.now().isoformat()
    # 重算统计
    total_files = 0
    total_size = 0
    for cat in catalog.get("categories", {}).values():
        items = cat.get("items", [])
        total_files += len(items)
        for item in items:
            total_size += item.get("size_bytes", 0)
    catalog["stats"] = {
        "total_files": total_files,
        "total_size_mb": round(total_size / 1024 / 1024, 2),
    }
    _catalog_path(knowledge_dir).write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def action_list(catalog: dict, category: str | None = None):
    """列出目录内容。"""
    categories = catalog.get("categories", {})
    if category:
        items = categories.get(category, {}).get("items", [])
        print(f"\n## {category} ({len(items)} 项)\n")
        for item in items:
            print(f"  - [{item.get('date', '?')}] {item.get('filename', '?')}")
            if item.get("firm"):
                print(f"    机构: {item['firm']}")
            if item.get("title"):
                print(f"    标题: {item['title']}")
    else:
        for cat_name, cat_data in categories.items():
            items = cat_data.get("items", [])
            print(f"\n## {cat_name} ({len(items)} 项)")
            for item in items[:10]:
                print(f"  - [{item.get('date', '?')}] {item.get('filename', '?')}")
            if len(items) > 10:
                print(f"  ... 还有 {len(items) - 10} 项")


def action_search(catalog: dict, query: str):
    """搜索目录。"""
    query_lower = query.lower()
    results = []
    for cat_name, cat_data in catalog.get("categories", {}).items():
        for item in cat_data.get("items", []):
            searchable = json.dumps(item, ensure_ascii=False).lower()
            if query_lower in searchable:
                results.append((cat_name, item))

    print(f"\n搜索 '{query}' — 找到 {len(results)} 条结果:\n")
    for cat_name, item in results:
        print(f"  [{cat_name}] {item.get('date', '?')} - {item.get('filename', '?')}")
        if item.get("firm"):
            print(f"    机构: {item['firm']}")
        if item.get("title"):
            print(f"    标题: {item['title']}")
        print()


def action_delete(knowledge_dir: Path, catalog: dict, category: str, filename: str) -> bool:
    """删除指定文件和目录记录。"""
    items = catalog.get("categories", {}).get(category, {}).get("items", [])
    found = None
    for item in items:
        if item.get("filename") == filename:
            found = item
            break

    if not found:
        print(f"[error] 未找到: {category}/{filename}")
        return False

    # 删除文件
    rel_path = found.get("path", "").replace("knowledge/", "")
    file_path = knowledge_dir / rel_path
    if file_path.exists():
        file_path.unlink()
        print(f"  [deleted file] {file_path}")

    # 删除目录记录
    catalog["categories"][category]["items"] = [
        i for i in items if i.get("filename") != filename
    ]
    print(f"  [deleted record] {category}/{filename}")
    return True


def action_stats(catalog: dict):
    """输出统计信息。"""
    print("\n## 知识库统计\n")
    print(f"  Symbol: {catalog.get('symbol', '?')}")
    print(f"  Market: {catalog.get('market', '?')}")
    print(f"  Last Updated: {catalog.get('last_updated', '?')}")
    print()

    stats = catalog.get("stats", {})
    print(f"  总文件数: {stats.get('total_files', 0)}")
    print(f"  总大小: {stats.get('total_size_mb', 0)} MB")
    print()

    print("  分类明细:")
    for cat_name, cat_data in catalog.get("categories", {}).items():
        items = cat_data.get("items", [])
        desc = cat_data.get("description", "")
        size = sum(i.get("size_bytes", 0) for i in items)
        print(f"    {cat_name} ({desc}): {len(items)} 项, {size / 1024 / 1024:.2f} MB")


def action_export_md(knowledge_dir: Path, catalog: dict):
    """导出 Markdown 格式的目录清单（写入 knowledge/INDEX.md）。"""
    lines = ["# 知识库数据目录\n"]
    lines.append(f"**股票**: {catalog.get('symbol', '?')} | **市场**: {catalog.get('market', '?')}")
    lines.append(f"**更新时间**: {catalog.get('last_updated', '?')}\n")

    stats = catalog.get("stats", {})
    lines.append(f"**总计**: {stats.get('total_files', 0)} 文件, {stats.get('total_size_mb', 0)} MB\n")
    lines.append("---\n")

    for cat_name, cat_data in catalog.get("categories", {}).items():
        items = cat_data.get("items", [])
        desc = cat_data.get("description", cat_name)
        lines.append(f"## {desc} ({len(items)} 项)\n")

        if not items:
            lines.append("_暂无数据_\n")
            continue

        lines.append("| 日期 | 文件 | 来源 | 备注 |")
        lines.append("|------|------|------|------|")
        for item in sorted(items, key=lambda x: x.get("date", ""), reverse=True):
            date = item.get("date", "?")
            filename = item.get("filename", "?")
            source = item.get("source", "?")
            note = item.get("firm", item.get("title", item.get("type", "")))[:40]
            lines.append(f"| {date} | `{filename}` | {source} | {note} |")
        lines.append("")

    md_content = "\n".join(lines)
    index_path = knowledge_dir / "INDEX.md"
    index_path.write_text(md_content, encoding="utf-8")
    print(f"[export] 已导出到: {index_path}")
    print(md_content)


def action_verify(knowledge_dir: Path, catalog: dict):
    """验证目录完整性（检查文件是否实际存在）。"""
    print("\n## 完整性验证\n")
    missing = []
    ok = 0
    for cat_name, cat_data in catalog.get("categories", {}).items():
        for item in cat_data.get("items", []):
            rel_path = item.get("path", "").replace("knowledge/", "")
            file_path = knowledge_dir / rel_path
            if file_path.exists():
                ok += 1
            else:
                missing.append((cat_name, item.get("filename", "?"), str(file_path)))

    print(f"  ✓ 存在: {ok} 文件")
    print(f"  ✗ 缺失: {len(missing)} 文件")
    if missing:
        print("\n  缺失列表:")
        for cat, name, path in missing:
            print(f"    [{cat}] {name} → {path}")


def main():
    parser = argparse.ArgumentParser(description="EVI Knowledge Manager (forked from sirius)")
    parser.add_argument("--symbol", required=True, help="股票代码")
    parser.add_argument("--action", required=True,
                        choices=["list", "search", "delete", "stats", "export-md", "verify"],
                        help="操作类型")
    parser.add_argument("--category", default=None,
                        choices=["financials", "announcements", "research", "transcripts"],
                        help="数据分类")
    parser.add_argument("--query", default="", help="搜索关键词")
    parser.add_argument("--filename", default="", help="文件名（用于删除）")
    parser.add_argument("--data-dir", default=None,
                        help="EVI 项目根目录（如 data/0700_HK）。若提供：管理 <data-dir>/base/ 下的下载 catalog；"
                             "若不提供：回退到 sirius 老布局 data/<symbol>/knowledge/。")

    args = parser.parse_args()

    symbol_dir = args.symbol.replace(".", "_")
    if args.data_dir:
        knowledge_dir = Path(args.data_dir).resolve() / "base"
    else:
        knowledge_dir = DATA_DIR / symbol_dir / "knowledge"

    if not knowledge_dir.exists():
        print(f"[error] 知识库目录不存在: {knowledge_dir}")
        print("请先运行 download_knowledge.py 下载数据")
        sys.exit(1)

    catalog = _load_catalog(knowledge_dir)

    if args.action == "list":
        action_list(catalog, args.category)
    elif args.action == "search":
        if not args.query:
            print("[error] --search 需要 --query 参数")
            sys.exit(1)
        action_search(catalog, args.query)
    elif args.action == "delete":
        if not args.category or not args.filename:
            print("[error] --delete 需要 --category 和 --filename 参数")
            sys.exit(1)
        if action_delete(knowledge_dir, catalog, args.category, args.filename):
            _save_catalog(knowledge_dir, catalog)
    elif args.action == "stats":
        action_stats(catalog)
    elif args.action == "export-md":
        action_export_md(knowledge_dir, catalog)
    elif args.action == "verify":
        action_verify(knowledge_dir, catalog)


if __name__ == "__main__":
    main()
