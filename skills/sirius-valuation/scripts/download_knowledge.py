#!/usr/bin/env python3
"""Sirius Knowledge Base Downloader & Manager.

一站式下载和管理股票分析所需的所有知识数据：
  - 财报 PDF（港股 HKEx / A股巨潮资讯）
  - 公司公告（港股 HKEx / A股巨潮资讯）
  - 研报（国内外专业投研机构）
  - 电话会纪要（FMP Earnings Call Transcript）

下载后自动维护 catalog.json 目录索引，供 Agent 加载和查询。

用法：
    # 下载全部数据
    python scripts/download_knowledge.py --symbol 0700.HK --market hk --all

    # 仅下载财报
    python scripts/download_knowledge.py --symbol 0700.HK --market hk --financials

    # 仅下载研报
    python scripts/download_knowledge.py --symbol 0700.HK --market hk --research

    # 仅下载公告
    python scripts/download_knowledge.py --symbol 0700.HK --market hk --announcements

    # 仅下载电话会纪要
    python scripts/download_knowledge.py --symbol 0700.HK --market hk --transcripts

    # 查看目录
    python scripts/download_knowledge.py --symbol 0700.HK --catalog

    # 清理过期数据
    python scripts/download_knowledge.py --symbol 0700.HK --cleanup --before 2020-01-01

环境变量：
    FMP_API_KEY         — FMP API key (电话会纪要)
    SERPAPI_KEY          — SerpAPI key (研报搜索, 可选)

输出目录结构：
    data/{symbol}/knowledge/
    ├── catalog.json                    # 数据目录索引
    ├── financials/                     # 财报 PDF
    │   ├── 2024-annual-report.pdf
    │   ├── 2024-H1-interim-report.pdf
    │   └── ...
    ├── announcements/                  # 公告
    │   ├── 2024-12-01_profit-warning.pdf
    │   └── ...
    ├── research/                       # 研报
    │   ├── 2024-12-15_goldman-sachs.pdf
    │   ├── 2024-11-20_morgan-stanley.pdf
    │   ├── 2024-10-08_cicc.pdf
    │   └── ...
    └── transcripts/                    # 电话会纪要
        ├── 2024-Q3-earnings-call.md
        └── ...
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("skill.download_knowledge")

SKILL_DIR = Path(__file__).resolve().parent.parent

# 数据输出目录：优先使用 workspace 根目录的 data/（沙箱环境）
# 判断逻辑：如果脚本在 .agents/skills/ 下运行，说明是沙箱环境，输出到 workspace 根目录
_in_sandbox = ".agents/skills" in str(SKILL_DIR)
if _in_sandbox:
    # 沙箱路径: /home/workspace/.agents/skills/sirius-valuation/ → /home/workspace/data/
    DATA_DIR = SKILL_DIR.parent.parent.parent / "data"
else:
    DATA_DIR = SKILL_DIR / "data"

# ═══════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════

def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _download_file(url: str, dest: Path, headers: dict | None = None, timeout: int = 60) -> bool:
    """下载文件到指定路径，返回是否成功。"""
    if dest.exists():
        log.info("  [skip] 已存在: %s", dest.name)
        return True
    try:
        req = urllib.request.Request(url, headers=headers or {})
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LangAlpha/1.0")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            dest.write_bytes(data)
            log.info("  [ok] %s (%.1f KB)", dest.name, len(data) / 1024)
            return True
    except Exception as e:
        log.warning("  [fail] %s: %s", dest.name, e)
        return False


def _fmp_key() -> str:
    key = os.environ.get("FMP_API_KEY", "")
    if key:
        return key
    env_file = SKILL_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("FMP_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _fmp_get(endpoint: str, params: dict | None = None, timeout: int = 15) -> Any:
    """调用 FMP API。"""
    import requests
    params = dict(params or {})
    key = _fmp_key()
    if not key:
        raise ValueError("FMP_API_KEY not set")
    params["apikey"] = key
    url = f"https://financialmodelingprep.com/api/v3/{endpoint}"
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _safe_filename(name: str) -> str:
    """清理文件名中的非法字符。"""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name[:200].strip()


# ═══════════════════════════════════════════
# Catalog（目录索引）管理
# ═══════════════════════════════════════════

class CatalogManager:
    """管理 knowledge/catalog.json 数据目录。"""

    def __init__(self, knowledge_dir: Path):
        self.knowledge_dir = knowledge_dir
        self.catalog_path = knowledge_dir / "catalog.json"
        self.catalog = self._load()

    def _load(self) -> dict[str, Any]:
        if self.catalog_path.exists():
            try:
                return json.loads(self.catalog_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "version": "1.0.0",
            "last_updated": None,
            "symbol": None,
            "market": None,
            "categories": {
                "financials": {"description": "财报 PDF", "items": []},
                "announcements": {"description": "公司公告", "items": []},
                "research": {"description": "研究报告", "items": []},
                "transcripts": {"description": "电话会纪要", "items": []},
            },
            "stats": {"total_files": 0, "total_size_mb": 0},
        }

    def save(self):
        """保存 catalog 到磁盘。"""
        self.catalog["last_updated"] = datetime.now().isoformat()
        # 重算统计
        total_files = 0
        total_size = 0
        for cat in self.catalog["categories"].values():
            total_files += len(cat.get("items", []))
            for item in cat.get("items", []):
                total_size += item.get("size_bytes", 0)
        self.catalog["stats"] = {
            "total_files": total_files,
            "total_size_mb": round(total_size / 1024 / 1024, 2),
        }
        self.catalog_path.write_text(
            json.dumps(self.catalog, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def add_item(self, category: str, item: dict[str, Any]):
        """添加一条目录记录（去重）。"""
        items = self.catalog["categories"].setdefault(category, {"description": category, "items": []})["items"]
        # 按 filename 去重
        filename = item.get("filename", "")
        for existing in items:
            if existing.get("filename") == filename:
                existing.update(item)
                return
        items.append(item)

    def remove_item(self, category: str, filename: str) -> bool:
        """删除一条目录记录。"""
        items = self.catalog["categories"].get(category, {}).get("items", [])
        before = len(items)
        self.catalog["categories"][category]["items"] = [
            i for i in items if i.get("filename") != filename
        ]
        return len(self.catalog["categories"][category]["items"]) < before

    def list_items(self, category: str | None = None) -> dict[str, list]:
        """列出目录内容。"""
        if category:
            return {category: self.catalog["categories"].get(category, {}).get("items", [])}
        return {k: v.get("items", []) for k, v in self.catalog["categories"].items()}

    def set_meta(self, symbol: str, market: str):
        self.catalog["symbol"] = symbol
        self.catalog["market"] = market


# ═══════════════════════════════════════════
# 港股财报/公告下载（HKEx titleSearchServlet）
# ═══════════════════════════════════════════

HKEX_BASE = "https://www1.hkexnews.hk"

def _hk_stock_code(symbol: str) -> str:
    """从 symbol 提取纯数字港股代码，如 '0700.HK' -> '00700'."""
    code = symbol.upper().replace(".HK", "").lstrip("0")
    return code.zfill(5)


def _hkex_search(stock_code: str, t1code: str = "-2", page_size: int = 20, keyword: str = "") -> list[dict]:
    """调用 HKEx titleSearchServlet.do 获取公告列表。

    t1code 分类：
      -2 = 全部
      40000 = Annual Reports
      40100 = Interim/Half-year Reports
      40200 = Quarterly Reports
    """
    params = {
        "sortDir": "0",
        "sortByOptions": "DateTime",
        "category": "0",
        "market": "SEHK",
        "searchType": "1",
        "documentType": "-1",
        "t1code": t1code,
        "t2Gcode": "-2",
        "t2code": "-2",
        "stockId": "-1",
        "from": "0",
        "pageSize": str(page_size),
        "lang": "EN",
        "keyword": keyword or stock_code,
    }
    query_str = urllib.parse.urlencode(params)
    url = f"{HKEX_BASE}/search/titleSearchServlet.do?{query_str}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LangAlpha/1.0")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            result_str = data.get("result", "null")
            if result_str and result_str != "null":
                items = json.loads(result_str)
                # 精确过滤：STOCK_CODE 必须完全匹配（排除衍生品等多code记录）
                filtered = [
                    item for item in items
                    if item.get("STOCK_CODE", "").strip() == stock_code
                ]
                # 如果精确匹配无结果，回退到包含匹配（排除含 <br/> 的多code记录）
                if not filtered:
                    filtered = [
                        item for item in items
                        if stock_code in item.get("STOCK_CODE", "") and "<br/>" not in item.get("STOCK_CODE", "")
                    ]
                return filtered if filtered else items[:20]
            return []
    except Exception as e:
        log.warning("  [warn] HKEx API 请求失败: %s", e)
        return []


def download_hk_financials(symbol: str, knowledge_dir: Path, catalog: CatalogManager, years: int = 3) -> int:
    """下载港股财报 PDF (年报/中期报告)。

    使用 HKEx titleSearchServlet.do API。
    """
    financials_dir = _ensure_dir(knowledge_dir / "financials")
    stock_code = _hk_stock_code(symbol)
    count = 0

    log.info("[港股财报] 正在获取 %s 的财报列表...", symbol)

    # 搜索年报和中期报告
    for t1code, doc_label in [("40000", "annual-report"), ("40100", "interim-report")]:
        items = _hkex_search(stock_code, t1code=t1code, page_size=years * 2)
        log.info("  [%s] 找到 %d 条记录", doc_label, len(items))

        for item in items[:years]:
            file_link = item.get("FILE_LINK", "")
            if not file_link:
                continue
            # 构建完整 URL
            if file_link.startswith("/"):
                file_link = f"{HKEX_BASE}{file_link}"

            file_type = item.get("FILE_TYPE", "PDF").upper()
            date_str = item.get("DATE_TIME", "")[:10]  # DD/MM/YYYY format
            title = item.get("TITLE", "")

            # 解析日期
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y")
                date_formatted = dt.strftime("%Y-%m-%d")
                year = dt.strftime("%Y")
            except (ValueError, TypeError):
                year_match = re.search(r'20[12]\d', file_link)
                year = year_match.group() if year_match else str(datetime.now().year)
                date_formatted = f"{year}-01-01"

            ext = ".pdf" if file_type == "PDF" else f".{file_type.lower()}"
            filename = f"{year}-{doc_label}{ext}"
            dest = financials_dir / filename

            if _download_file(file_link, dest):
                catalog.add_item("financials", {
                    "filename": filename,
                    "path": f"knowledge/financials/{filename}",
                    "type": doc_label,
                    "title": title,
                    "year": year,
                    "date": date_formatted,
                    "market": "hk",
                    "source": "HKEx",
                    "url": file_link,
                    "size_bytes": dest.stat().st_size if dest.exists() else 0,
                    "downloaded_at": datetime.now().isoformat(),
                })
                count += 1

    # 备选方案：使用 FMP SEC Filings API
    if count == 0:
        log.info("  [fallback] 尝试 FMP SEC filings API...")
        try:
            filings = _fmp_get(f"sec_filings/{symbol}", {"type": "20-F", "limit": years * 2})
            for filing in (filings or []):
                link = filing.get("finalLink") or filing.get("link", "")
                if not link:
                    continue
                date = filing.get("fillingDate", "")[:10]
                ftype = filing.get("type", "annual")
                filename = f"{date}_{ftype}.pdf"
                dest = financials_dir / _safe_filename(filename)
                if _download_file(link, dest):
                    catalog.add_item("financials", {
                        "filename": dest.name,
                        "path": f"knowledge/financials/{dest.name}",
                        "type": ftype,
                        "date": date,
                        "market": "hk",
                        "source": "FMP/SEC",
                        "url": link,
                        "size_bytes": dest.stat().st_size if dest.exists() else 0,
                        "downloaded_at": datetime.now().isoformat(),
                    })
                    count += 1
        except Exception as e:
            log.warning("  [warn] FMP fallback 失败: %s", e)

    log.info("[港股财报] 完成，下载 %d 份文件", count)
    return count


# ═══════════════════════════════════════════
# A股财报下载（巨潮资讯）
# ═══════════════════════════════════════════

def _cn_stock_code(symbol: str) -> tuple[str, str]:
    """提取A股代码和交易所。'600519.SS' -> ('600519', 'sh'), '000001.SZ' -> ('000001', 'sz')."""
    parts = symbol.upper().replace(".SS", ".SH").split(".")
    code = parts[0]
    exchange = parts[1].lower() if len(parts) > 1 else ("sh" if code.startswith("6") else "sz")
    return code, exchange


def download_cn_financials(symbol: str, knowledge_dir: Path, catalog: CatalogManager, years: int = 3) -> int:
    """下载A股财报 PDF（年报/半年报/季报）。

    使用巨潮资讯网 API。
    """
    financials_dir = _ensure_dir(knowledge_dir / "financials")
    stock_code, exchange = _cn_stock_code(symbol)
    org_id = f"gs{exchange}{stock_code}"
    count = 0

    log.info("[A股财报] 正在获取 %s 的财报列表...", symbol)

    # 巨潮资讯网 API
    # category: category_ndbg_szsh (年报), category_bndbg_szsh (半年报), category_sjdbg_szsh (季报)
    categories = [
        ("category_ndbg_szsh", "annual-report"),
        ("category_bndbg_szsh", "semi-annual-report"),
    ]

    for cat_code, cat_label in categories:
        try:
            api_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
            post_data = urllib.parse.urlencode({
                "stock": f"{stock_code},{org_id}",
                "tabName": "fulltext",
                "pageSize": str(years * 2),
                "pageNum": "1",
                "column": exchange.upper() + "SE_MAIN",
                "category": cat_code,
                "seDate": "",
            }).encode("utf-8")

            req = urllib.request.Request(api_url, data=post_data, method="POST")
            req.add_header("User-Agent", "Mozilla/5.0 LangAlpha/1.0")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            req.add_header("Accept", "application/json")

            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                announcements = result.get("announcements", []) or []

                for ann in announcements[:years]:
                    adj_title = ann.get("announcementTitle", "")
                    ann_id = ann.get("announcementId", "")
                    ann_time = ann.get("announcementTime", 0)

                    if ann_time:
                        date_str = datetime.fromtimestamp(ann_time / 1000).strftime("%Y-%m-%d")
                    else:
                        date_str = "unknown"

                    # 巨潮 PDF 下载链接
                    pdf_url = f"http://static.cninfo.com.cn/{ann.get('adjunctUrl', '')}"
                    filename = f"{date_str}_{cat_label}_{_safe_filename(adj_title)[:60]}.pdf"
                    dest = financials_dir / filename

                    if _download_file(pdf_url, dest):
                        catalog.add_item("financials", {
                            "filename": filename,
                            "path": f"knowledge/financials/{filename}",
                            "type": cat_label,
                            "title": adj_title,
                            "date": date_str,
                            "market": "cn",
                            "source": "巨潮资讯",
                            "url": pdf_url,
                            "size_bytes": dest.stat().st_size if dest.exists() else 0,
                            "downloaded_at": datetime.now().isoformat(),
                        })
                        count += 1
        except Exception as e:
            log.warning("  [warn] 巨潮 API 请求失败 (%s): %s", cat_label, e)

    log.info("[A股财报] 完成，下载 %d 份文件", count)
    return count


# ═══════════════════════════════════════════
# 公告下载
# ═══════════════════════════════════════════

def download_hk_announcements(symbol: str, knowledge_dir: Path, catalog: CatalogManager, limit: int = 20) -> int:
    """下载港股公告（盈利预告、内幕消息、股权变动等）。"""
    ann_dir = _ensure_dir(knowledge_dir / "announcements")
    stock_code = _hk_stock_code(symbol)
    count = 0

    log.info("[港股公告] 正在获取 %s 的公告列表...", symbol)

    # 使用 HKEx titleSearchServlet 搜索所有公告
    items = _hkex_search(stock_code, t1code="-2", page_size=limit)
    log.info("  找到 %d 条记录", len(items))

    for item in items[:limit]:
        file_link = item.get("FILE_LINK", "")
        if not file_link:
            continue
        if file_link.startswith("/"):
            file_link = f"{HKEX_BASE}{file_link}"

        file_type = item.get("FILE_TYPE", "PDF").upper()
        if file_type not in ("PDF", "HTM", "HTML"):
            continue  # 跳过 xlsx 等非文档

        date_str = item.get("DATE_TIME", "")[:10]
        title = item.get("TITLE", "")[:80]
        long_text = item.get("LONG_TEXT", "")

        # 解析日期
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            date_formatted = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_formatted = datetime.now().strftime("%Y-%m-%d")

        ext = ".pdf" if file_type == "PDF" else f".{file_type.lower()}"
        filename = f"{date_formatted}_{_safe_filename(title)[:60]}{ext}"
        dest = ann_dir / filename

        if _download_file(file_link, dest):
            catalog.add_item("announcements", {
                "filename": filename,
                "path": f"knowledge/announcements/{filename}",
                "type": "announcement",
                "title": title,
                "category": long_text,
                "date": date_formatted,
                "market": "hk",
                "source": "HKEx",
                "url": file_link,
                "size_bytes": dest.stat().st_size if dest.exists() else 0,
                "downloaded_at": datetime.now().isoformat(),
            })
            count += 1

    log.info("[港股公告] 完成，下载 %d 份文件", count)
    return count


def download_cn_announcements(symbol: str, knowledge_dir: Path, catalog: CatalogManager, limit: int = 20) -> int:
    """下载A股公告（业绩预告、重大事项等）。"""
    ann_dir = _ensure_dir(knowledge_dir / "announcements")
    stock_code, exchange = _cn_stock_code(symbol)
    org_id = f"gs{exchange}{stock_code}"
    count = 0

    log.info("[A股公告] 正在获取 %s 的公告列表...", symbol)

    try:
        api_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        post_data = urllib.parse.urlencode({
            "stock": f"{stock_code},{org_id}",
            "tabName": "fulltext",
            "pageSize": str(limit),
            "pageNum": "1",
            "column": exchange.upper() + "SE_MAIN",
            "category": "",
            "seDate": "",
        }).encode("utf-8")

        req = urllib.request.Request(api_url, data=post_data, method="POST")
        req.add_header("User-Agent", "Mozilla/5.0 LangAlpha/1.0")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        req.add_header("Accept", "application/json")

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            announcements = result.get("announcements", []) or []

            for ann in announcements[:limit]:
                adj_title = ann.get("announcementTitle", "")
                ann_time = ann.get("announcementTime", 0)
                date_str = datetime.fromtimestamp(ann_time / 1000).strftime("%Y-%m-%d") if ann_time else "unknown"

                pdf_url = f"http://static.cninfo.com.cn/{ann.get('adjunctUrl', '')}"
                filename = f"{date_str}_{_safe_filename(adj_title)[:80]}.pdf"
                dest = ann_dir / filename

                if _download_file(pdf_url, dest):
                    catalog.add_item("announcements", {
                        "filename": filename,
                        "path": f"knowledge/announcements/{filename}",
                        "type": "announcement",
                        "title": adj_title,
                        "date": date_str,
                        "market": "cn",
                        "source": "巨潮资讯",
                        "url": pdf_url,
                        "size_bytes": dest.stat().st_size if dest.exists() else 0,
                        "downloaded_at": datetime.now().isoformat(),
                    })
                    count += 1
    except Exception as e:
        log.error("  [error] A股公告下载异常: %s", e)

    log.info("[A股公告] 完成，下载 %d 份文件", count)
    return count


# ═══════════════════════════════════════════
# 研报下载（国内外专业投研机构）
# ═══════════════════════════════════════════

# 知名投研机构列表
RESEARCH_FIRMS = {
    "global": [
        "Goldman Sachs", "Morgan Stanley", "JP Morgan", "UBS",
        "Citigroup", "HSBC", "Deutsche Bank", "Barclays",
        "Credit Suisse", "BofA Securities", "Jefferies", "Bernstein",
    ],
    "china": [
        "中金公司", "中信证券", "华泰证券", "国泰君安", "招商证券",
        "海通证券", "申万宏源", "广发证券", "兴业证券", "东方证券",
        "光大证券", "天风证券", "浙商证券", "国盛证券",
    ],
    "hk": [
        "CLSA", "Macquarie", "Daiwa", "Nomura", "Bank of China International",
    ],
}


def download_research_reports(
    symbol: str,
    company_name: str,
    market: str,
    knowledge_dir: Path,
    catalog: CatalogManager,
    limit: int = 20,
) -> int:
    """下载研究报告。

    策略：
    1. 使用 FMP 的 analyst estimates / price target 获取机构覆盖信息
    2. 使用 SerpAPI（如有）搜索研报 PDF
    3. 记录到目录，即使无法直接下载 PDF 也记录研报元信息
    """
    research_dir = _ensure_dir(knowledge_dir / "research")
    count = 0

    log.info("[研报] 正在获取 %s (%s) 的研报信息...", symbol, company_name)

    # === 方式 1: FMP Analyst Estimates / Price Target ===
    try:
        # 获取分析师评级
        price_targets = _fmp_get(f"price-target/{symbol}", {"limit": str(limit)})
        if price_targets:
            for pt in price_targets[:limit]:
                analyst = pt.get("analystName", "Unknown")
                company = pt.get("analystCompany", "Unknown")
                date = pt.get("publishedDate", "")[:10]
                target = pt.get("priceTarget", "")
                rating = pt.get("newsTitle", pt.get("rating", ""))

                # 研报命名：日期-机构
                filename = f"{date}_{_safe_filename(company)}.json"
                dest = research_dir / filename

                # 保存研报元信息（即使没有 PDF）
                report_meta = {
                    "analyst": analyst,
                    "firm": company,
                    "date": date,
                    "price_target": target,
                    "rating": rating,
                    "symbol": symbol,
                    "source": "FMP Price Target",
                }
                dest.write_text(json.dumps(report_meta, ensure_ascii=False, indent=2), encoding="utf-8")

                catalog.add_item("research", {
                    "filename": filename,
                    "path": f"knowledge/research/{filename}",
                    "type": "analyst-report",
                    "firm": company,
                    "analyst": analyst,
                    "date": date,
                    "price_target": target,
                    "rating": rating,
                    "market": market,
                    "source": "FMP",
                    "has_pdf": False,
                    "size_bytes": dest.stat().st_size if dest.exists() else 0,
                    "downloaded_at": datetime.now().isoformat(),
                })
                count += 1
    except Exception as e:
        log.warning("  [warn] FMP analyst API 失败: %s", e)

    # === 方式 2: FMP Earnings Surprises (补充机构覆盖信息) ===
    try:
        surprises = _fmp_get(f"earnings-surprises/{symbol}")
        if surprises:
            for s in surprises[:5]:
                date = s.get("date", "")
                actual = s.get("actualEarningResult", "")
                estimated = s.get("estimatedEarning", "")
                # 这些信息附加到目录中
                log.info("  [info] Earnings %s: actual=%s est=%s", date, actual, estimated)
    except Exception:
        pass

    # === 方式 3: SerpAPI 搜索研报 (如有 key) ===
    serpapi_key = os.environ.get("SERPAPI_KEY", "")
    if serpapi_key and company_name:
        log.info("  [serpapi] 搜索研报 PDF...")
        try:
            # 搜索国际机构研报
            query = f'"{company_name}" research report filetype:pdf site:research'
            search_url = (
                f"https://serpapi.com/search.json?"
                f"q={urllib.parse.quote(query)}&api_key={serpapi_key}&num=10"
            )
            req = urllib.request.Request(search_url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                results = json.loads(resp.read().decode("utf-8"))
                organic = results.get("organic_results", [])
                for r in organic[:5]:
                    link = r.get("link", "")
                    title = r.get("title", "")
                    snippet = r.get("snippet", "")

                    if not link.endswith(".pdf"):
                        continue

                    # 尝试从标题/snippet提取机构名
                    firm = "Unknown"
                    for firm_list in RESEARCH_FIRMS.values():
                        for f in firm_list:
                            if f.lower() in title.lower() or f.lower() in snippet.lower():
                                firm = f
                                break
                        if firm != "Unknown":
                            break

                    date_str = datetime.now().strftime("%Y-%m-%d")
                    filename = f"{date_str}_{_safe_filename(firm)}_{_safe_filename(title)[:40]}.pdf"
                    dest = research_dir / filename

                    if _download_file(link, dest):
                        catalog.add_item("research", {
                            "filename": filename,
                            "path": f"knowledge/research/{filename}",
                            "type": "research-report",
                            "firm": firm,
                            "title": title,
                            "date": date_str,
                            "market": market,
                            "source": "SerpAPI",
                            "url": link,
                            "has_pdf": True,
                            "size_bytes": dest.stat().st_size if dest.exists() else 0,
                            "downloaded_at": datetime.now().isoformat(),
                        })
                        count += 1
        except Exception as e:
            log.warning("  [warn] SerpAPI 搜索失败: %s", e)

    # === 方式 4: 搜索国内研报 (东方财富/慧博) ===
    if market == "cn" and company_name:
        log.info("  [cn] 搜索国内研报...")
        stock_code, _ = _cn_stock_code(symbol)
        try:
            # 东方财富研报列表 API
            eastmoney_url = (
                f"https://reportapi.eastmoney.com/report/list?"
                f"industryCode=*&pageSize={limit}&industry=*&rating=*"
                f"&ratingChange=*&beginTime=&endTime=&pageNo=1"
                f"&fields=&qType=0&orgCode=&rcode=&code={stock_code}"
            )
            req = urllib.request.Request(eastmoney_url)
            req.add_header("User-Agent", "Mozilla/5.0 LangAlpha/1.0")

            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                reports_list = result.get("data", []) or []

                for rpt in reports_list[:limit]:
                    title = rpt.get("title", "")
                    org_name = rpt.get("orgSName", rpt.get("orgName", "Unknown"))
                    pub_date = rpt.get("publishDate", "")[:10]
                    researcher = rpt.get("researcher", "")
                    info_code = rpt.get("infoCode", "")

                    filename = f"{pub_date}_{_safe_filename(org_name)}.json"
                    dest = research_dir / filename

                    report_meta = {
                        "title": title,
                        "firm": org_name,
                        "researcher": researcher,
                        "date": pub_date,
                        "symbol": symbol,
                        "info_code": info_code,
                        "source": "东方财富",
                    }

                    # 尝试下载 PDF（东方财富研报链接）
                    pdf_url = f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else ""
                    pdf_filename = f"{pub_date}_{_safe_filename(org_name)}_{_safe_filename(title)[:40]}.pdf"
                    pdf_dest = research_dir / pdf_filename
                    has_pdf = False

                    if pdf_url and _download_file(pdf_url, pdf_dest):
                        has_pdf = True
                        report_meta["pdf_path"] = f"knowledge/research/{pdf_filename}"

                    dest.write_text(json.dumps(report_meta, ensure_ascii=False, indent=2), encoding="utf-8")

                    catalog.add_item("research", {
                        "filename": pdf_filename if has_pdf else filename,
                        "path": f"knowledge/research/{pdf_filename if has_pdf else filename}",
                        "type": "research-report",
                        "firm": org_name,
                        "title": title,
                        "researcher": researcher,
                        "date": pub_date,
                        "market": "cn",
                        "source": "东方财富",
                        "has_pdf": has_pdf,
                        "size_bytes": (pdf_dest if has_pdf else dest).stat().st_size if (pdf_dest if has_pdf else dest).exists() else 0,
                        "downloaded_at": datetime.now().isoformat(),
                    })
                    count += 1
        except Exception as e:
            log.warning("  [warn] 东方财富研报 API 失败: %s", e)

    log.info("[研报] 完成，获取 %d 条研报信息", count)
    return count


# ═══════════════════════════════════════════
# 电话会纪要下载（FMP Earnings Call Transcript）
# ═══════════════════════════════════════════

def download_transcripts(symbol: str, knowledge_dir: Path, catalog: CatalogManager, years: int = 2) -> int:
    """下载电话会纪要（FMP API）。"""
    transcripts_dir = _ensure_dir(knowledge_dir / "transcripts")
    count = 0

    log.info("[电话会] 正在获取 %s 的电话会纪要...", symbol)

    try:
        # FMP Earnings Call Transcript
        transcripts = _fmp_get(f"earning_call_transcript/{symbol}", {"limit": str(years * 4)})
        if not transcripts:
            log.info("  [info] FMP 无电话会数据")
            return 0

        for t in transcripts:
            date = t.get("date", "")[:10]
            quarter = t.get("quarter", "")
            year = t.get("year", date[:4])
            content = t.get("content", "")

            if not content:
                continue

            filename = f"{year}-Q{quarter}-earnings-call.md"
            dest = transcripts_dir / filename

            if not dest.exists():
                # 格式化为 Markdown
                md_content = f"# {symbol} Earnings Call Transcript\n\n"
                md_content += f"**Date**: {date}\n"
                md_content += f"**Quarter**: Q{quarter} {year}\n\n"
                md_content += "---\n\n"
                md_content += content
                dest.write_text(md_content, encoding="utf-8")
                log.info("  [ok] %s", filename)
            else:
                log.info("  [skip] %s", filename)

            catalog.add_item("transcripts", {
                "filename": filename,
                "path": f"knowledge/transcripts/{filename}",
                "type": "earnings-call",
                "date": date,
                "quarter": f"Q{quarter}",
                "year": year,
                "market": "auto",
                "source": "FMP",
                "size_bytes": dest.stat().st_size if dest.exists() else 0,
                "downloaded_at": datetime.now().isoformat(),
            })
            count += 1
    except Exception as e:
        log.error("  [error] 电话会纪要下载异常: %s", e)

    log.info("[电话会] 完成，下载 %d 份纪要", count)
    return count


# ═══════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Sirius Knowledge Base Downloader — 下载和管理股票分析知识数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--symbol", required=True, help="股票代码（如 0700.HK / AAPL / 600519.SS）")
    parser.add_argument("--market", default="", choices=["cn", "hk", "us", ""],
                        help="市场（空=自动推断）")
    parser.add_argument("--company-name", default="", help="公司名称（用于研报搜索）")

    # 下载选项
    parser.add_argument("--all", action="store_true", help="下载全部数据")
    parser.add_argument("--financials", action="store_true", help="仅下载财报")
    parser.add_argument("--announcements", action="store_true", help="仅下载公告")
    parser.add_argument("--research", action="store_true", help="仅下载研报")
    parser.add_argument("--transcripts", action="store_true", help="仅下载电话会纪要")

    # 管理选项
    parser.add_argument("--catalog", action="store_true", help="显示目录索引")
    parser.add_argument("--cleanup", action="store_true", help="清理过期数据")
    parser.add_argument("--before", default="", help="清理此日期之前的数据 (YYYY-MM-DD)")

    # 配置
    parser.add_argument("--years", type=int, default=3, help="财报下载年数 (默认 3)")
    parser.add_argument("--limit", type=int, default=20, help="公告/研报数量限制 (默认 20)")

    args = parser.parse_args()

    # 自动推断市场
    symbol = args.symbol
    market = args.market
    if not market:
        if ".HK" in symbol.upper():
            market = "hk"
        elif ".SS" in symbol.upper() or ".SZ" in symbol.upper():
            market = "cn"
        else:
            market = "us"

    # 设置输出目录
    symbol_dir = symbol.replace(".", "_")
    out_dir = DATA_DIR / symbol_dir
    knowledge_dir = _ensure_dir(out_dir / "knowledge")

    # 初始化目录管理器
    catalog = CatalogManager(knowledge_dir)
    catalog.set_meta(symbol, market)

    # === 查看目录 ===
    if args.catalog:
        items = catalog.list_items()
        print(json.dumps(catalog.catalog, ensure_ascii=False, indent=2))
        return

    # === 清理 ===
    if args.cleanup:
        if not args.before:
            log.error("--cleanup 需要 --before YYYY-MM-DD 参数")
            sys.exit(1)
        before_date = args.before
        removed = 0
        for cat_name, items in catalog.list_items().items():
            for item in items:
                item_date = item.get("date", "9999-99-99")
                if item_date < before_date:
                    filepath = knowledge_dir / item.get("path", "").replace("knowledge/", "")
                    if filepath.exists():
                        filepath.unlink()
                    catalog.remove_item(cat_name, item.get("filename", ""))
                    removed += 1
        catalog.save()
        log.info("清理完成，移除 %d 条过期记录", removed)
        return

    # === 下载 ===
    if not any([args.all, args.financials, args.announcements, args.research, args.transcripts]):
        args.all = True  # 默认下载全部

    log.info("=" * 60)
    log.info("Sirius Knowledge Base Downloader")
    log.info("  Symbol: %s | Market: %s", symbol, market)
    log.info("  Output: %s", knowledge_dir)
    log.info("=" * 60)

    total = 0
    t0 = time.time()

    # 1. 财报
    if args.all or args.financials:
        log.info("\n" + "─" * 40)
        if market == "hk":
            total += download_hk_financials(symbol, knowledge_dir, catalog, years=args.years)
        elif market == "cn":
            total += download_cn_financials(symbol, knowledge_dir, catalog, years=args.years)
        else:
            # 美股使用 FMP SEC filings
            try:
                filings = _fmp_get(f"sec_filings/{symbol}", {"type": "10-K", "limit": str(args.years)})
                financials_dir = _ensure_dir(knowledge_dir / "financials")
                for filing in (filings or []):
                    link = filing.get("finalLink") or filing.get("link", "")
                    date = filing.get("fillingDate", "")[:10]
                    filename = f"{date}_10-K.pdf"
                    dest = financials_dir / _safe_filename(filename)
                    if link and _download_file(link, dest):
                        catalog.add_item("financials", {
                            "filename": dest.name,
                            "path": f"knowledge/financials/{dest.name}",
                            "type": "10-K",
                            "date": date,
                            "market": "us",
                            "source": "FMP/SEC",
                            "url": link,
                            "size_bytes": dest.stat().st_size if dest.exists() else 0,
                            "downloaded_at": datetime.now().isoformat(),
                        })
                        total += 1
            except Exception as e:
                log.warning("[美股财报] 失败: %s", e)

    # 2. 公告
    if args.all or args.announcements:
        log.info("\n" + "─" * 40)
        if market == "hk":
            total += download_hk_announcements(symbol, knowledge_dir, catalog, limit=args.limit)
        elif market == "cn":
            total += download_cn_announcements(symbol, knowledge_dir, catalog, limit=args.limit)

    # 3. 研报
    if args.all or args.research:
        log.info("\n" + "─" * 40)
        company_name = args.company_name or ""
        # 尝试从 FMP profile 获取公司名
        if not company_name:
            try:
                profiles = _fmp_get(f"profile/{symbol}")
                if profiles and len(profiles) > 0:
                    company_name = profiles[0].get("companyName", "")
            except Exception:
                pass
        total += download_research_reports(symbol, company_name, market, knowledge_dir, catalog, limit=args.limit)

    # 4. 电话会纪要
    if args.all or args.transcripts:
        log.info("\n" + "─" * 40)
        total += download_transcripts(symbol, knowledge_dir, catalog, years=args.years)

    # 保存目录
    catalog.save()

    # 输出总结
    elapsed = time.time() - t0
    log.info("\n" + "=" * 60)
    log.info("知识库下载完成！")
    log.info("  📁 %s", knowledge_dir)
    log.info("  📊 总计: %d 项 | 耗时: %.1fs", total, elapsed)
    log.info("  📋 目录索引: %s", catalog.catalog_path)
    log.info("")
    log.info("目录结构：")
    log.info("  knowledge/catalog.json          — 数据目录索引")
    log.info("  knowledge/financials/           — 财报 PDF")
    log.info("  knowledge/announcements/        — 公告")
    log.info("  knowledge/research/             — 研报（按日期-机构命名）")
    log.info("  knowledge/transcripts/          — 电话会纪要")
    log.info("")
    stats = catalog.catalog.get("stats", {})
    log.info("统计: %d 文件, %.2f MB", stats.get("total_files", 0), stats.get("total_size_mb", 0))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
