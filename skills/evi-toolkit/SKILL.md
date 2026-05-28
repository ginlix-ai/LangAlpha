---
name: evi-toolkit
description: "EVI Strategy 共享工具集（v2）：项目骨架、PDF→MD、MD&A 抽取、catalog 维护、CHECKLIST 构建、Facts Index 抽取、报告优先持久化、以及 fetch/download/manage 三件套。"
---

# EVI Toolkit (v2)

> 这是 EVI Strategy 模板的共享工具 skill。所有其它 `evi-*` skill 在执行流水线中都会调用本 skill 提供的脚本。
>
> 本 skill 自带 `evi_fetch_data.py` / `evi_download_knowledge.py` / `evi_manage_knowledge.py`，与外部依赖完全解耦。

---

## 脚本清单

| 脚本 | 作用 | 何时调用 |
|---|---|---|
| `init_project.py` | 创建 `data/{symbol_dir}/` 目录骨架 + 空 catalog | Phase 1 起点 |
| `evi_fetch_data.py` | FMP 6 张表 + 量化估值引擎 → `base/fmp/`、`base/financials/financial_context.md`、`base/validation/engine_result.json` | Phase 1 |
| `evi_download_knowledge.py` | 下载财报/公告/研报/电话会 PDF → `base/{financials,research,transcripts}/raw/` | Phase 1 |
| `evi_manage_knowledge.py` | 查询/管理下载 catalog（_dl_catalog.json） | Phase 1 / 监控 |
| `parse_pdf.py` | PDF → Markdown（pdfplumber → pypdf 退化） | Phase 1 |
| `extract_mdna.py` | 从 parsed md 抽 MD&A 章节 | Phase 1 |
| `update_catalog.py` | 维护主 catalog `base/catalog.json`（与下载器的 _dl_catalog 分离） | Phase 1 / 监控 |
| `build_checklist.py` ⭐ | 自检数据完整性 → `base/CHECKLIST.{md,json}` | Phase 1 末 / Phase 4 末 |
| `format_facts.py` ⭐ | 从 reports/*.md 的 `## Facts Index` 抽 fact → `information/indexed_facts.json` | 每篇 report 写完后 |
| `persist_evi_report.py` ⭐ | 把 reports + facets + checklist + legacy 数据组装成 v2 payload，POST 给后端 | Phase 4 |
| `evi_persist_entry.py` | v1 持久化（旧版，保留） | 兼容历史 entry |

---

## 1. EVI 项目数据架构

每个公司一个独立目录，结构如下（与设计文档第 11 节一致）：

```
data/{symbol_dir}/
├── base/
│   ├── financials/
│   │   ├── raw/             # 原始 PDF（来自 download_knowledge）
│   │   ├── parsed/          # PDF→Markdown
│   │   ├── mdna/            # MD&A 单独抽取
│   │   ├── segments/        # 分部数据 segment_data.json
│   │   └── indicators/      # 关键指标 key_metrics.json
│   ├── research/
│   │   ├── raw/
│   │   ├── parsed/
│   │   └── research_catalog.json
│   ├── transcripts/
│   │   ├── raw/
│   │   └── transcript_catalog.json
│   ├── fmp/                 # fetch_data.py 写入的 FMP raw
│   ├── validation/          # FMP 校验结果 fmp_reconcile.json
│   ├── catalog.json         # base 目录索引
│   └── INDEX.md
│
├── information/
│   ├── indexed_facts.json
│   ├── search_plan.json
│   ├── source_reliability.json
│   └── information_delta.json
│
├── business_segments.json
├── valuation_method_matrix.json
│
├── valuation/
│   ├── group/
│   │   ├── assumption_ledger.json
│   │   ├── reverse_valuation.json
│   │   └── final_company_valuation.json
│   └── {segment_id}/
│       ├── assumption_ledger.json
│       ├── growth_bridge.json
│       ├── margin_bridge.json
│       ├── risk_adjustment.json
│       ├── dcf_result.json
│       ├── ps_result.json
│       ├── peg_result.json
│       ├── comps_result.json
│       ├── ddm_result.json
│       └── final_segment_valuation.json
│
└── monitor/
    ├── new_materials.json
    ├── trigger_log.json
    ├── information_delta.json
    └── revaluation_tasks.json
```

---

## 2. 脚本清单

### 2.1 `init_project.py` — 项目骨架初始化

```bash
python3 scripts/init_project.py --symbol 0700.HK --market hk
```

会做：
1. 创建上述全部目录骨架
2. 写空的 `base/catalog.json`（schema_version=1）
3. 写空的 `information/indexed_facts.json`
4. 写出 `base/INDEX.md` 占位

输出：`data/{symbol_dir}/base/catalog.json` + 全部目录。

### 2.2 `parse_pdf.py` — PDF 转 Markdown

```bash
python3 scripts/parse_pdf.py \
  --in  data/{symbol_dir}/base/financials/raw/2024-annual-report.pdf \
  --out data/{symbol_dir}/base/financials/parsed/2024-annual-report.md
```

实现策略（自包含、不依赖宿主项目）：
1. 优先使用 `pdfplumber`（如安装），按页提取文本+保留段落
2. 退化使用 `pypdf` 的 `extract_text()`
3. 都不可用时记录 `{"status":"manual_required"}`，让 Agent 知道需要手动处理

> 由于 PDF 解析对长财报很耗时，调用方（`evi-base-data-builder`）会在 prompt 里限制只解析最近 3 个年报 + 最近 1 个半年报。

### 2.3 `extract_mdna.py` — 从已解析的 Markdown 中抽取 MD&A 章节

```bash
python3 scripts/extract_mdna.py \
  --parsed-md data/{symbol_dir}/base/financials/parsed/2024-annual-report.md \
  --out       data/{symbol_dir}/base/financials/mdna/2024-mdna.md
```

抽取策略（启发式 + 可被 LLM 校正）：
1. 港股 / 美股关键词：`Management Discussion and Analysis` / `MD&A` / `管理层讨论与分析` / `董事局报告` / `业务回顾`
2. 抽取该章节标题到下一个一级章节标题之间的全部内容
3. 命中失败时输出 `{"status":"unmatched", "candidates":[...]}` 给 Agent 接管

### 2.4 `update_catalog.py` — 维护 base/catalog.json

```bash
python3 scripts/update_catalog.py --symbol 0700.HK --rebuild
```

扫描 `base/` 下所有文件，重建 `catalog.json`：

```json
{
  "schema_version": 1,
  "symbol": "0700.HK",
  "updated_at": "2026-05-21T22:30:00Z",
  "items": [
    {"id":"doc_2024_annual","kind":"financials","title":"2024 Annual Report","raw_path":"base/financials/raw/2024-annual-report.pdf","parsed_path":"base/financials/parsed/2024-annual-report.md","mdna_path":"base/financials/mdna/2024-mdna.md","period":"2024","language":"zh-Hant"}
  ]
}
```

`update_catalog.py --add` / `--remove` 提供增量维护接口。

### 2.5 `evi_persist_entry.py` — 最终持久化到 template_entries

被 Agent 在工作流末尾调用：

```bash
python3 scripts/evi_persist_entry.py \
  --entry-id <uuid> \
  --data-dir data/{symbol_dir}
```

会读取 `valuation/group/final_company_valuation.json`、各 `valuation/{segment_id}/*` 结果，
组合为 EVI payload（结构见下文 §3），POST 到 `/api/v1/templates/_internal/entries/{entry_id}/finalize`。

> **行为约定**：找不到 `final_company_valuation.json` 但有部分 segment 结果时，会以 `partial` 状态写回，让看板把这家公司标为"部分完成"，而不是直接 failed。

---

## 3. EVI Payload 约定

`evi_persist_entry.py` 写入 `template_entries.payload` 的结构：

```jsonc
{
  "schema_version": "evi-1.0",
  "company": {
    "symbol": "0700.HK",
    "display_name": "腾讯科技",
    "market": "hk"
  },
  "business_segments": { ...business_segments.json... },
  "valuation_method_matrix": { ...valuation_method_matrix.json... },

  "segments": {
    "<segment_id>": {
      "name": "云业务",
      "assumption_ledger": { ... },
      "results": {
        "DCF":   { "values": {"bear":..,"base":..,"bull":..}, "confidence":.. },
        "PS":    { ... },
        "PEG":   { ... },
        "Comps": { ... },
        "DDM":   { ... }
      },
      "final": {
        "method_results": [ {"method":"DCF","weight":0.5,"base":150000}, ... ],
        "final_values":   {"bear":.., "base":.., "bull":..},
        "currency":       "RMB million"
      }
    }
  },

  "group": {
    "assumption_ledger":   { ... },
    "reverse_valuation":   { ... },
    "final":               { ...final_company_valuation.json... }
  },

  "monitor": {
    "last_run_id":   "monitor_0700.HK_2026-05-21",
    "last_checked_at": "2026-05-21",
    "open_tasks":    [ ...revaluation_tasks.json items still pending... ]
  },

  "indexed_facts_summary": {
    "total": 142,
    "by_segment": {"cloud": 38, "games": 51, "fintech": 22, "advertising": 31},
    "high_reliability_pct": 0.62
  }
}
```

`summary`（看板展示用，4-8 个字段）：

```jsonc
{
  "company_name":        "腾讯科技",
  "fair_value_base":     145.2,        // 单股 / 总市值（按 currency_unit 决定）
  "fair_value_bear":     110.0,
  "fair_value_bull":     205.0,
  "current_price":       380.0,
  "upside_pct":          -16.4,
  "judgment":            "高估" ,       // 低估 / 合理 / 高估
  "currency_unit":       "HKD per share",
  "n_segments":          5,
  "monitor_open_tasks":  2,
  "schema_version":      "evi-1.0"
}
```

---

## 4. 与其它 evi-* skill 的协作

| skill | 调用本 toolkit 的脚本 | 时机 |
|---|---|---|
| `evi-base-data-builder` | `init_project.py` → `parse_pdf.py` → `extract_mdna.py` → `update_catalog.py` | 流程开始 |
| `evi-information-search` | `update_catalog.py` | 把新发现的材料登记进 catalog |
| `evi-valuation-orchestrator` | `evi_persist_entry.py` | 流程末尾 |
| `evi-monitor` | `update_catalog.py` | 增量发现新材料 |
| `evi-revaluation-updater` | `evi_persist_entry.py` | 监控更新后重新持久化 |

> ⚠️ **不要**让其它 skill 直接重写本 toolkit 的脚本，所有目录约定以本文档为准。
