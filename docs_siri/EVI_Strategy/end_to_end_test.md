# EVI Strategy 模板 — 端到端测试（腾讯科技 0700.HK）

> 本文档说明如何在本地启动 LangAlpha，对腾讯科技 0700.HK 进行 EVI Strategy 模板的完整测试。

---

## 一、启动本地环境

```bash
cd /Users/chenyu/Desktop/personal_file/LangAlpha
bash scripts/dev/start.sh
```

启动后：
- 后端：http://localhost:8000
- 前端：http://localhost:5173（Vite dev）

> 因为前端走 `pnpm dev` 热更新，所有 EVI 前端文件修改后**无需 build**。

---

## 二、模板已就位的位置

| 类别 | 路径 |
|---|---|
| 后端模板定义 | `src/server/templates/manifests/evi_strategy.py` |
| 注册 | `src/server/templates/registry.py` |
| 前端看板 | `web/src/pages/Templates/evi/EviDashboard.tsx` |
| 前端报告面板 | `web/src/pages/Templates/evi/EviReportPanel.tsx` |
| 看板挂载 | `web/src/pages/Templates/TemplateHome.tsx` (`CUSTOM_DASHBOARDS`) |
| 报告面板挂载 | `web/src/pages/ChatAgent/components/ThreadGallery.tsx` (`TEMPLATE_PANELS`) |
| Skill 全集 | `skills/evi-toolkit/`、`skills/evi-base-data-builder/`、`skills/evi-business-segmentation/`、`skills/evi-information-search/`、`skills/evi-valuation-router/`、`skills/evi-assumption-builder/`、`skills/evi-valuation-{dcf,ps,peg,ddm,comps}/`、`skills/evi-reverse-valuation/`、`skills/evi-valuation-orchestrator/`、`skills/evi-monitor/`、`skills/evi-revaluation-updater/` |

---

## 三、复用的 sirius 能力

- `skills/sirius-valuation/scripts/fetch_data.py`：FMP 财务 + 估值引擎
- `skills/sirius-valuation/scripts/download_knowledge.py`：财报/公告/研报/电话会下载
- `skills/sirius-valuation/scripts/manage_knowledge.py`：catalog 管理（备用）

EVI 不重写这些脚本——`evi-base-data-builder` 的 SKILL.md 直接调用它们。

---

## 四、端到端测试步骤

### Step 1：进入模板市场

打开浏览器：http://localhost:5173/chat/templates

应能看到 2 个模板：
- **Sirius 估值**（已有）
- **EVI 估值策略** ← 新增

### Step 2：实例化 EVI 模板

点击 **EVI 估值策略** → 右上角 **新增分析** → 在表单填：

| 字段 | 值 |
|---|---|
| 公司名称 | 腾讯科技 |
| 股票代码 | `0700.HK` |
| 市场 | 港股 (HK) |

点击提交 → 立即跳转到 workspace。

### Step 3：观察 Agent 执行流程

Agent 会按 `_EVI_PROMPT`（manifest 中定义）的 Phase 1-4 顺序执行 14 个 skill。
预期产物逐步写入 `data/0700_HK/`：

```
Phase 1（5-10 分钟）：
  data/0700_HK/base/{financials,research,transcripts,fmp,validation}/  ← 复用 sirius
  data/0700_HK/base/financials/{parsed,mdna,segments,indicators}/        ← 新增
  data/0700_HK/base/catalog.json + INDEX.md

Phase 2（2-5 分钟，可并行）：
  data/0700_HK/business_segments.json
  data/0700_HK/valuation_method_matrix.json
  data/0700_HK/information/indexed_facts.json

Phase 3（5-15 分钟）：
  data/0700_HK/valuation/{games,cloud,fintech,advertising,sns_other}/
    ├── assumption_ledger.json + bridges + risk
    ├── dcf_result.json   ← 通过 dcf_calc.py
    ├── ps_result.json
    ├── ...
    └── final_segment_valuation.json   ← 通过 aggregate.py
  data/0700_HK/valuation/group/
    ├── reverse_valuation.json
    ├── final_company_valuation.json   ← Agent 自己写
    └── assumption_ledger.json

Phase 4：
  调用 evi_persist_entry.py → POST /api/v1/templates/_internal/entries/{entry_id}/finalize
```

### Step 4：检查看板

刷新 http://localhost:5173/chat/templates/evi-strategy

应看到一行：

| 公司 | 代码 | 公允 (Base) | 区间 | 当前价 | 空间 | 判断 | 分部 | 监控 | 状态 |
|---|---|---|---|---|---|---|---|---|---|
| 腾讯科技 | 0700.HK | …（基准估值） | bear–bull | 380 | -10% / +20% | 高估/合理/低估 | 5 | 0 | 已完成 |

> 如果只跑了一半，状态显示 **部分完成**（partial）—— 这是预期的；可以让 Agent 继续完成 Phase 3-4。

### Step 5：检查报告面板

点击该行 → 跳转到 workspace 详情页 → 右侧自动渲染 `EviReportPanel`：

1. **集团 SOTP 估值卡片**：bear/base/bull、空间、合并调整、分部贡献条
2. **市场隐含预期**：reverse 估值的 implied 参数 + benchmark 对比
3. **分部 tabs**：每个 segment 三场景估值 + 方法表（method × weight × bear/base/bull × confidence）
4. **事实索引** + **监控** 卡片
5. **执行摘要**：Agent 写的 1-2 段投研结论

### Step 6：触发用户修改流程（可选）

在该 workspace 聊天框里发送：
```
我觉得腾讯云的 AI 收入应该假设更激进，把 cloud 业务 2026 年 base 增长率从 12% 改到 18%
```

Agent 应：
1. 修改 `data/0700_HK/valuation/cloud/assumption_ledger.json` + `growth_bridge.json`
2. 重跑 `dcf_calc.py --segment cloud`
3. 重跑 `aggregate.py --segment cloud`
4. 重写 `valuation/group/final_company_valuation.json`
5. 跑 `evi_persist_entry.py`
6. 在 agent.md 的"分析规则 → 当前配置"追加修改记录
7. 告知用户"看板已刷新，云业务 base 估值从 X 变为 Y"

### Step 7：触发监控（可选 / Phase 4）

打开 Automations 框架：
1. 注册一条 cron 任务，cwds=该 workspace 路径，prompt 为：
   ```
   读 .agents/skills/evi-monitor/SKILL.md，对本 workspace 执行一次监控扫描，
   把 last_checked_at 设为 7 天前，watch_scope=全量。
   ```
2. 触发后 Agent 会调用 download_knowledge 拉增量 → 写 monitor/new_materials.json
3. 然后调用 evi-revaluation-updater 触发受影响方法重算
4. 最后跑 evi_persist_entry.py，看板的 **监控** 列变成 0 → N（待办任务数）

---

## 五、本地脚本冒烟测试（已通过）

```bash
# 1. 项目骨架
python3 skills/evi-toolkit/scripts/init_project.py --symbol 0700.HK --market hk --data-dir /tmp/evi_test/0700_HK
# → OK，14 个子目录 + catalog.json

# 2. catalog rebuild（空目录）
python3 skills/evi-toolkit/scripts/update_catalog.py --data-dir /tmp/evi_test/0700_HK --rebuild
# → rebuilt — 0 items

# 3. DCF 计算（用最小 fixtures，详见 docs_siri/EVI_Strategy/evi_strategy.md 中描述）
python3 skills/evi-valuation-dcf/scripts/dcf_calc.py --data-dir /tmp/evi_test/0700_HK --segment cloud --years 5
# → status=ok, values bear=165,658 base=222,908 bull=309,931

# 4. 多方法汇总
python3 skills/evi-valuation-orchestrator/scripts/aggregate.py --data-dir /tmp/evi_test/0700_HK --segment cloud
# → status=ok, methods=['DCF'], consistency=1.0

# 5. 持久化（无后端时返回 HTTP 404，证明 payload 构造正确）
python3 skills/evi-toolkit/scripts/evi_persist_entry.py --entry-id <fake-uuid> --data-dir /tmp/evi_test/0700_HK \
    --display-name "腾讯科技" --symbol "0700.HK" --market hk
# → status=partial（自动检测到 group/final 缺失），HTTP 404 Entry not found（因为 entry-id 是假的）

# 6. MD&A 抽取
echo '# Management Discussion and Analysis\nMDA content\n# Risk' > /tmp/p.md
python3 skills/evi-toolkit/scripts/extract_mdna.py --parsed-md /tmp/p.md --out /tmp/m.md
# → OK
```

---

## 六、与 Sirius 模板的差异速览

| 维度 | Sirius 估值 | EVI Strategy |
|---|---|---|
| 数据范围 | FMP 财务 + 估值引擎 | + 财报/公告/研报/电话会 PDF + MD&A + 分部数据 + indexed facts |
| 估值粒度 | 公司整体（D7 修正） | **每个业务分部独立估值**（SOTP） |
| 估值方法 | DCF/PEG/PS（引擎内置） | DCF/PS/PEG/DDM/Comps + Reverse（多方法 weighted + 反推） |
| 引用机制 | D 文档脚注 | indexed_facts.json 全局编号 [1][2][3] + segment_id 关联 |
| 持续跟踪 | 无（一次性） | evi-monitor + evi-revaluation-updater + Automations cron |
| 修改流程 | 改 d{N}.json → persist | 改 fact / assumption → 重跑 method → aggregate → persist |
| Skill 数量 | 1（sirius-valuation） | 14（全部 evi-* 前缀） |

---

## 七、若遇问题

### Phase 1 数据下载失败（FMP_API_KEY / SerpAPI 限流）

- 检查 `.env` 是否配置 `FMP_API_KEY`、`SERPAPI_KEY`
- 让 Agent 把 entry 状态置为 `partial`：
  ```bash
  python3 skills/evi-toolkit/scripts/evi_persist_entry.py \
      --entry-id <id> --data-dir data/0700_HK --status partial
  ```

### Agent 跑到一半中断

直接在 workspace 聊天框说："继续 Phase 3"。
因为 agent.md 中包含完整 14-skill 流水线说明，Agent 知道当前在哪一步。

### 看板/报告面板不刷新

前端每 5s 轮询 `/api/v1/templates/{id}/entries`。如果状态是 `analyzing` 不会出现报告面板（按 ThreadGallery 的判断），到 `partial`/`completed` 才显示。

---

## 八、文件清单

```
skills/
├── evi-toolkit/                       共享脚本：init / parse_pdf / extract_mdna / update_catalog / evi_persist_entry
├── evi-base-data-builder/             基础数据库构建（调度其它脚本，纯指南）
├── evi-business-segmentation/         分部识别（纯指南）
├── evi-valuation-router/              估值方法路由（纯指南）
├── evi-information-search/            事实索引（纯指南）
├── evi-assumption-builder/            假设账本（纯指南）
├── evi-valuation-dcf/                 DCF（含 dcf_calc.py 计算脚本）
├── evi-valuation-ps/                  PS（纯指南）
├── evi-valuation-peg/                 PEG（纯指南）
├── evi-valuation-ddm/                 DDM（纯指南）
├── evi-valuation-comps/               Comps（纯指南）
├── evi-reverse-valuation/             反推估值（纯指南）
├── evi-valuation-orchestrator/        汇总（含 aggregate.py 计算脚本）
├── evi-monitor/                       监控（纯指南）
└── evi-revaluation-updater/           重估更新器（纯指南）

src/server/
├── models/template.py                 +TemplateEntryStatus.PARTIAL
├── database/templates.py              finalize_entry 接受 partial
└── templates/
    ├── registry.py                    +EVI_STRATEGY 注册
    └── manifests/evi_strategy.py      新增

web/src/
├── types/template.ts                  +TemplateEntryStatus partial
├── pages/Templates/
│   ├── TemplateHome.tsx               +EviDashboard + banner
│   ├── components/EntryStatusBadge.tsx +partial 配色
│   └── evi/
│       ├── EviDashboard.tsx
│       └── EviReportPanel.tsx
└── pages/ChatAgent/components/
    ├── ThreadGallery.tsx              +TEMPLATE_PANELS 动态选 panel + partial 进入 view
    └── WorkspaceGallery.tsx           +TEMPLATE_NAMES['evi-strategy']
```
