# Template 系统设计文档

## 1. 概述

Template 系统是 LangAlpha 的**上层应用框架**——在底层 Agent + Workspace 基础设施之上，提供结构化的分析流程、专属 Dashboard、持续跟踪能力。

一个 Template = 一套完整的 AI 分析方法论（如"EVI 估值策略"、"Sirius 快速估值"），每次实例化产生一个 Entry（如"腾讯科技 0700.HK"），1:1 绑定一个 Workspace。

```
┌─ Template System ────────────────────────────────────────────────┐
│                                                                   │
│  Template（方法论定义）                                            │
│    ├── Manifest（公开信息：名称/描述/字段/版本）                    │
│    ├── initial_prompt_template（首次分析的提示词）                  │
│    ├── agent_md_template（注入工作区的 agent.md）                  │
│    ├── release_notes（版本更新纪要 + 建议操作）                    │
│    ├── params_enricher（参数扩展器）                                │
│    ├── seed_files_builder（初始种子文件）                           │
│    ├── allowed_skill_names（sandbox skill 白名单）                  │
│    └── finalize_spec（产物检查 + 自动持久化）                       │
│                                                                   │
│  Entry（实例化后的分析任务）                                       │
│    ├── 1:1 Workspace（独立沙盒）                                  │
│    ├── Skills（按白名单 sync 到沙盒的 .agents/skills/）           │
│    ├── Reports + facets.json（分析产物）                           │
│    └── params._agent_md_version（版本追踪）                        │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

---

## 2. 架构组件

### 2.1 后端

| 文件 | 职责 |
|---|---|
| `src/server/templates/registry.py` | TemplateDefinition / FinalizeSpec / FinalizeExpected 类定义 + 全局注册表 |
| `src/server/templates/manifests/{id}.py` | 各模板的具体定义（prompt / agent.md / release_notes / 白名单 / finalize 清单） |
| `src/server/templates/finalize/runner.py` | **通用** finalize 执行器（扫产物 / 占位 / 调持久化 / 缺件回喂 Agent） |
| `src/server/services/template_orchestrator.py` | 生命周期管理（创建/运行/升级/重跑/删除），含 finalize hook |
| `src/server/app/templates.py` | REST API 路由 |
| `src/server/database/templates.py` | DB 操作层 |
| `src/server/models/template.py` | Pydantic 模型 |

### 2.2 前端

| 文件 | 职责 |
|---|---|
| `web/src/pages/Templates/TemplateHome.tsx` | 模板首页（所有模板入口） |
| `web/src/pages/Templates/{id}/` | 各模板的专属 Dashboard + Report Panel |
| `web/src/types/template.ts` | TypeScript 类型定义 |

### 2.3 Skills（沙盒侧）

| 目录 | 职责 |
|---|---|
| `skills/{template_id}-*/SKILL.md` | Skill 指南（Agent 在沙盒里 Read） |
| `skills/{template_id}-*/scripts/*.py` | 自动化脚本（Agent 在沙盒里 Bash 执行） |

---

## 3. TemplateDefinition 字段说明

```python
@dataclass(frozen=True)
class TemplateDefinition:
    manifest: TemplateManifest           # 公开元信息
    initial_prompt_template: str         # 首次分析的 user message
    agent_md_template: str | None        # 注入沙盒的 agent.md
    workspace_name_builder: Callable     # 工作区命名规则
    params_enricher: Callable            # 从用户输入派生更多变量
    seed_files_builder: Callable         # 初始种子文件（如 memory.md）
    release_notes: dict[str, dict]       # 版本更新纪要
    allowed_skill_names: set[str] | None # sandbox skill 白名单（None = 全量）
    finalize_spec: FinalizeSpec | None   # 产物检查 + 自动持久化（None = 不接入）
```

### 3.1 manifest

```python
TemplateManifest(
    id="evi-strategy",           # 稳定标识符（不可改）
    name="EVI 估值策略",          # 人类可读名
    description="...",           # 卡片描述
    icon="layers",               # lucide-react 图标名
    version="3.1.0",             # 当前版本（SemVer）
    estimated_minutes=25,        # 预估耗时
    fields=[...]                 # 实例化表单字段
)
```

### 3.2 release_notes（版本升级机制）

```python
release_notes={
    "3.1.0": {
        "summary": "一句话说明",
        "changes": [              # 变更列表
            "变更 1",
            "变更 2",
        ],
        "suggested_actions": [    # 建议操作（前端渲染为按钮）
            {
                "label": "按钮文字",
                "prompt": "点击后自动发给 Agent 的消息",
            },
        ],
    },
}
```

### 3.3 allowed_skill_names（sandbox skill 白名单）

**作用**：控制本模板的 workspace sandbox 里只装哪些 skill，避免通用 skill 与模板专属 skill 冲突。

```python
allowed_skill_names = {
    # 模板自有 skill
    "evi-data-orchestrator",
    "evi-valuation-analysis",
    # ...
    # 不冲突的通用 skill（按需挑选）
    "pdf", "docx", "xlsx",          # 文件处理
    "automation",                   # 注册定时任务
    "competitive-analysis",         # 互补能力
}
```

**语义**：

| 值 | 行为 |
|---|---|
| `None`（默认） | 上传所有本地 `./skills/*`（与普通 chat workspace 一致） |
| `set[str]` | 只上传名字命中白名单的 skill；之前装过的非白名单 skill 会被自动 prune |

**生效点**：`WorkspaceManager` 创建 / 恢复 workspace 时调 `PtcSandbox._upload_skills(allowed_skill_names=...)`。

**冲突判断原则（写白名单时）**：

- 模板自有的 skill **全部**保留
- **与自有 skill 实现完全重合**的通用 skill → 排除（典型：模板有 `evi-valuation-dcf` 就排除通用 `dcf-model`）
- **能力互补、不重合**的通用 skill → 保留（典型：`pdf` / `xlsx` / `competitive-analysis` / `automation`）
- 拿不准时倾向保留——白名单的目的是消除"两个 skill 干同一件事"的冲突，不是缩小工具箱

### 3.4 finalize_spec（产物检查 + 自动持久化）

**作用**：声明 "agent 跑完后必须有哪些产物 + 怎么把结果推到 DB"，由框架层 `finalize/runner.py` 自动执行。

```python
finalize_spec = FinalizeSpec(
    # 1. 解析 sandbox 内数据根目录
    data_dir_builder=lambda key, name, params: f"data/{params['symbol_dir']}",

    # 2. 预期产物清单
    expected_files=(
        FinalizeExpected(
            rel_path="facets.json",
            level="required",
            description="看板核心数据，必须有 fair_value / current_price 字段",
        ),
        FinalizeExpected(
            rel_path="reports/quality.md",
            level="optional",
            description="定性分析报告",
            placeholder="# 定性分析\n\n> 本次未生成。\n",
        ),
        # ...
    ),

    # 3. 持久化脚本（sandbox 内路径）
    persist_script=".agents/skills/evi-toolkit/scripts/persist_evi_report.py",

    # 4. 可选：persist 额外参数
    persist_args_builder=lambda key, name, params: ["--symbol", key, "--market", params.get("market", "")],

    # 5. 缺 required 时最多让 Agent 再跑几轮
    max_retries=1,
)
```

**执行时机**：在 `TemplateOrchestrator._run_agent` 的 agent generator drain 完之后自动调用 —— 模板侧 prompt / SKILL.md 都**不需要**告诉 Agent 调持久化脚本。

**runner 的决策**：

| 扫描结果 | 行为 | DB 最终状态 |
|---|---|---|
| 全部 required 齐 + optional 齐 | 调 persist_script，status=`completed` | `completed` |
| 全部 required 齐 + 部分 optional 缺 | 自动写 placeholder，调 persist_script，status=`partial` | `partial` |
| 任一 required 缺 | **不调** persist；生成结构化提示文本（列出缺什么、怎么补），通过**同一 thread_id** 作为下一条 user message 喂回 Agent，最多 retry `max_retries` 次 | 视下一轮结果而定 |
| Retry 用尽仍缺 required | 不写 DB，外层 `_run_agent_safe` 的 fallback 把 entry 置 `failed`（带错误描述） | `failed` |

每次扫描都会在 `reports/changelog.md` 顶部追加一行审计记录（如果 changelog 存在）。

**给 Agent 的提示文本格式**（runner 自动组装）：

```
[FINALIZE_CHECK] 你的任务还**没有真正完成**。数据目录 data/0700_HK 缺少以下 required 产物：

- **facets.json** — 看板核心数据，必须有 fair_value / current_price 字段
- **reports/final.md** — 最终估值结论报告

以下 optional 产物也缺（不阻塞但建议补）：
- reports/quality.md — 定性分析报告

**下一步**：
1. 逐项生成上述缺失的 required 文件
2. 完成后不需要手动调持久化脚本，系统会自动再次检查并 finalize
3. 如果某个文件因为外部原因无法生成，请在 changelog.md 写明并 Write 一个最小占位
```

**与单独脚本（如 wakeup_check.py）的关系**：单独脚本只是同一份清单的**本地副本**，用于开发者手动 dry-run / 应急补救。正常流程下框架自动接管，模板里**不应该**再要求 Agent 显式调脚本（否则一旦 Agent 漏调就会卡 analyzing）。

---

## 4. 生命周期

### 4.1 创建 Entry

```
用户填写表单 → POST /templates/{id}/entries
  ↓
orchestrator.instantiate()
  ├── 创建 Workspace（独立沙盒）
  │     └─ PtcSandbox._upload_skills(allowed_skill_names=template.allowed_skill_names)
  │        只装白名单内的 skill，prune 之前装的非白名单 skill
  ├── INSERT template_entries（params 含 _agent_md_version）
  ├── 写入 agent.md + seed files 到沙盒
  └── 后台启动 Agent（发 initial_prompt）
```

### 4.2 分析执行 + 自动 finalize

```
Agent 在沙盒里执行：
  1. Read SKILL.md（每次都读最新，sync_assets 会热更）
  2. 调用 scripts/*.py（Bash 执行）
  3. 写 reports/*.md + facets.json
        ↑
        Agent 只负责"产出"，不需要手动调持久化

Agent generator drain 完后：
  ↓
TemplateOrchestrator._run_agent 调 finalize runner
  ├── 扫 data_dir 下的 expected_files
  ├── required 齐全 → 调 persist_script → POST /_internal/entries/{id}/finalize
  │                  → DB.status = completed/partial
  └── required 缺   → 把缺件提示通过 **同一 thread_id** 作为新 user message
                      喂回 Agent（最多 retry max_retries 次）
                      → Agent 补完产物后，runner 再扫一次自动 finalize
```

**关键**：模板的 prompt / agent.md / SKILL.md 都**不应该**要求 Agent 手动调 persist 脚本，否则一旦遗漏就会出现"产物都齐了但 entry 卡在 analyzing"。框架自动接管这一步，模板只需声明 `finalize_spec`。

### 4.3 模板升级

```
开发者发布新版本（改 manifest.version + 加 release_notes）
  ↓
用户前端看到"更新到 vX.Y.Z"按钮
  ↓ 点击
POST /templates/{id}/entries/{id}/upgrade
  ├── 重新渲染 agent.md（用 entry 原始 params）
  ├── 写入沙盒（覆盖旧 agent.md）
  ├── 更新 params._agent_md_version
  └── 返回 release_notes + suggested_actions
  ↓
前端展示更新纪要 + 建议操作按钮
  ↓ 用户点击建议操作
跳转到对话框 → 自动发消息给 Agent
```

### 4.4 热更 vs 冷更

| 内容 | 更新方式 | 触发时机 |
|---|---|---|
| `skills/*/SKILL.md` | **热更**（自动 sync） | 下次对话开始时 |
| `skills/*/scripts/*.py` | **热更**（自动 sync） | 下次对话开始时 |
| `agent.md`（agent_md_template） | **冷更**（需升级） | 用户点击"更新模板" |
| `initial_prompt` | **不更新** | 只影响新 Entry |
| `seed_files`（memory.md 等） | **不更新** | 只影响新 Entry |

---

## 5. API 参考

### 5.1 Public Endpoints

| Method | Path | 说明 |
|---|---|---|
| GET | `/api/v1/templates` | 列出所有注册模板 |
| GET | `/api/v1/templates/{id}` | 获取单个模板 manifest |
| GET | `/api/v1/templates/{id}/entries` | 列出用户的 entries |
| POST | `/api/v1/templates/{id}/entries` | 创建新 entry（实例化） |
| GET | `/api/v1/templates/{id}/entries/{eid}` | 获取 entry 详情 |
| POST | `/api/v1/templates/{id}/entries/{eid}/rerun` | 重跑分析 |
| POST | `/api/v1/templates/{id}/entries/{eid}/upgrade` | 升级 agent.md |
| DELETE | `/api/v1/templates/{id}/entries/{eid}` | 删除 entry |

### 5.2 Internal Endpoints（沙盒内脚本调用）

| Method | Path | 说明 |
|---|---|---|
| POST | `/api/v1/templates/_internal/entries/{eid}/finalize` | 写入分析结果 |
| POST | `/api/v1/templates/_internal/entries/{eid}/progress` | 更新进度 |

### 5.3 Entry Response 字段

```typescript
interface TemplateEntry {
  entry_id: string;
  template_id: string;
  workspace_id: string;
  entry_key: string;
  display_name?: string;
  status: 'pending' | 'analyzing' | 'completed' | 'partial' | 'failed';
  progress: Record<string, unknown>;
  summary: Record<string, unknown>;   // 看板摘要数据
  payload: Record<string, unknown>;   // 完整分析结果
  params: Record<string, unknown>;    // 含 _agent_md_version
  // 升级追踪
  upgradable?: boolean;               // 是否有新版本可用
  current_version?: string;           // 当前 agent.md 版本
  latest_version?: string;            // 最新 manifest 版本
  created_at: string;
  updated_at: string;
}
```

---

## 6. 开发新模板指南

### Step 1：定义 Manifest

创建 `src/server/templates/manifests/my_template.py`：

```python
from src.server.models.template import TemplateField, TemplateManifest
from src.server.templates.registry import (
    TemplateDefinition, FinalizeSpec, FinalizeExpected,
)

# ① 白名单：只装本模板用得到的 skill
_MY_SKILL_WHITELIST = {
    "my-template-core",     # 模板自有
    "pdf", "xlsx",          # 通用基础设施（不冲突就放进来）
}

# ② finalize 清单：声明产物 + 持久化脚本
_MY_FINALIZE = FinalizeSpec(
    data_dir_builder=lambda key, name, params: f"data/{key}",
    expected_files=(
        FinalizeExpected(
            rel_path="result.json",
            level="required",
            description="主结果文件",
        ),
        FinalizeExpected(
            rel_path="summary.md",
            level="optional",
            description="摘要",
            placeholder="# 摘要\n\n> 本次未生成。\n",
        ),
    ),
    persist_script=".agents/skills/my-template/scripts/persist.py",
    max_retries=1,
)

MY_TEMPLATE = TemplateDefinition(
    manifest=TemplateManifest(
        id="my-template",          # 稳定 ID（kebab-case）
        name="我的模板",
        description="一句话描述",
        icon="brain",              # lucide-react icon
        version="1.0.0",
        estimated_minutes=10,
        fields=[
            TemplateField(name="company", label="公司名", type="text", required=True),
        ],
    ),
    initial_prompt_template=(
        "对 {company} 执行分析。Read `.agents/skills/my-template/SKILL.md` 按指引执行。"
        "完成后正常结束即可——系统会自动检查产物并持久化，缺件会给你发提示。"
    ),
    agent_md_template="...",       # 或 None 用默认
    allowed_skill_names=_MY_SKILL_WHITELIST,
    finalize_spec=_MY_FINALIZE,
)
```

### Step 2：注册到 Registry

在 `src/server/templates/registry.py` 底部加：

```python
from src.server.templates.manifests.my_template import MY_TEMPLATE

TEMPLATE_REGISTRY["my-template"] = MY_TEMPLATE
```

### Step 3：创建 Skills

```
skills/my-template/
├── SKILL.md           # Agent 读取的指南
└── scripts/
    ├── analyze.py     # 自动化脚本
    └── persist.py     # 写回 DB 的脚本
```

### Step 4：创建前端面板

```
web/src/pages/Templates/my-template/
├── Dashboard.tsx      # 看板（entry 卡片列表）
└── ReportPanel.tsx    # 报告面板（Tab + 报告渲染）
```

### Step 5：配置 agent_config.yaml

确保 skills 目录被包含在 sandbox sync 中。

---

## 7. 版本管理规范

### 版本号（SemVer）

- **MAJOR**（X.0.0）：不兼容的结构变更（如 reports 目录重组）
- **MINOR**（0.X.0）：新功能 + 向后兼容（如新增 skill、新增 Tab）
- **PATCH**（0.0.X）：Bug 修复（如脚本修复、prompt 微调）

### 升级时的注意事项

1. **agent.md 模板**里不要放 entry-specific 的工作内容（Thread Index / File Index 等）——这些是 Agent 自己维护的。升级时会被覆盖。
2. **release_notes 的 suggested_actions**：prompt 必须是完整可执行的指令，不需要额外上下文。
3. **MAJOR 升级**时可能需要迁移 reports 目录结构 → 在 orchestrator.upgrade_agent_md 里加迁移逻辑。
4. **params._agent_md_version** 只在升级成功后才更新（失败不写）。

---

## 8. 数据流总览

```
┌─ 创建 ─────────────────────────────────────────────────────┐
│                                                             │
│  用户填表 → API → Orchestrator → Workspace + Entry + Agent │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─ 执行 ─────────────────────────────────────────────────────┐
│                                                             │
│  Agent 在沙盒执行：                                         │
│    Read SKILL.md → Bash scripts → Write reports             │
│    → persist_entry.py → POST /_internal/finalize            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─ 展示 ─────────────────────────────────────────────────────┐
│                                                             │
│  前端读 entry.payload → Dashboard 卡片 + Report Panel       │
│  前端读 entry.upgradable → 显示/隐藏升级按钮               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─ 持续跟踪 ─────────────────────────────────────────────────┐
│                                                             │
│  用户对话 / Automation 触发 → Agent 更新 reports            │
│  → persist 刷新 payload → 前端实时反映                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─ 升级 ─────────────────────────────────────────────────────┐
│                                                             │
│  开发者发新版 → 用户看到按钮 → POST /upgrade               │
│  → 重写 agent.md → 展示 release notes → 建议操作          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 9. 当前已注册模板

| ID | 名称 | 版本 | 说明 |
|---|---|---|---|
| `evi-strategy` | EVI 估值策略 | 3.7.0 | 自适应估值 + 4 维定性分析 + 持续监控 + 重估闭环；声明了白名单（38 个 skill）+ finalize_spec（10 项产物） |

---

## 10. 模板能力分层（重要）

为避免"模板私有逻辑"和"框架通用能力"混在一起，遵循以下分层：

### 10.1 框架通用能力（住在 `src/server/templates/`）

任何模板都能复用，不绑定具体业务：

| 能力 | 位置 | 用法 |
|---|---|---|
| 模板定义类 | `registry.py` `TemplateDefinition` | 模板 manifest 文件直接 import |
| 产物清单数据类 | `registry.py` `FinalizeSpec` / `FinalizeExpected` | 模板填字段即可 |
| Finalize 执行器 | `finalize/runner.py` | 模板**不直接调用**，由 orchestrator 自动调 |
| Workspace skill 上传过滤 | `services/workspace_manager.py` + `ptc_sandbox.py` | 自动读 `allowed_skill_names` |
| Agent 重入消息注入 | `template_orchestrator._drain_one_round` | 自动循环 |

### 10.2 模板特有配置（住在 `src/server/templates/manifests/{id}.py`）

每个模板自己声明，框架按数据驱动执行：

| 配置 | 类型 | 例子 |
|---|---|---|
| 白名单成员 | `set[str]` | EVI 的 38 个 skill |
| 产物清单 | `tuple[FinalizeExpected]` | EVI 的 10 项（4 required + 6 optional） |
| `data_dir_builder` | callable | `lambda key, name, params: f"data/{params['symbol_dir']}"` |
| `persist_script` | str | `.agents/skills/evi-toolkit/scripts/persist_evi_report.py` |
| `persist_args_builder` | callable | EVI 拼 `--symbol --market` |

### 10.3 反面教材

❌ 把 finalize 清单硬编码在某个 skill 的脚本里（如旧的 `wakeup_check.py`）—— 其它模板无法复用，且 Agent 漏调就出 bug。

❌ 在 prompt / agent.md 里写"任务结束必须跑某脚本" —— 这是软约束，Agent 总有概率忘。

✅ 把清单作为数据放在模板 manifest 里，由框架自动执行 —— 不依赖 Agent 自觉。

---

## 11. 设计原则

1. **声明式优先**：能用数据描述的就不写代码（清单、字段、版本都是数据）
2. **框架做兜底，模板做配置**：例如 finalize 这种"任务结束的必经动作"必须由框架兜底，模板只声明检查规则
3. **不依赖 Agent 自觉**：凡是"必须做"的事，要么在框架层强制执行，要么有自动回喂机制让 Agent 不可能漏
4. **白名单写宽不写窄**：只排除真冲突的 skill，互补能力都留着 —— 限制 Agent 工具会让它放弃任务
5. **skill 是热更，agent.md 是冷更，prompt + seed 是一次性的**：改动时心里要清楚哪些影响存量 entry
