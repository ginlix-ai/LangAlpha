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
│    └── seed_files_builder（初始种子文件）                           │
│                                                                   │
│  Entry（实例化后的分析任务）                                       │
│    ├── 1:1 Workspace（独立沙盒）                                  │
│    ├── Skills（自动 sync 到沙盒的 .agents/skills/）               │
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
| `src/server/templates/registry.py` | TemplateDefinition 类定义 + 全局注册表 |
| `src/server/templates/manifests/{id}.py` | 各模板的具体定义（prompt / agent.md / release_notes） |
| `src/server/services/template_orchestrator.py` | 生命周期管理（创建/运行/升级/重跑/删除） |
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

---

## 4. 生命周期

### 4.1 创建 Entry

```
用户填写表单 → POST /templates/{id}/entries
  ↓
orchestrator.instantiate()
  ├── 创建 Workspace（独立沙盒）
  ├── INSERT template_entries（params 含 _agent_md_version）
  ├── 写入 agent.md + seed files 到沙盒
  └── 后台启动 Agent（发 initial_prompt）
```

### 4.2 分析执行

```
Agent 在沙盒里执行：
  1. Read SKILL.md（每次都读最新，因为 sync_assets 会热更）
  2. 调用 scripts/*.py（Bash 执行）
  3. 写 reports/*.md + facets.json
  4. 调用 persist_evi_report.py → POST /_internal/entries/{id}/finalize
```

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
from src.server.templates.registry import TemplateDefinition

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
    ),
    agent_md_template="...",       # 或 None 用默认
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
| `sirius-valuation` | Sirius 快速估值 | 1.0.0 | 单次快速估值（30分钟内） |
| `evi-strategy` | EVI 估值策略 | 3.1.0 | 自适应估值 + 持续监控 + 重估闭环 |
