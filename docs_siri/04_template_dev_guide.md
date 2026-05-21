# 模板开发指南：系统与模板的关系

> 版本：v2（重构后的解耦版本）
> 阅读对象：想开发新模板、或理解模板框架设计的人

---

## 一、核心设计原则

**系统（orchestrator / 路由 / 数据库）完全不知道任何模板的业务逻辑。**

所有和"这个模板具体干什么"有关的内容，都定义在 `TemplateDefinition` 里，由模板作者自己写。系统只负责：

1. 存一条数据库记录（`template_entries`）
2. 创建一个 workspace
3. 写 agent.md（内容来自模板定义，系统只是执行写入）
4. 启动 agent（prompt 来自模板定义，系统只是发第一条消息）
5. 提供内部 API 让 sandbox 脚本回写结果

这是"**插槽模型**"：系统预留了几个插槽，模板把内容填进来。

---

## 二、系统预留的插槽（系统层面的扩展点）

| 插槽 | 位置 | 作用 |
|---|---|---|
| `TemplateDefinition.manifest` | 表单 + 看板 | 控制 InstantiateDialog 的字段 |
| `TemplateDefinition.agent_md_template` | **system prompt** | 每次 LLM 调用都注入，控制 agent 的"长期记忆" |
| `TemplateDefinition.initial_prompt_template` | 首次 agent 调用 | 启动分析的指令 |
| `TemplateDefinition.params_enricher` | 动态 context | 从用户输入派生额外的 prompt 变量 |
| `template_entries.payload` | 数据库 → 前端看板 | sandbox 端脚本写入，前端展示 |
| 前端 `CUSTOM_DASHBOARDS[templateId]` | 自定义 UI | 每个模板的看板展示组件（lazy load） |

**`agent_md_template` 是最关键的插槽**，因为它利用了系统已有的 `WorkspaceContextMiddleware` 机制：

```
系统已有机制（不需要模板关心）：
  WorkspaceContextMiddleware
    → 每次 LLM 调用前，读 sandbox 的 agent.md
    → 注入到 system prompt 最末尾
    → agent 永远能看到最新的 workspace 上下文

模板利用这个机制：
  在 agent_md_template 里写：
    - 这个 workspace 属于哪个模板（template_id）
    - 对应的数据库记录 entry_id（agent 知道要更新哪行）
    - persist_entry.py 的完整调用命令
    - 用户可配置的"分析规则"节
```

这样 agent 在**每次对话**中都知道自己在做什么、结果往哪写——无论是初次分析还是用户来修改逻辑。

---

## 三、模板文件结构（一个模板包含什么）

```
src/server/templates/manifests/your_template.py   ← 后端：模板全部定义
web/src/templates/your_template/                  ← 前端：看板 UI
  ├── Dashboard.tsx      ← 自定义看板组件（接收 entries[]）
  └── schema.ts          ← payload 类型定义（可选）
skills/your-skill/                                 ← 可复用的 skill（可选）
  └── scripts/persist_entry.py                     ← sandbox 端回写脚本
```

**没有额外的数据库表**：通用的 `template_entries` 表（JSONB `payload`）对所有模板都够用。

---

## 四、TemplateDefinition 完整字段说明

```python
@dataclass(frozen=True)
class TemplateDefinition:
    manifest: TemplateManifest          # 表单字段 / 描述 / 预计时间
    initial_prompt_template: str        # agent 第一条消息，{entry_id} 等占位
    agent_md_template: str | None       # workspace agent.md 内容，None 用默认空模板
    workspace_name_builder: Callable    # 工作区名字（可选，默认 display_name）
    params_enricher: Callable           # 派生额外 prompt 变量（可选）
```

**占位符规则**（prompt 和 agent.md 共用同一套 context）：

| 占位符 | 来源 | 示例 |
|---|---|---|
| `{entry_key}` | 用户填写（或 auto_ 派生） | `0700.HK` |
| `{display_name}` | 用户填写 | `腾讯科技` |
| `{workspace_name}` | `workspace_name_builder` 结果 | `腾讯科技（0700.HK）` |
| `{template_name}` | `manifest.name` | `Sirius 估值` |
| `{entry_id}` | 系统注入（entry 创建后） | `020781a3-...` |
| `{symbol_dir}` | 系统注入（由 entry_key 派生） | `0700_HK` |
| `{market}` | 用户填写（params） | `hk` |
| `{market_label}` | `params_enricher` 派生 | `港股 (HK)` |
| `{fetch_step}` | `params_enricher` 派生 | （根据有无股票代码变化） |

---

## 五、开发一个新模板的完整流程

### 第一步：创建后端 manifest

新建 `src/server/templates/manifests/your_template.py`：

```python
from src.server.templates.registry import TemplateDefinition
from src.server.models.template import TemplateManifest, TemplateField

_PROMPT = """
请分析 {display_name}，...
完成后运行：
  python3 .agents/skills/your-skill/scripts/persist_entry.py \\
    --entry-id {entry_id} \\
    --data-dir .agents/skills/your-skill/data/{symbol_dir}
"""

_AGENT_MD = """
---
workspace_name: {workspace_name}
description: [Your Template] {entry_key}
template_id: your-template
entry_id: {entry_id}
entry_key: {entry_key}
---

# {workspace_name} — {template_name}

## 模板说明
...（告诉 agent 如何更新数据库，用户如何修改规则）

## 分析规则（用户可配置）
...

## Thread Index / Key Findings / File Index
"""

YOUR_TEMPLATE = TemplateDefinition(
    manifest=TemplateManifest(
        id="your-template",
        name="你的模板",
        description="...",
        fields=[TemplateField(name="entry_key", label="主键", type="text")],
    ),
    initial_prompt_template=_PROMPT,
    agent_md_template=_AGENT_MD,
)
```

### 第二步：注册到 registry

`src/server/templates/registry.py`：

```python
from src.server.templates.manifests.your_template import YOUR_TEMPLATE

TEMPLATE_REGISTRY: dict[str, TemplateDefinition] = {
    SIRIUS_VALUATION.id: SIRIUS_VALUATION,
    YOUR_TEMPLATE.id: YOUR_TEMPLATE,   # ← 加这行
}
```

### 第三步：写 sandbox 端回写脚本

`skills/your-skill/scripts/persist_entry.py`：

```python
# 读分析产物，POST 到 finalize 端点
# 参考 skills/sirius-valuation/scripts/persist_entry.py
```

### 第四步：前端看板 UI（可选）

`web/src/templates/your_template/Dashboard.tsx`：

```tsx
export default function YourDashboard({ templateId, entries }) {
  return <table>...</table>;  // 展示 entries[].summary 里的字段
}
```

在 `web/src/pages/Templates/TemplateHome.tsx` 注册：

```ts
const CUSTOM_DASHBOARDS = {
  'sirius-valuation': lazy(() => import('./sirius/SiriusDashboard')),
  'your-template': lazy(() => import('./your_template/Dashboard')),
};
```

### 第五步（可选）：加 skill

如果模板需要预置的 skill：

- `skills/your-skill/SKILL.md`（模板会通过 initial_prompt 触发 skill 加载）
- `skills/your-skill/scripts/fetch_data.py`（获取数据）
- `skills/your-skill/scripts/persist_entry.py`（回写 DB）

**Skill 和模板是解耦的**：模板的 initial_prompt 只是"让 agent 使用某个 skill"的自然语言指令，skill 是独立存在的。

---

## 六、从头到现在：系统与模板的演进关系

### 阶段 0：系统原有能力

LangAlpha 原有的系统能力，模板全程复用，一行未改：

| 系统能力 | 模板怎么用 |
|---|---|
| `WorkspaceManager.create_workspace()` | 每只票创建独立 workspace + sandbox |
| `WorkspaceContextMiddleware` | agent.md 注入 system prompt（模板利用这个插槽） |
| `astream_ptc_workflow()` | 运行 agent（模板的 initial_prompt 就是第一条用户消息） |
| `MemoryContextMiddleware` | 用户偏好跨对话持久化（模板 workspace 自动受益） |
| Skills 加载机制 | `/sirius-valuation` 等斜杠命令 + auto-load（模板 prompt 直接触发） |
| SSE 流式 + Redis buffer | 分析过程的实时流，看板进度展示（复用） |
| Automations 框架 | 可对模板 workspace 注册 cron（"每周更新看板数据"） |

### 阶段 1：新增的系统能力（为模板预留的槽）

| 新增 | 文件 | 是否耦合到具体模板 |
|---|---|---|
| `template_entries` 表 | `migrations/011_*.py` | ❌ 通用 JSONB，任何模板通用 |
| `src/server/database/templates.py` | CRUD 层 | ❌ 纯通用 |
| `TemplateDefinition` dataclass | `registry.py` | ❌ 描述接口，不含业务逻辑 |
| `TemplateOrchestrator` | `services/template_orchestrator.py` | ❌ 纯通用编排，不知道任何模板细节 |
| REST 路由 | `app/templates.py` | ❌ 纯通用 CRUD + finalize |
| `TemplateHome.tsx` / `TemplateMarket.tsx` | 前端路由和容器 | ❌ 纯通用，通过 `CUSTOM_DASHBOARDS` 分发 |
| `InstantiateDialog.tsx` | 通用弹窗 | ❌ 按 `manifest.fields` 动态渲染 |

### 阶段 2：具体模板实现（Sirius 估值）

| 文件 | 耦合性 | 备注 |
|---|---|---|
| `manifests/sirius_valuation.py` | ✅ Sirius 专属 | 包含 prompt/agent.md/字段/规则 |
| `skills/sirius-valuation/` | ✅ Sirius 专属 | 七维度分析 skill + 数据获取脚本 |
| `web/src/templates/sirius/` | ✅ Sirius 专属 | 看板表格 + 报告面板 |
| `SiriusReportPanel.tsx` | ✅ Sirius 专属 | D1-D7 可视化 |

---

## 七、关键设计决策（Why）

**为什么 agent.md 属于模板定义而不是系统**

最初实现里，`_seed_template_agent_md()` 在 orchestrator 里写死了 sirius 的 persist_entry 命令和规则节。这是错误的——orchestrator 成了模板的"实现"。

重构后：orchestrator 只负责"调 `template.build_agent_md()` 然后写文件"，完全不知道内容是什么。这和 initial_prompt 的处理方式完全一致。

**为什么用 JSONB 而不是每个模板建一张表**

模板的输出 schema 会演化（新字段、改字段）。用 JSONB payload 可以自由演化，不需要每次改表结构。前端展示时直接访问 `payload.dimensions.D2.metrics.moat_rating` 这样的路径。

**为什么看板数据不从 sandbox 文件读，而是写入数据库**

1. **解耦**：sandbox 可能被停止/归档，数据库始终可查
2. **历史记录**：数据库可以保留历史快照
3. **权限**：数据库数据可以跨用户/跨平台访问，sandbox 文件不能

---

## 八、一行代码的扩展体感

添加"行业研究"模板，只需要：

```
新增 3 个文件：
  src/server/templates/manifests/industry_research.py  ← ~80 行
  web/src/templates/industry_research/Dashboard.tsx    ← ~50 行
  skills/industry-research/scripts/persist_entry.py   ← 复制 sirius 改 30 行

修改 2 处：
  src/server/templates/registry.py           ← 加 1 行 import + 1 行注册
  web/src/pages/Templates/TemplateHome.tsx   ← 加 1 行 lazy import
```

系统（数据库/orchestrator/路由/基础 UI）不需要任何改动。
