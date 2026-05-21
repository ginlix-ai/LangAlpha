# LangAlpha 深度分析流程拆解

> 以"速腾聚创 (RoboSense, HK:2498) 深度分析"为例，逐步拆解 LangAlpha 从接收用户请求到输出完整研究报告的全流程，精确到代码模块级别。

---

## 一、概述：一次深度分析的完整生命周期

当用户在 Chat 界面输入 **"对速腾聚创进行深度分析"** 时，LangAlpha 会经历以下阶段：

```
用户消息 → API路由 → 工作流构建 → Agent推理循环 → 代码执行(数据获取+分析+图表) → SSE流式输出 → 前端渲染
```

**关键特征**：Agent 不是简单地调用几个 API 拼接结果——它**写 Python 代码**在云沙箱中执行，代码通过 MCP 协议调用金融数据服务，然后用 pandas/matplotlib 做分析和可视化。这就是 PTC (Programmatic Tool Calling) 模式。

---

## 二、阶段一：API 入口 → 工作流启动

### 2.1 HTTP 请求处理

**代码位置**：`src/server/app/threads.py`

```python
@router.post("/messages")
async def send_new_thread_message(request: ChatRequest, ...):
    thread_id = str(uuid4())
    return await _handle_send_message(request, auth, thread_id, raw_request)
```

`_handle_send_message()` 做以下事情：

| 步骤 | 代码逻辑 | 说明 |
|------|---------|------|
| 1 | `resolve_llm_config(user, request)` | 解析用户选择的模型（Claude/GPT/Gemini）、BYOK 密钥 |
| 2 | `enforce_credit_limit(user_id)` | 信用额度检查 |
| 3 | `workspace_id` 解析 | PTC 模式必须绑定 workspace（=一个 Daytona 云沙箱） |
| 4 | 路由到 `astream_ptc_workflow()` | 返回 `StreamingResponse(media_type="text/event-stream")` |

### 2.2 PTC 工作流启动

**代码位置**：`src/server/handlers/chat/ptc_workflow.py`

```python
async def astream_ptc_workflow(request, thread_id, user_input, ...):
    # 1. 确保 workspace 沙箱 session 就绪
    session = await workspace_manager.get_session_for_workspace(workspace_id)
    
    # 2. 构建 Agent Graph（核心！）
    graph = await build_ptc_graph_with_session(session, config, ...)
    
    # 3. 构建输入状态
    input_state = {"messages": messages, "current_agent": "ptc"}
    
    # 4. 启动流式工作流
    handler = WorkflowStreamHandler(thread_id, user_id, ...)
    async for sse_event in handler.stream_workflow(graph, input_state, config):
        yield sse_event
```

### 2.3 Workspace Session 初始化

**代码位置**：`src/ptc_agent/core/sandbox/` + `src/ptc_agent/core/mcp/`

每个 workspace 对应一个 **Daytona 云沙箱**，session 包含：
- `sandbox` — Daytona SDK 连接实例，提供文件系统和代码执行能力
- `mcp_registry` — 已启动的 MCP 服务器注册表（10+ 金融数据服务器）
- `tool_modules` — 自动生成的 Python 模块（已上传到沙箱 `/home/daytona/.mcp/`）

```
Session 结构：
├── sandbox (Daytona)
│   ├── /home/daytona/workspace/  ← Agent 工作目录
│   └── /home/daytona/.mcp/       ← MCP 客户端代码 + 工具模块
├── mcp_registry
│   ├── price_data (stdio)
│   ├── fundamentals (stdio)
│   ├── yf_analysis (stdio)
│   ├── yf_fundamentals (stdio)
│   └── ... (共9个启用的MCP服务器)
└── tool_modules (dict[server_name → module_code])
```

---

## 三、阶段二：Agent Graph 构建

### 3.1 Graph 组装

**代码位置**：`src/ptc_agent/agent/graph.py`

```python
async def build_ptc_graph_with_session(session, config, ...):
    # 并行初始化
    user_profile, ptc_agent = await asyncio.gather(
        get_user_profile_for_prompt(user_id),
        asyncio.to_thread(PTCAgent, agent_config),
    )
    
    # 创建完整 agent（含所有中间件）
    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        ...
    )
    return inner_agent
```

### 3.2 PTCAgent.create_agent() — 组件装配

**代码位置**：`src/ptc_agent/agent/agent.py`

这是系统的核心工厂方法，组装所有组件：

#### A. 工具注册

```python
tools = [
    # === 核心执行工具 ===
    create_execute_code_tool(sandbox, mcp_registry, thread_id),  # 沙箱代码执行
    create_bash_tool(sandbox),                                    # Bash 命令
    
    # === 文件系统工具 ===
    create_read_tool(sandbox),        # 读文件
    create_write_tool(sandbox),       # 写文件
    create_edit_tool(sandbox),        # 编辑文件
    create_glob_tool(sandbox),        # 文件搜索
    create_grep_tool(sandbox),        # 内容搜索
    
    # === Web 工具 ===
    WebSearchTool(),                  # DuckDuckGo/Tavily 搜索
    WebFetchTool(),                   # 网页抓取
    
    # === 金融直接工具（不经过沙箱） ===
    get_company_overview,             # 公司概况
    get_stock_daily_prices,           # 日K数据
    get_market_indices,               # 市场指数
    get_options_chain,                # 期权链
    screen_stocks,                    # 股票筛选
    get_sec_filing,                   # SEC 文件
    get_sector_performance,           # 板块表现
    
    # === 可视化 ===
    ShowWidgetTool(),                 # 内联 HTML 可视化组件
    TodoWriteTool(),                  # 任务进度管理
    
    # === 子 Agent 工具 ===
    TaskTool(),                       # 启动子 agent
    TaskOutputTool(),                 # 获取子 agent 结果
]
```

#### B. 中间件链（~25 层）

```python
middleware = [
    # --- 输入预处理 ---
    ToolArgumentParsingMiddleware,    # JSON参数解析容错
    ProtectedPathsMiddleware,         # 禁止写入 .agents/ 等保护路径
    
    # --- 错误处理 ---
    ToolErrorHandlingMiddleware,      # 工具执行异常包装
    LeakDetectionMiddleware,          # 检测并阻止 API key 泄露
    
    # --- 输出处理 ---
    FileArtifactMiddleware,           # 文件写入 → artifact 事件
    TodoArtifactMiddleware,           # Todo 更新 → 前端同步
    
    # --- 多模态 ---
    MultimodalMiddleware,             # 图片/PDF 附件处理
    
    # --- Agent 行为控制 ---
    SkillsMiddleware,                 # 动态技能加载（如 sirius-valuation）
    SteeringMiddleware,               # 用户实时干预/纠偏
    BackgroundSubagentMiddleware,     # 后台并行子任务协调
    HITLMiddleware,                   # Human-in-the-Loop 中断/恢复
    
    # --- 上下文管理 ---
    CompactionMiddleware,             # Context window 压缩（防溢出）
    MemoryContextMiddleware,          # 注入长期记忆 memory.md
    MemoAwarenessMiddleware,          # 告知 agent 有哪些 memo 文件可查
    
    # --- 模型管理 ---
    ModelRetryMiddleware,             # 失败重试 + 模型降级回退
    PromptCachingMiddleware,          # Anthropic 提示缓存优化
    
    # --- 上下文注入 ---
    WorkspaceContextMiddleware,       # 注入 workspace 文件结构
]
```

#### C. System Prompt 渲染

**代码位置**：`src/ptc_agent/agent/prompts/templates/system.md.j2`

Jinja2 模板动态组装系统提示，告诉 Agent：
- 你是金融研究 Agent
- 你可以用 `ExecuteCode` 写 Python 代码执行
- MCP 工具模块在 `/home/daytona/.mcp/` 下，直接 import 使用
- 输出要有引用、图表、结构化分析
- 如何使用 `ShowWidget` 生成可视化

#### D. 最终包装

```python
agent = create_agent(model, system_prompt, tools, middleware, checkpointer, store)
return BackgroundSubagentOrchestrator(agent)  # 支持并行子任务
```

---

## 四、阶段三：Agent 推理循环（对速腾聚创的分析）

Agent 收到 "对速腾聚创进行深度分析" 后，进入 **ReAct 循环**（Reasoning + Acting）：

### 4.1 思考阶段（LLM 推理）

Agent 的系统提示中有 `<task_workflow>` 组件，指导它按以下流程分析：

```
1. 理解用户意图 → 深度分析 = 公司概况 + 财务分析 + 行业对比 + 估值 + 风险
2. 制定研究计划（TodoWrite）
3. 分步执行数据收集和分析
4. 生成可视化图表
5. 综合输出结论
```

### 4.2 典型执行步骤

以下是 Agent 实际会执行的步骤序列（模拟）：

---

#### Step 1: 研究计划

Agent 调用 `TodoWrite`：
```json
{
  "todos": [
    {"id": "1", "content": "获取速腾聚创基本信息和业务概况", "status": "in_progress"},
    {"id": "2", "content": "分析财务报表（收入、利润、现金流）", "status": "pending"},
    {"id": "3", "content": "行业对比与竞争格局分析", "status": "pending"},
    {"id": "4", "content": "估值分析（DCF + 相对估值）", "status": "pending"},
    {"id": "5", "content": "风险评估与投资建议", "status": "pending"}
  ]
}
```

---

#### Step 2: 数据获取 — 通过 ExecuteCode 调用 MCP

Agent 调用 `ExecuteCode` 工具，写一段 Python 代码：

```python
# Agent 生成的代码 —— 在 Daytona 沙箱中执行
import sys
sys.path.insert(0, '/home/daytona/.mcp')

# 导入自动生成的 MCP 工具模块
from yf_fundamentals import get_financial_statements, get_company_info
from yf_analysis import get_analyst_recommendations, get_institutional_holders
from price_data import get_stock_data

# 1. 获取公司基本信息
info = get_company_info(symbol="2498.HK")
print("=== 公司信息 ===")
print(f"公司名称: {info.get('longName', 'N/A')}")
print(f"行业: {info.get('industry', 'N/A')}")
print(f"市值: {info.get('marketCap', 'N/A')}")
print(f"员工数: {info.get('fullTimeEmployees', 'N/A')}")

# 2. 获取财务报表
financials = get_financial_statements(symbol="2498.HK", statement_type="income", period="annual")
print("\n=== 收入表 ===")
print(financials)

# 3. 获取股价数据
prices = get_stock_data(symbol="2498.HK", interval="daily", outputsize=365)
print("\n=== 近一年股价 ===")
print(f"数据点数: {len(prices) if prices else 0}")
```

---

#### Step 3: ExecuteCode 的内部执行流程

**代码位置**：`src/ptc_agent/agent/tools/code_execution.py`

```python
@tool("ExecuteCode")
async def execute_code(code: str, description: str = None) -> str:
    # 1. 安全检查 — 禁止直接操作 memory/memo 路径
    if any(pattern in code for pattern in PROTECTED_PATTERNS):
        return "ERROR: Cannot directly access memory/memo paths..."
    
    # 2. 在 Daytona 沙箱中执行
    result = await backend.aexecute_code(code, thread_id=thread_id)
    
    # 3. 格式化返回
    if result.exit_code == 0:
        return f"SUCCESS\n{result.stdout}\nFiles created: {result.artifacts}"
    else:
        return f"ERROR\n{result.stderr or result.stdout}"
```

**沙箱执行的底层**（`src/ptc_agent/core/sandbox/providers/`）：
- Daytona SDK 的 `workspace.execute()` API
- 代码写入临时文件 → `python /tmp/exec_{uuid}.py`
- stdout/stderr 捕获返回
- 文件系统变更检测（新生成的文件列为 artifacts）

---

#### Step 4: MCP 工具调用的底层机制

**代码位置**：`src/ptc_agent/core/tool_generator.py`

当沙箱中的代码执行 `from yf_fundamentals import get_financial_statements` 时：

1. **模块文件**在沙箱初始化时已经生成并上传到 `/home/daytona/.mcp/yf_fundamentals.py`

2. 模块内容是 `ToolFunctionGenerator` 自动生成的：

```python
# Auto-generated: yf_fundamentals.py (沙箱内)
from mcp_client import _call_mcp_tool

def get_financial_statements(symbol: str, statement_type: str = "income", period: str = "annual"):
    """Get financial statements for a given stock symbol.
    
    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "2498.HK")
        statement_type: Type of statement (income/balance/cash/all)
        period: Period type (annual/quarterly)
    
    Returns:
        dict: Financial statement data
    """
    arguments = {"symbol": symbol, "statement_type": statement_type, "period": period}
    arguments = {k: v for k, v in arguments.items() if v is not None}
    return _call_mcp_tool("yf_fundamentals", "get_financial_statements", arguments)
```

3. `_call_mcp_tool()` 通过 **stdio JSON-RPC** 与 MCP server 进程通信：

```python
# Auto-generated: mcp_client.py (沙箱内)
def _call_mcp_tool_stdio(server_name, tool_name, arguments):
    process = _get_or_start_server(server_name)  # 复用已启动的进程
    request = {
        "jsonrpc": "2.0",
        "id": _next_id(),
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments}
    }
    with _server_locks[server_name]:
        process.stdin.write(json.dumps(request) + "\n")
        process.stdin.flush()
        response = json.loads(process.stdout.readline())
    return _unpack_mcp_response(response)
```

4. **MCP Server 端**（`mcp_servers/yf_fundamentals_mcp_server.py`）处理请求，调用 Yahoo Finance API，返回数据。

---

#### Step 5: 数据分析与可视化

Agent 拿到原始数据后，再次调用 `ExecuteCode` 进行分析和绘图：

```python
# Agent 生成的分析代码
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# 构建收入趋势数据
revenue_data = {
    '2021': 3.07, '2022': 5.30, '2023': 11.21, '2024': 20.15  # 亿元
}
df = pd.DataFrame(list(revenue_data.items()), columns=['Year', 'Revenue'])
df['Growth'] = df['Revenue'].pct_change() * 100

# 绘制收入增长图
fig, ax1 = plt.subplots(figsize=(10, 6))
ax1.bar(df['Year'], df['Revenue'], color='steelblue', alpha=0.7, label='Revenue (亿元)')
ax1.set_xlabel('Year')
ax1.set_ylabel('Revenue (亿元)', color='steelblue')

ax2 = ax1.twinx()
ax2.plot(df['Year'][1:], df['Growth'][1:], color='red', marker='o', label='YoY Growth %')
ax2.set_ylabel('Growth %', color='red')

plt.title('速腾聚创 (2498.HK) Revenue Growth')
plt.tight_layout()
plt.savefig('/home/daytona/workspace/robosense_revenue.png', dpi=150)
print("Chart saved: robosense_revenue.png")
```

生成的图片文件会被 **FileArtifactMiddleware** 捕获，作为 `artifact` SSE 事件发送到前端。

---

#### Step 6: ShowWidget 内联可视化

对于交互式图表，Agent 使用 `ShowWidget` 工具：

```python
# Agent 调用 ShowWidget 生成内联 HTML 图表
show_widget(
    title="速腾聚创估值对比",
    html="""
    <div id="chart"></div>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
    new Chart(document.getElementById('chart'), {
        type: 'bar',
        data: {
            labels: ['RoboSense', 'Hesai', 'Innovusion', 'Luminar'],
            datasets: [{
                label: 'P/S Ratio',
                data: [8.5, 12.3, 6.7, 15.2],
                backgroundColor: ['#4e79a7', '#f28e2b', '#e15759', '#76b7b2']
            }]
        }
    });
    </script>
    """
)
```

---

## 五、阶段四：SSE 流式输出

### 5.1 事件流处理器

**代码位置**：`src/server/handlers/streaming_handler.py`

`WorkflowStreamHandler` 订阅 LangGraph 的三种事件流：

```python
class WorkflowStreamHandler:
    async def stream_workflow(self, graph, input_state, config):
        async for event in graph.astream(input_state, config, stream_mode=["messages", "updates", "custom"]):
            # 解析事件类型并格式化为 SSE
            sse_events = self._process_event(event)
            for sse in sse_events:
                yield self._format_sse_event(sse)
```

### 5.2 SSE 事件类型

| 事件类型 | 触发时机 | 数据内容 |
|---------|---------|---------|
| `message_chunk` | LLM 生成文本时 | `{"content": "...", "agent": "ptc"}` |
| `reasoning_start` / `reasoning_chunk` | 推理链展示 | 思维过程片段 |
| `tool_calls` | Agent 决定调用工具 | `{"name": "ExecuteCode", "args": {...}}` |
| `tool_call_result` | 工具返回结果 | `{"name": "ExecuteCode", "result": "SUCCESS..."}` |
| `artifact` | 生成文件/图表 | `{"type": "file", "path": "...", "mime": "image/png"}` |
| `todo_update` | 研究进度更新 | `{"todos": [...]}` |
| `widget` | ShowWidget 输出 | `{"title": "...", "html": "..."}` |
| `credit_usage` | 工作流结束 | `{"tokens": ..., "credits": ...}` |
| `done` | 完成 | `{}` |

### 5.3 前端渲染

**代码位置**：`web/src/pages/ChatAgent/`

前端通过 `fetch()` + `ReadableStream` 消费 SSE：

```javascript
const response = await fetch('/api/v1/threads/{id}/messages', {
    method: 'POST',
    body: JSON.stringify({ content: "对速腾聚创进行深度分析" }),
    headers: { 'Authorization': `Bearer ${token}` }
});

const reader = response.body.getReader();
// 逐行解析 SSE 事件，分类渲染：
// - message_chunk → Markdown 流式显示
// - artifact → 图片/文件卡片
// - widget → iframe 嵌入
// - todo_update → 进度条
```

---

## 六、阶段五：关键中间件在分析过程中的作用

### 6.1 CompactionMiddleware — 防止 Context 溢出

**代码位置**：`src/ptc_agent/agent/middleware/compaction.py`

深度分析过程中，Agent 可能执行 10+ 次工具调用，每次返回大量数据。Compaction 中间件监控 token 用量：

```
if total_tokens > model_context_window * 0.75:
    # 压缩早期消息为摘要
    compressed = await llm.summarize(early_messages)
    state.messages = [compressed] + recent_messages
```

### 6.2 ModelRetryMiddleware — 模型故障自动切换

**代码位置**：`src/ptc_agent/agent/middleware/model_retry.py`

如果主模型（如 Claude Sonnet）超时或返回错误：
```
重试策略: Claude Sonnet → retry(3次) → 降级到 GPT-4o → retry(2次) → 降级到 Gemini Pro
```

### 6.3 MemoryContextMiddleware — 记住用户偏好

**代码位置**：`src/ptc_agent/agent/middleware/memory.py`

在每次模型调用前注入 `memory.md`，包含：
- 用户研究偏好（如"倾向关注现金流和增长"）
- 之前的分析历史（如"上次分析过禾赛科技"）
- workspace 级记忆（如"该 workspace 专注于激光雷达行业"）

### 6.4 SteeringMiddleware — 用户实时干预

**代码位置**：`src/ptc_agent/agent/middleware/steering.py`

用户可以在 Agent 分析过程中发送"转向"消息（如"重点分析海外市场"），通过 Redis 队列注入：

```python
# 每次 LLM 调用前检查
messages = await redis.lrange(f"workflow:steering:{thread_id}", 0, -1)
if messages:
    inject HumanMessage("[Steering from User]\n重点分析海外市场")
```

### 6.5 FileArtifactMiddleware — 文件产物追踪

**代码位置**：`src/ptc_agent/agent/middleware/artifacts.py`

监控 `Write` 和 `ExecuteCode` 工具的输出，检测新文件：
- `.png` / `.jpg` → `artifact` 事件（前端显示图片）
- `.csv` / `.xlsx` → `artifact` 事件（前端提供下载）
- `.html` → 可能触发 `widget` 事件

---

## 七、MCP 金融数据服务器详解

### 7.1 可用数据源

| MCP Server | 文件 | 能力 |
|------------|------|------|
| `price_data` | `mcp_servers/price_data_mcp_server.py` | OHLCV数据（1s~daily），多资产（股票/商品/加密/外汇），做空数据 |
| `fundamentals` | `mcp_servers/fundamentals_mcp_server.py` | 财报、比率、增长、估值(DCF/EV)、内部交易、高管、技术指标 |
| `macro` | `mcp_servers/macro_mcp_server.py` | GDP、CPI、失业率、国债、盈利日历 |
| `options` | `mcp_servers/options_mcp_server.py` | 期权链、历史价格、实时快照 |
| `yf_price` | `mcp_servers/yf_price_mcp_server.py` | Yahoo Finance 价格（覆盖港股！） |
| `yf_fundamentals` | `mcp_servers/yf_fundamentals_mcp_server.py` | Yahoo Finance 财报/盈利/公司信息 |
| `yf_analysis` | `mcp_servers/yf_analysis_mcp_server.py` | 分析师评级、机构持仓、内部人士、ESG |
| `yf_market` | `mcp_servers/yf_market_mcp_server.py` | 市场状态、筛选器、板块日历 |
| `scrapling` | （独立）| 带反爬虫绕过的网页抓取 |
| `x_api` | `mcp_servers/x_mcp_server.py` | Twitter/X 搜索（舆情分析） |

### 7.2 对于速腾聚创（港股 2498.HK）的数据获取路径

```
速腾聚创分析所需数据 → 数据源映射：

公司概况         → yf_fundamentals.get_company_info("2498.HK")
财务报表         → yf_fundamentals.get_financial_statements("2498.HK")
                  + fundamentals.get_financial_statements("2498.HK")
分析师评级       → yf_analysis.get_analyst_recommendations("2498.HK")
机构持仓         → yf_analysis.get_institutional_holders("2498.HK")
股价历史         → yf_price.get_price_history("2498.HK") 或 price_data.get_stock_data("2498.HK")
行业新闻         → yf_analysis.get_news("2498.HK") + web_search("速腾聚创 最新动态")
竞品对比         → 对 Hesai(HSAI), Luminar(LAZR) 等重复上述调用
宏观环境         → macro.get_treasury_yield() + macro.get_economic_indicator("GDP")
社交媒体舆情     → x_api.search_posts("RoboSense OR 速腾聚创")
深度网页信息     → scrapling.scrape(url) / web_fetch(url)
```

---

## 八、Prompt 系统 — 指导 Agent 的行为

### 8.1 模板结构

**代码位置**：`src/ptc_agent/agent/prompts/templates/`

```
templates/
├── system.md.j2               ← 主系统提示（组合下面的组件）
└── components/
    ├── task_workflow.md.j2     ← 任务执行工作流指导
    ├── tool_guide.md.j2       ← 工具使用三层分类
    ├── data_processing.md.j2  ← 数据处理最佳实践
    ├── visualizations.md.j2   ← 可视化指南（matplotlib/plotly/HTML）
    ├── output_guidelines.md.j2← 输出格式要求
    ├── citation_rules.md.j2   ← 引用规则
    ├── memory.md.j2           ← 记忆系统使用指南
    ├── memo.md.j2             ← Memo 文件访问指南
    ├── security_policy.md.j2  ← 安全策略
    ├── subagent_coordination.md.j2 ← 子agent协调规则
    ├── workspace_context.md.j2← workspace 文件结构注入
    └── custom_skills.md.j2    ← 自定义技能使用
```

### 8.2 核心 Prompt 告诉 Agent 什么

**System Prompt 关键片段**：

```markdown
You are LangAlpha Agent, created by Ginlix AI. 
You are an investment research agent specializing in financial analysis, 
market research, and trading strategy.

## Tool Architecture (Three Tiers)

1. **Core Tools (Direct)** — Read/Write/Edit/Glob/Grep/Bash/ExecuteCode/WebSearch/WebFetch
2. **Financial Tools (Direct)** — get_company_overview/get_stock_daily_prices/...
3. **MCP Tools (via ExecuteCode)** — 在沙箱中 import 使用
   - 路径: /home/daytona/.mcp/
   - 用法: from {server_name} import {function_name}

## Visualization Guidelines
- 使用 matplotlib 生成 PNG (保存到 workspace)
- 使用 ShowWidget 生成交互式 HTML 内联图表
- 数据表格用 pandas DataFrame → markdown

## Output Requirements
- 结构化 Markdown 输出
- 数据来源引用
- 风险提示
```

---

## 九、子 Agent 系统

### 9.1 结构

**代码位置**：`src/ptc_agent/agent/subagents/`

对于特别复杂的深度分析，主 Agent 可以通过 `Task` 工具启动**子 Agent**进行并行研究：

```python
# 主 Agent 可以这样拆分任务：
task(prompt="分析速腾聚创的财务状况，重点关注收入增长和现金流")  # 子agent 1
task(prompt="调研激光雷达行业竞争格局和市场规模")                 # 子agent 2
task(prompt="搜索速腾聚创最近的新闻和公告")                       # 子agent 3
```

子 Agent 类型：
- `general-purpose` — 通用研究助手
- `research` — 深度研究专用（有 web_search + web_fetch 能力）

### 9.2 BackgroundSubagentOrchestrator

**代码位置**：`src/ptc_agent/agent/orchestrator.py`

包装主 Agent，管理并行子任务：
- 子 Agent 运行在独立的 asyncio task 中
- 主 Agent 通过 `TaskOutput` 工具轮询子 Agent 结果
- 所有子 Agent 共享同一个 sandbox session

---

## 十、完整数据流图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         用户: "对速腾聚创进行深度分析"                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  FastAPI Router (src/server/app/threads.py)                              │
│  _handle_send_message() → astream_ptc_workflow()                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PTC Workflow (src/server/handlers/chat/ptc_workflow.py)                  │
│  1. WorkspaceManager.get_session()                                       │
│  2. build_ptc_graph_with_session()                                       │
│  3. WorkflowStreamHandler.stream_workflow()                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LangGraph Agent Loop                                                    │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Middleware Stack (25 layers)                                     │    │
│  │  ┌─────────────────────────────────────────────────────────┐    │    │
│  │  │  LLM (Claude/GPT/Gemini)                                 │    │    │
│  │  │  思考 → 决定调用工具 → 获取结果 → 继续思考...              │    │    │
│  │  └─────────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                          │                    │                           │
│            ┌─────────────┘                    └──────────────┐           │
│            ▼                                                  ▼           │
│  ┌──────────────────┐                          ┌──────────────────────┐ │
│  │ Direct Tools     │                          │ ExecuteCode          │ │
│  │ - WebSearch      │                          │ (Daytona Sandbox)    │ │
│  │ - get_company_.. │                          │                      │ │
│  │ - ShowWidget     │                          │ Python Code:         │ │
│  └──────────────────┘                          │ ├─ import mcp_tools  │ │
│                                                │ ├─ 数据获取           │ │
│                                                │ ├─ pandas 分析        │ │
│                                                │ ├─ matplotlib 绘图   │ │
│                                                │ └─ 保存结果文件       │ │
│                                                └──────────┬───────────┘ │
│                                                           │              │
│                                                           ▼              │
│                                                ┌──────────────────────┐ │
│                                                │ MCP Servers (stdio)  │ │
│                                                │ ├─ yf_fundamentals   │ │
│                                                │ ├─ yf_analysis       │ │
│                                                │ ├─ price_data        │ │
│                                                │ ├─ fundamentals      │ │
│                                                │ ├─ macro             │ │
│                                                │ └─ x_api             │ │
│                                                └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (SSE Events)
┌─────────────────────────────────────────────────────────────────────────┐
│  WorkflowStreamHandler (src/server/handlers/streaming_handler.py)        │
│  message_chunk | tool_calls | tool_call_result | artifact | widget | ... │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Frontend (web/src/pages/ChatAgent/)                                     │
│  ├─ Markdown 流式渲染 (message_chunk)                                   │
│  ├─ 代码块折叠显示 (tool_calls/tool_call_result)                         │
│  ├─ 图表/图片卡片 (artifact)                                            │
│  ├─ 交互式图表 iframe (widget)                                           │
│  └─ 研究进度条 (todo_update)                                             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 十一、关键设计决策总结

| 设计决策 | 为什么这样做 | 代码位置 |
|---------|-------------|---------|
| PTC 模式（写代码而非直接调JSON工具） | 1) 不撑爆 context（数据在沙箱内处理）<br>2) 可做复杂多步分析<br>3) 自然支持图表生成 | `src/ptc_agent/agent/tools/code_execution.py` |
| MCP 协议 | 1) 标准化数据源接口<br>2) 进程隔离<br>3) 可独立扩展新数据源 | `mcp_servers/*.py` + `src/ptc_agent/core/tool_generator.py` |
| 中间件栈 | Agent 行为可组合、可测试、可插拔 | `src/ptc_agent/agent/middleware/` |
| Daytona 云沙箱 | 1) 安全隔离<br>2) 持久化文件系统<br>3) 可预装环境 | `src/ptc_agent/core/sandbox/` |
| SSE 流式输出 | 1) 实时反馈<br>2) 支持断线重连<br>3) 多类型事件分类渲染 | `src/server/handlers/streaming_handler.py` |
| 子 Agent 并行 | 加速深度研究（同时搜索+分析+对比） | `src/ptc_agent/agent/subagents/` |
| Compaction 压缩 | 深度分析 turn 数多，必须防止 context 溢出 | `src/ptc_agent/agent/middleware/compaction.py` |

---

## 十二、如果你要修改/扩展研究能力

| 想做什么 | 改哪里 |
|---------|-------|
| 新增一个数据源（如 Wind） | 1) 写 `mcp_servers/wind_mcp_server.py`<br>2) 在 `agent_config.yaml` 注册<br>3) 重启 session 自动生成模块 |
| 修改研究流程模板 | `src/ptc_agent/agent/prompts/templates/components/task_workflow.md.j2` |
| 添加新的直接工具 | `src/tools/` 写工具 → `src/ptc_agent/agent/agent.py` 注册 |
| 自定义研究 Skill | `skills/` 目录写 skill manifest + prompt |
| 修改输出格式 | `src/ptc_agent/agent/prompts/templates/components/output_guidelines.md.j2` |
| 增加新的可视化类型 | `components/visualizations.md.j2` + `ShowWidget` 模板 |

---

## 附录：速腾聚创深度分析的预期输出结构

```markdown
# 速腾聚创 (2498.HK) 深度研究报告

## 1. 公司概况
- 主营业务：激光雷达（LiDAR）研发制造
- 核心产品线：机械式/MEMS/Flash LiDAR
- 下游应用：自动驾驶、ADAS、机器人、智慧城市

## 2. 财务分析
[收入增长图表 — artifact: revenue_growth.png]
- 收入 CAGR 分析
- 毛利率趋势
- 研发投入占比
- 现金流分析

## 3. 行业竞争格局
[竞品对比表格 — widget: 交互式对比图]
- vs 禾赛科技 (HSAI)
- vs Luminar (LAZR)
- vs Innovusion
- 市场份额演变

## 4. 估值分析
- DCF 模型（多情景）
- P/S 相对估值
- 与同业对比

## 5. 风险因素
- 技术路线风险
- 客户集中度
- 竞争加剧
- 宏观环境

## 6. 投资建议
[综合评分卡 — widget]
```
