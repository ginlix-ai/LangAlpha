# PLAN: 聊天回复 Suggestion 便捷按钮

## 需求概述

1. **新增设置**：suggestion 开关，控制是否在聊天回复后生成 suggestion 按钮
2. **Suggestion 按钮**：聊天回复下方显示最多 3 个 suggestion，根据回复内容总结生成
3. **点击行为**：点击 suggestion 按钮直接将其作为下一轮输入，开启下一轮聊天

---

## 实现步骤

### Step 1: 后端 — 持久化 suggestion 开关

**目标**：在 `other_preference` JSONB 中存储 `suggestion_enabled` 开关。

**涉及文件**：
- 无需新建 migration，使用已有 `user_preferences.other_preference` JSONB 列存储

**工作内容**：
- 无需 schema 变更 — `other_preference` 已是 `JSONB DEFAULT '{}'`，直接写入 `suggestion_enabled: boolean`
- 无需新增 Pydantic model — `OtherPreference` 已设 `extra="allow"`
- 无需新增 API — 已有 `PUT /api/v1/users/me/preferences` 支持 JSONB merge
- **默认值**：`suggestion_enabled` 默认为 `true`（即该 key 不存在时视为开启）。前端读取时使用 `?? true` 兜底，后端读取时对 `None`/缺省值按 `True` 处理

**验证**：
- `PUT /api/v1/users/me/preferences` 携带 `{"other_preference": {"suggestion_enabled": true}}` 能正确持久化
- 再次 `GET /api/v1/users/me/preferences` 能读到 `other_preference.suggestion_enabled`

---

### Step 2: 前端 — Settings 页面新增 suggestion 开关

**目标**：在 Settings 页面添加一个 toggle，控制 suggestion 功能的开关。

**参考模式**：`voice_input_enabled` toggle（Settings.tsx 第 517–527 行）

**涉及文件**：
- `web/src/pages/Settings/Settings.tsx`

**工作内容**：
1. 从 `usePreferences()` 读取 `other_preference?.suggestion_enabled`，默认值为 `true`（即 `suggestion_enabled ?? true`）
2. 在 Settings 页面的**模型 tab 底部**（BYOK 配置下方）添加 toggle 开关，用 `borderTop` 分割线与上方内容隔离
3. 切换时调用 `updatePrefsMutation.mutateAsync({ other_preference: { ...currentOtherPref, suggestion_enabled: !current } })`
4. 遵循已有 toggle 样式（与 voice input toggle 一致的 switch UI）
5. 新用户无此 key 时 toggle 显示为开启状态

**验证**：
- Settings 页面能看到 suggestion 开关
- 切换开关后刷新页面，状态保持

---

### Step 3: 后端 — 聊天完成后异步生成 suggestions

**目标**：在每次 assistant 回复完成后，调用 LLM 生成 1–3 条 follow-up suggestion。

**涉及文件**：
- `src/server/services/llm_service.py` — 已有 `LLMService.complete()`，开箱即用
- `src/server/handlers/chat/flash_workflow.py` — Flash 模式的 completion callback
- `src/server/handlers/chat/ptc_workflow.py` — PTC 模式的 completion callback

**工作内容**：

1. **新增 Pydantic response schema**（建议放在 `src/server/models/` 或 `src/server/services/llm_service.py` 同级）：
```python
from pydantic import BaseModel, Field

class SuggestionItem(BaseModel):
    text: str = Field(description="A concise follow-up question or suggestion, in the user's language")

class SuggestionResponse(BaseModel):
    suggestions: list[SuggestionItem] = Field(
        max_length=3,
        description="1-3 follow-up suggestions based on the assistant's last reply"
    )
```

2. **新增 suggestion 生成函数**（建议放在 `src/server/services/suggestion_service.py`）：
   - 输入：`user_id`, 最后一条 assistant 回复文本
   - 检查用户 prefs 中 `other_preference.suggestion_enabled`，缺省时默认为 `true`（即 key 不存在视为开启）
   - 若显式设置为 `false`，直接返回空
   - 调用 `LLMService.complete()` 生成 suggestions：
     - `mode="flash"`（轻量模型，节省成本）
     - `response_schema=SuggestionResponse`（结构化输出）
     - system_prompt 指引 LLM 生成简短、有针对性、延续对话的 follow-up 问题
   - 返回 `list[str]`（最多 5 条）

3. **在 completion callback 中触发 suggestion 生成**：
   - Flash mode：`flash_workflow.py` 的 `on_flash_workflow_complete()`（约第 428 行）
   - PTC mode：`ptc_workflow.py` 的 `on_background_workflow_complete()`（约第 823 行）
   - 在 `persist_completion()` 之后，调用 suggestion service
   - 将生成的 suggestions 追加写入 `conversation_responses.sse_events` JSONB 中，作为一个新的 SSE event：
     ```json
     {"event": "suggestions", "data": {"suggestions": ["...", "...", "..."]}}
     ```
   - 使用方案 B（写入 sse_events）：无需新增数据库列，无需 migration，利用已有 JSONB 列即可。前端 replay 时也能一并回放

4. **新增 API 端点**：`GET /api/v1/threads/{thread_id}/turns/{run_id}/suggestions`
   - 从 `conversation_responses.sse_events` 中提取 `event: suggestions` 的 data
   - 返回 `{"suggestions": ["...", "..."]}`
   - 若未找到 suggestion event，返回 `{"suggestions": []}`

**验证**：
- 开启 suggestion 开关后，发送一条消息，收到回复后调用 API 能获取 suggestions
- 关闭 suggestion 开关后，API 返回空列表
- 新用户（无 `suggestion_enabled` key）默认视为开启，正常生成 suggestions
- suggestions 数量 ≤ 5

---

### Step 4: 前端 — 数据模型扩展

**目标**：在 TypeScript 类型和 chat message 结构中支持 suggestions。

**涉及文件**：
- `web/src/types/chat.ts`
- `web/src/pages/ChatAgent/utils/api.ts`
- `web/src/lib/queryKeys.ts`

**工作内容**：

1. 在 `chat.ts` 的 `AssistantMessage` 接口中添加字段：
```typescript
export interface AssistantMessage {
  // ... 已有字段
  suggestions?: string[];  // 最多 5 条 follow-up suggestion
}
```

2. 在 `api.ts` 中新增 API 调用函数：
```typescript
export async function fetchSuggestions(
  threadId: string,
  runId: string
): Promise<{ suggestions: string[] }> {
  const { data } = await api.get(`/api/v1/threads/${threadId}/turns/${runId}/suggestions`);
  return data;
}
```

3. 在 `queryKeys.ts` 中添加 query key（可选，也可以用 local state）。

**验证**：
- TypeScript 编译无错误

---

### Step 5: 前端 — SSE 流结束后拉取 suggestions

**目标**：当 assistant 回复的 SSE 流结束时，自动拉取 suggestions 并更新到消息上。

**涉及文件**：
- `web/src/pages/ChatAgent/hooks/useChatMessages.ts`
- `web/src/pages/ChatAgent/hooks/utils/streamEventHandlers.ts`

**工作内容**：

1. 在 SSE 流结束处理逻辑中（`finish` 事件或 stream 自然结束），获取当前 `run_id` 和 `thread_id`
2. 检查用户 preferences 中 `suggestion_enabled`，缺省时默认为 `true`（`other_preference?.suggestion_enabled ?? true`）
3. 若开启，调用 `fetchSuggestions(threadId, runId)`
4. 将返回的 `suggestions` 写入最后一条 assistant message：
```typescript
setMessages(prev => prev.map(msg =>
  msg.id === assistantMsgId ? { ...msg, suggestions: result.suggestions } : msg
));
```

**关键细节**：
- Suggestions 只在 assistant 消息**非 streaming** 状态下显示（`isStreaming === false` 且 `suggestions.length > 0`）
- 历史消息（`isHistory === true`）不拉取 suggestions（避免大量 API 调用）
- Error 消息不显示 suggestions

**验证**：
- 发一条消息，收到完整回复后，MessageList 中的最后一条 assistant message 的 `suggestions` 字段被填充

---

### Step 6: 前端 — Suggestion 按钮 UI 组件

**目标**：在 assistant 消息下方渲染 suggestion 快捷按钮。

**参考模式**：`Dashboard/components/ChatInputCard.tsx` 的 suggestion chips（`dashboard-suggestion-bubble` CSS class）

**涉及文件**：
- `web/src/pages/ChatAgent/components/MessageList.tsx`（`MessageBubble` 内部）
- 可选：新建 `web/src/pages/ChatAgent/components/SuggestionButtons.tsx`

**工作内容**：

1. **新建 `SuggestionButtons` 组件**：
```tsx
interface SuggestionButtonsProps {
  suggestions: string[];
  onSuggestionClick: (text: string) => void;
  disabled?: boolean;  // 当正在 streaming 时禁用
}
```

2. **渲染位置**：在 `MessageBubble` 中，action buttons 行**上方**、widget deck **下方**（约 MessageList.tsx 第 799 行之后，第 807 行之前）：

```
┌─────────────────────────────┐
│  Assistant Message Content  │
│  (text, tool calls, etc.)   │
├─────────────────────────────┤
│  [Suggestion 1] [Sug 2] ... │  ← 新增
├─────────────────────────────┤
│  📋  👍  👎  🔄            │  ← 已有 action buttons
└─────────────────────────────┘
```

3. **样式**：
   - 水平排列，flex wrap
   - 使用 chip/badge 风格，圆角边框
   - Hover 时高亮
   - 使用 CSS 变量（`var(--color-*)`）保持主题一致
   - 仅在 `!isStreaming && suggestions?.length > 0` 时显示
   - 跟随 action buttons 的 hover 显示逻辑（`opacity-0 group-hover:opacity-100`），保持 UI 整洁

4. **在 `MessageList.tsx` 中集成**：
   - `MessageBubble` 接收新的 `onSuggestionClick` prop
   - `MessageList` 接收新的 `onSuggestionClick` prop 并下发给 `MessageBubble`
   - `ChatView` 传入 `onSuggestionClick` handler

**验证**：
- 收到回复后，消息下方出现 suggestion chips
- Hover 消息时 chips 可见
- Streaming 中不显示
- 历史消息不显示

---

### Step 7: 前端 — 点击 Suggestion 发送下一轮消息

**目标**：点击 suggestion 按钮直接将其作为下一条 user message 发送。

**涉及文件**：
- `web/src/pages/ChatAgent/components/ChatView.tsx`
- `web/src/pages/ChatAgent/components/MessageList.tsx`

**工作内容**：

1. 在 `ChatView.tsx` 中创建 `handleSuggestionClick`：
```typescript
const handleSuggestionClick = useCallback((text: string) => {
  handleSendMessage(text);
}, [handleSendMessage]);
```

2. 传递给 `MessageList`：
```tsx
<MessageList
  // ... 已有 props
  onSuggestionClick={handleSuggestionClick}
/>
```

3. `MessageList` → `MessageBubble` → `SuggestionButtons` 逐层传递

4. 点击后的用户体验：
   - 立即调用 `handleSendMessage(suggestionText)` 发送消息
   - 不需要先填入 ChatInput，直接发送（因为 suggestion 本身已经是完整的提问）
   - ChatInput 可选择性填入该文本（通过 `chatInputRef.current?.setValue(text)`），方便用户修改后再发送（可选增强）

**可选的交互增强**：
- 点击后给 suggestion 按钮一个短暂的 active/pressed 动画
- 发送期间禁用所有 suggestion 按钮（避免重复点击）

**验证**：
- 点击 suggestion 按钮后，自动发送对应的消息
- AI 正常回复
- 新回复的 suggestion 按钮正常出现
- 旧消息的 suggestion 按钮仍然可点击

---

### Step 8: 后端 — 将 suggestions 写入 sse_events（无需 Migration）

**目标**：利用已有 `conversation_responses.sse_events` JSONB 列存储 suggestions，避免新增数据库列。

**涉及文件**：
- `src/server/services/suggestion_service.py`（Step 3 已创建）
- `src/server/handlers/chat/flash_workflow.py`
- `src/server/handlers/chat/ptc_workflow.py`

**工作内容**：

1. 在 completion callback 中，调用 suggestion service 获取 suggestions
2. 将 suggestions 序列化为 SSE event 追加到 `sse_events` JSONB 数组：
   ```python
   suggestion_event = {
       "event": "suggestions",
       "data": {"suggestions": ["...", "...", "..."]},
   }
   ```
3. 更新 `conversation_responses` 行：`UPDATE conversation_responses SET sse_events = sse_events || $1::jsonb WHERE ...`
   - 使用 PostgreSQL `||` 操作符追加元素到 JSONB 数组末尾

**方案 B 的优势**：
- 无需 migration，无需新增数据库列
- 前端 replay 时自然能回放 suggestion event
- `sse_events` 已是 JSONB 数组，追加操作简单高效

**验证**：
- 回复完成后，`sse_events` 数组末尾包含 `{"event": "suggestions", "data": {...}}`
- 开关关闭时不会追加 suggestion event

---

### Step 9: 测试

**涉及文件**：
- `tests/unit/server/app/` — 后端 API 测试
- `web/src/pages/ChatAgent/components/__tests__/` — 前端组件测试

**工作内容**：

1. **后端测试**：
   - 测试 `suggestion_enabled` 开关的读写
   - 测试 suggestion API 端点返回正确格式
   - 测试开关关闭时 API 返回空

2. **前端测试**：
   - 测试 `SuggestionButtons` 组件渲染
   - 测试点击触发 `onSuggestionClick` 回调
   - 测试 streaming 状态下不显示
   - 测试空 suggestions 时不渲染

---

## 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `src/server/models/suggestion.py` | 新建 | SuggestionResponse Pydantic schema |
| `src/server/services/suggestion_service.py` | 新建 | Suggestion 生成逻辑 + 写入 sse_events |
| `src/server/handlers/chat/flash_workflow.py` | 修改 | completion callback 中触发 suggestion 生成并写入 sse_events |
| `src/server/handlers/chat/ptc_workflow.py` | 修改 | completion callback 中触发 suggestion 生成并写入 sse_events |
| `src/server/app/threads.py` | 修改 | 新增 `GET /{thread_id}/turns/{run_id}/suggestions` 端点，从 sse_events 提取 |
| `web/src/pages/Settings/Settings.tsx` | 修改 | 新增 suggestion_enabled toggle（默认开启） |
| `web/src/types/chat.ts` | 修改 | AssistantMessage 加 suggestions 字段 |
| `web/src/pages/ChatAgent/utils/api.ts` | 修改 | 新增 fetchSuggestions API 调用 |
| `web/src/pages/ChatAgent/components/SuggestionButtons.tsx` | 新建 | Suggestion 按钮 UI 组件 |
| `web/src/pages/ChatAgent/components/MessageList.tsx` | 修改 | 集成 SuggestionButtons，新增 onSuggestionClick prop |
| `web/src/pages/ChatAgent/components/ChatView.tsx` | 修改 | handleSuggestionClick 回调，传给 MessageList |
| `web/src/pages/ChatAgent/hooks/useChatMessages.ts` | 修改 | SSE 流结束后拉取 suggestions |
| `tests/unit/server/app/test_suggestions.py` | 新建 | 后端测试 |
| `web/src/pages/ChatAgent/components/__tests__/SuggestionButtons.test.tsx` | 新建 | 前端组件测试 |

## 数据流

```
用户发送消息
  → SSE 流返回 assistant 回复
  → finish 事件触发
  → 检查 other_preference.suggestion_enabled（缺省默认 true）
  → 若开启：调用 resolve_model_client(OAuth → BYOK → platform) 解析凭证
  → make_api_call() 生成 1-5 条 suggestion
  → 追加写入 conversation_responses.sse_events 作为 "suggestions" event
  → tracker.mark_completed() 通知前端 stream 结束
  → 前端 SSE 流结束后调用 GET .../suggestions
  → API 从 sse_events 中提取 suggestions event 的 data
  → 更新 AssistantMessage.suggestions
  → SuggestionButtons 组件渲染
  → 用户点击 → handleSendMessage(suggestionText) → 下一轮聊天
```

---

## 实际实现与计划差异

以下记录了开发过程中发现的、与原始计划不同的关键决策和踩坑记录。

### 1. 模型凭证解析：`resolve_model_client` 替代 `LLMService.complete()`

**计划**：使用 `LLMService.complete(user_id=..., mode="flash")` 走 `resolve_llm_config` 做用户级凭证解析。

**问题**：`resolve_llm_config` 内部 `_cow()` 调用 `config.model_copy(deep=True)`，在 workflow 执行后 config 被 LangGraph/中间件注入了不可 pickle 的对象（如 `RLock`），导致 `TypeError: cannot pickle '_thread.RLock' object`。

**尝试过 `create_llm()` 直接调用**：绕过 `resolve_llm_config`，但 `create_llm` 只查 env var（如 `DEEPSEEK_API_KEY`），不处理 BYOK/OAuth 凭证。BYOK 用户的 API key 存在 DB `custom_providers` 中，`create_llm` 找不到。

**最终方案**：使用 `resolve_model_client(user_id, model_name, is_byok=True, allow_platform_fallback=True)` — 这是 `resolve_llm_config` 内部的子函数，走 OAuth → BYOK → platform fallback 三级凭证发现，但不触发 `_cow()` 的 deepcopy。模型名从 `other_preference.preferred_flash_model` 读取（fallback `agent_config.llm.flash`）。

### 2. 时序问题：suggestion 生成必须在 `mark_completed` 之前

**问题**：completion callback 中 `tracker.mark_completed()` 发出完成信号 → 前端 SSE 流结束 → 调 `GET .../suggestions`。但原来 suggestion 生成在 `mark_completed` **之后**，前端调用时数据还没写入 DB。

**修复**：将 suggestion 生成 + `update_sse_events()` 移到 `tracker.mark_completed()` **之前**。

```python
# 正确顺序
persist_completion()
generate_suggestions() + update_sse_events()  # ← 先写库
tracker.mark_completed()                       # ← 再发完成信号
```

### 3. 前端获取方式：HTTP 请求 → SSE 流内推送

**计划**：SSE 流结束后前端通过 `GET /api/v1/threads/{tid}/turns/{rid}/suggestions` 拉取。

**问题**：额外 HTTP 往返导致 suggestions 出现有明显延迟（消息先渲染完，suggestions 才到）。

**修复**：改为在 completion callback 中通过 Redis Stream 推送 `suggestions` SSE 事件。前端在 `processEvent`（`createStreamEventProcessor`）中直接处理 `eventType === 'suggestions'`，将 `event.suggestions` 写入对应 assistant message。

- `suggestion_service.py` 新增 `push_suggestions_to_redis()`，用 `XADD` 写入 Redis stream
- `flash_workflow.py` / `ptc_workflow.py` 在 `update_sse_events` 后、`mark_completed` 前调用
- `useChatMessages.ts` 新增 `suggestions` event handler，删除 `fetchSuggestionsAsync` 和 HTTP 调用
- `api.ts` 删除 `fetchSuggestions()` 函数（API 端点保留，供将来 replay 使用）
- `scripts/utils/test_suggestions.py` 已删除（没用上）

**注意**：SSE 解析器会把 `data:` 行的 JSON 展开到事件顶层，取值时用 `event.suggestions` 而非 `event.data.suggestions`。

### 4. Settings toggle 位置调整

### 5. Settings toggle 位置调整

计划放在 User Info tab 的 voice input 旁边 → 实际放在**模型 tab 底部**（BYOK 配置下方，用 `borderTop` 分割线隔离）。

### 6. 数量上限调整

`SuggestionResponse.max_length` 从 3 改为 5，同步更新 service prompt 和截断逻辑。

### 7. 已知限制：页面刷新后 suggestions 消失

当前 history replay（`loadHistory` → `replayThreadHistory`）有独立的事件处理逻辑，尚未添加 `suggestions` event 的处理。页面刷新后 `sse_events` 中的 suggestion 数据虽然存在，但 replay 不会将其写入 `AssistantMessage.suggestions`，导致刷新后按钮不显示。后续可在 history replay 的事件分发（`useChatMessages.ts` 约 1621 行 `credit_usage` 附近）加入 suggestion 处理逻辑。
