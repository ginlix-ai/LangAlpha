# Bots & Rooms：在 LangAlpha 上再加两层抽象

> 状态：方案（2026-11-08）。分支 `feat/bots-rooms`。
> 参考：`roostbot/docs/design/{bots,rooms,messaging,agent-collaboration,sandboxing}.md`。
> 原则：**Workspace 概念不变**；在它上面加「Bot 身份层」和「Room 群聊层」，Thread/Turn/run 管线、checkpoint、livefs、记忆、transcript 都不动。

## 实现状态（2026-11-08，分支 `feat/bots-rooms`）

| 阶段 | 状态 | 落在哪 |
|---|---|---|
| P0 身份层 | ✅ 后端 + API + 前端 Bots 页 | 迁移 114、`database/bots.py`、`services/bots/resolver.py`、`app/bots.py`、`<bot_identity>` prompt 块、`web/src/pages/Settings/panels/BotsPanel.tsx` |
| P0 Bot 模型/人设 | ✅ | `resolve_turn_config` 用 bot 的固定模型，人设进 system prompt |
| P1 点对点消息 | ✅ | `bot_messages`/`assignments`、`services/bot_messages/{delivery,sweeper}.py`、`tools/bots`（`list_bots` / `send_bot_message`） |
| P1 自动重试 | ✅ | 每 worker 的 `BotMessageDelivery` sweeper（claim、attempt 上限、request_key 幂等） |
| P2 群聊 | ✅ 后端 + API + Rooms 页 | `database/rooms.py`、`services/rooms/{coordinator,sweeper,context,threads}.py`、`app/rooms.py`（REST + SSE）、`tools/rooms`（read_room/post_to_room/read_own_chat）、`web/src/pages/Rooms/**` |
| P3 定时任务/routine 接线 | ✅ 后端（前端待做） | `automations.bot_id`；Bot 归属的 automation 默认 `continue` 并落 Bot 私聊/既定 routine 会话；executor 以 Bot 角色路由并加 `[Scheduled task]` 框；`scripts/automations_to_persistent.py`（dry-run/apply） |
| P3 Bot 工具面 | ✅ | `tools_allow`/`tools_deny` 在构建期收窄主 agent 工具集（`tools/bots/scope.py`） |
| P3 会话可见性 | ✅ | 线程响应带 `bot_id`（聊天头部可解析名字/头像）；rooms 行有 `updated_at` 并推 `room_updated` |
| V1.1 review 修复 | ✅ | 见 §25（P0/P1/P2 修复清单与仍留 v2 的项）；含任意 Bot 私聊入口（`POST /threads {bot_id}` + Bots 页 Chat） |
| P3 待做 | ⏳ | `approval` 执行映射、`skills`/`memory_scopes` 生效、非默认 Bot 私聊、Exchange 抽屉（v2） |
| Bot 消息页面显示 | ✅ | `bot_message_source`（history + live 首帧，含 frame-stripped body）；`BotMessageRow`（ds-bot EventLine 风格：发送者行 + 正文气泡、长文折叠、无用户操作）；发出方向工具行 "Messaging X · kind" / "Messaged X · status" |
| Bot 头像（BotMark） | ✅ | `lib/botMark.ts`（FNV-1a 推导 8 色 × 4 形状 + 首字；image/emoji/mark/auto）+ `components/bots/BotMark.tsx`；替换 Chat 头部、Bot 消息行、Bots 列表/编辑、房间成员/日志/轮次、自动化选择器；Bots 编辑页可编辑；`--color-bot-1..8` 双主题实色 token |

已知取舍：
- Bot 工具叫 `send_bot_message`（`send_message` 是既有聊天渠道工具）。
- `GET /api/v1/bots`：交互用户的无过滤读取会确保 Home Bot 存在，让 Bots 页在任何 Home 轮次之前就能显示 CoS；computer token 的无过滤读取不创建 Home，且只返回本机可达的 Bot。
- 一个 Bot 的私聊 thread 在首次需要时惰性创建（专用线程，不收养旧线程），钉在 `bots.primary_thread_id`。
- 平台 Bot 的工具叫 `send_bot_message`；渠道消息仍是 `send_message`。

## 1. 目标与非目标

### 目标

1. **一个会话就是一个 Bot**：`conversation_threads.bot_id` 把身份挂在会话上；同一个 Bot 的私聊、群聊、定时任务都写入它自己的会话（或它的 routine 会话），上下文连续。
2. **Bot 之间可以发消息**：点对点、可追踪（assignment），不是消息总线。
3. **群聊**：一条共享有序日志 + 一轮由成员按规则串行发言 + 每个成员只收到自己的未读增量。
4. **定时任务不再每次新开会话**：automation 归属 Bot，默认写进 Bot 的持续会话；保留 `new` 给一次性任务。
5. **不破坏现有语义**：workspace/computer/sandbox/livefs/memory/transcript/checkpoint/replay/admission 全部沿用。

### 非目标（本设计明确不做）

- 不把 Bot 做成独立进程/独立沙箱；Bot 的执行环境仍是 Workspace 绑定的那台电脑。
- 不做跨用户的团队/多租户（v2 之后再谈）。
- 不引入 Kafka/NATS 消息总线；投递 = 持久 inbox + 注入一条 user 消息。
- 不改 LangGraph 图结构、checkpoint schema、chat wire 事件类型（v1）。
- 不做 Bot 自主建群/自治讨论（`auto` responder 放 v2）。

## 2. 概念模型

```
User
 ├─ Computer                      （不变）
 │    └─ Workspace                （不变：文件夹/电脑/记忆/checkpoint 的单位）
 │         └─ Bot                 （新增：身份层）
 │              ├─ Thread  private   ← 私聊（一个会话 = 一个 Bot）
 │              ├─ Thread  routine   ← 定时任务（可选，每个 automation 一条持续会话）
 │              └─ Thread  room      ← 群聊中的成员会话（每个 (room,bot) 一条）
 └─ Room                          （新增：群聊层）
      ├─ members: Bot[] (+ User)
      ├─ log: 共享有序日志（唯一事实）
      └─ rounds: 一条用户消息 = 一轮 = 有序 slots
```

### 三层职责

| 层 | 负责 | 不负责 |
|---|---|---|
| Workspace（不变） | 文件夹、电脑、沙箱/启停/档位、文件备份、workspace 记忆、livefs | 谁在说话、人设、群聊 |
| Bot（新增） | 名字、人设、模型、skills/tools 白名单、审批级别、记忆作用域、会话归属 | 文件放哪、跑在哪台电脑 |
| Room（新增） | 共享日志、成员、responder、轮次/slots、@ 规则、公告 | 执行、上下文持久化 |

### 与现有角色的关系

- `AgentRole`（`analyst` / `chief_of_staff`）保留，成为 Bot 的属性 `bots.role`。
- 每个 Workspace 回填一个默认 Bot（role=`analyst`），每个用户回填一个 Home Bot（role=`chief_of_staff`，绑定 Home 工作区）——**存量行为不变**。
- 不指定 Bot 的旧请求：解析到 thread 的 bot；thread 没有 bot 就落到 workspace 默认 Bot。永远向后兼容。

## 3. 不变清单（实现时必须守住）

| 不动 | 说明 |
|---|---|
| `conversation_threads` / `conversation_queries` / `conversation_responses` 模型 | 只加 `bot_id` 列 |
| run 管线：admission → executor → coordinator → SSE → finalize | 完全复用 |
| LangGraph checkpoint / replay 契约 | 不动；room 上下文只注入，不进 checkpoint 的历史结构 |
| livefs / 文件备份 / workspace 记忆 / transcript | 不动 |
| `hook_outbox` 投递模式 | v1 未复用（投递走 `post_notification_turn` + DB claim/lease）；outbox 事件（`bot.updated` 等）留 v2 |
| workspace 表 | 不动（可选加 `default_bot_id`，v1 不加，用查询解析） |
| chat wire 事件类型 | v1 不新增；Bot 消息作为普通 user 消息进入会话 |

## 4. 数据模型（迁移 114）

> 表名/列名以实际 schema 为准：会话表是 `conversation_threads`，主键 `thread_id`；运行表 `conversation_responses`。

```sql
-- 4.1 Bot（身份层）
CREATE TABLE bots (
    bot_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id      UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    name              VARCHAR(64) NOT NULL,
    role              VARCHAR(32) NOT NULL DEFAULT 'analyst'
                          CHECK (role IN ('analyst','chief_of_staff')),
    persona           TEXT NOT NULL DEFAULT '',
    avatar            JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider          VARCHAR(64),
    model             VARCHAR(128),          -- NULL = 跟随会话/线程的模型（存量默认 bot）
    skills            JSONB NOT NULL DEFAULT '[]'::jsonb,
    tools_allow       JSONB NOT NULL DEFAULT '[]'::jsonb,
    tools_deny        JSONB NOT NULL DEFAULT '[]'::jsonb,
    approval          VARCHAR(16) NOT NULL DEFAULT 'ask'
                          CHECK (approval IN ('ask','edits','auto','full')),
    memory_scopes     JSONB NOT NULL DEFAULT '["workspace"]'::jsonb,
    status            VARCHAR(16) NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active','paused')),
    primary_thread_id UUID,
    created_by        VARCHAR(255) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workspace_id, name)
);
CREATE UNIQUE INDEX ix_bots_one_home ON bots (user_id)
    WHERE role = 'chief_of_staff';

CREATE TABLE bot_versions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_id     UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
    field      VARCHAR(32) NOT NULL,
    value      JSONB NOT NULL,
    actor      VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4.2 会话归属（只加一列）
ALTER TABLE conversation_threads ADD COLUMN bot_id UUID REFERENCES bots(bot_id) ON DELETE SET NULL;
CREATE INDEX ix_threads_bot ON conversation_threads (bot_id);

-- 4.3 Bot 的会话映射（私聊/room/routine）
CREATE TABLE bot_threads (
    bot_id    UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
    thread_id UUID NOT NULL REFERENCES conversation_threads(thread_id) ON DELETE CASCADE,
    kind      VARCHAR(16) NOT NULL CHECK (kind IN ('private','room','routine')),
    room_id   UUID,
    seen_seq  BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bot_id, thread_id)
);
CREATE UNIQUE INDEX ix_bot_threads_room ON bot_threads (bot_id, room_id)
    WHERE room_id IS NOT NULL;

-- 4.4 房间（群聊层）
CREATE TABLE rooms (
    room_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    name         VARCHAR(64) NOT NULL,
    host_bot_id  UUID REFERENCES bots(bot_id) ON DELETE SET NULL,
    responder    VARCHAR(64) NOT NULL DEFAULT 'everyone',  -- everyone | mentions | member:<botId>
    bulletin     TEXT NOT NULL DEFAULT '',
    status       VARCHAR(16) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','archived')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ix_rooms_name ON rooms (workspace_id, name);

CREATE TABLE room_members (
    room_id     UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    member_type VARCHAR(16) NOT NULL CHECK (member_type IN ('bot','user')),
    member_id   VARCHAR(255) NOT NULL,
    role        VARCHAR(16) NOT NULL DEFAULT 'member' CHECK (role IN ('host','member')),
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, member_type, member_id)
);

-- 共享有序日志（唯一事实；每个成员按 seen_seq 投影增量）
CREATE TABLE room_log (
    room_id    UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    seq        BIGSERIAL,
    round_id   UUID,
    sender_type VARCHAR(16) NOT NULL CHECK (sender_type IN ('bot','user','system')),
    sender_id  VARCHAR(255) NOT NULL,
    kind       VARCHAR(16) NOT NULL DEFAULT 'message'
                  CHECK (kind IN ('message','pass','status','system')),
    content    TEXT NOT NULL,
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, seq)
);

CREATE TABLE room_rounds (
    round_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    room_id      UUID NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    started_by   VARCHAR(255) NOT NULL,
    start_seq    BIGINT NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued','running','finished','failed')),
    slots        JSONB NOT NULL DEFAULT '[]'::jsonb,   -- [{i, bot_id, state, run_id}]
    slot_cursor  INTEGER NOT NULL DEFAULT 0,
    lease_owner  VARCHAR(64),
    lease_until  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ
);
CREATE UNIQUE INDEX ix_room_one_running_round ON room_rounds (room_id)
    WHERE status IN ('queued','running');

-- 4.5 Bot 间消息（点对点；不是房间多播）
CREATE TABLE bot_messages (
    message_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    from_type    VARCHAR(16) NOT NULL CHECK (from_type IN ('bot','user','system')),
    from_id      VARCHAR(255) NOT NULL,
    to_bot_id    UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
    kind         VARCHAR(16) NOT NULL DEFAULT 'message'
                    CHECK (kind IN ('message','request','reply','report','question','notice')),
    content      TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    idem         VARCHAR(128),
    status       VARCHAR(16) NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued','delivered','consumed','failed')),
    thread_id    UUID REFERENCES conversation_threads(thread_id) ON DELETE SET NULL,
    in_reply_to  UUID REFERENCES bot_messages(message_id) ON DELETE SET NULL,
    root_id      UUID,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX ix_bot_messages_idem ON bot_messages (user_id, idem) WHERE idem IS NOT NULL;
CREATE INDEX ix_bot_messages_inbox ON bot_messages (to_bot_id, status, created_at);

-- 4.6 协作状态（消息只是指针，状态在这里）
CREATE TABLE assignments (
    assignment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    from_bot_id   UUID REFERENCES bots(bot_id) ON DELETE SET NULL,
    to_bot_id     UUID NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
    thread_id     UUID REFERENCES conversation_threads(thread_id) ON DELETE SET NULL,
    status        VARCHAR(16) NOT NULL DEFAULT 'requested'
                     CHECK (status IN ('requested','working','needs_input','done','failed','cancelled')),
    outcome       VARCHAR(32),
    evidence      JSONB NOT NULL DEFAULT '[]'::jsonb,
    depth         INTEGER NOT NULL DEFAULT 0,
    root_id       UUID,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at     TIMESTAMPTZ
);

-- 4.7 定时任务与 Bot 绑定
ALTER TABLE automations ADD COLUMN bot_id UUID REFERENCES bots(bot_id) ON DELETE SET NULL;
CREATE INDEX ix_automations_bot ON automations (bot_id);
```

### 回填（迁移 114 + 修复迁移 115）

1. **Home 不回填 analyst**：Home 是派生的 Chief of Staff 的地盘，CoS 由 resolver 首次需要时创建；114 的早期版本若已回填 Home analyst，由 115 解除 thread/automation 绑定并软删该 Bot。
2. 每个非 Home 的 live Workspace：回填一个 `role='analyst'` 的 Bot（`name` 用 workspace `name`），`model=NULL`。
3. `conversation_threads.bot_id` → 所在 workspace 的默认 Bot（Home 线程保持 NULL，等 resolver 首次 turn 时绑定 CoS）。
4. `automations.bot_id` → 该 automation `workspace_id` 的默认 Bot（Home 保持 NULL）。
5. `bot_threads`：114 的早期版本的 "每条旧 thread 都是 private" 映射会被 115 清理；`ensure_private_thread` 只使用 `bots.primary_thread_id` 指向的专用线程，绝不收养用户旧聊天/共享/归档线程。
6. **不改** `automations.thread_strategy`（行为不变）；提供显式脚本 `scripts/automations_to_persistent.py --dry-run/--apply` 给运维确认后批量切到 `continue`。

## 5. Bot 身份与生命周期

- 创建：`POST /api/v1/bots`（name/role/workspace/persona/model/skills/tools/approval）。名称锁：同 workspace 唯一 + 事务内 `INSERT ... ON CONFLICT` 处理并发。
- 模型：新建 Bot 的 `model` 必须具体；`model NULL` 仅存量回填 Bot 使用（= 跟随线程的模型，保持旧行为）。
- 修改：persona/model/skills/tools/approval 走 `bot_versions` 留痕 + `bot.updated` 事件；`workspace_id` 不可改（迁移走工作区迁移逻辑）。
- 暂停：`status='paused'` → 不接新轮次（私聊发消息返回 409、room 里跳过、automation 跳过）；进行中的 run 跑完。
- 删除：软删（`status='archived'` 或 `deleted_at`），7 天可恢复；room 成员保留占位显示"已删除"。
- 内置 Bot：每用户 Home Bot（CoS）；每个 workspace 默认 Bot。删除 workspace 前要求先删/迁移 Bot。

## 6. 会话归属（一个会话 = 一个 Bot）

- `conversation_threads.bot_id` 决定这条 thread 属于谁。
- 新建会话可显式指定 Bot：`POST /api/v1/threads {workspace_id, bot_id}`（Home CoS 用 Home 的 workspace_id）；Bots 页的 Chat 按钮走这条路径。未指定时由 resolver 取该位置的默认 Bot。
- 三种线程：
  - `private`：Bot 私聊（用户 ↔ Bot）；一个 Bot 默认一条（`bots.primary_thread_id`，首次发消息时创建）。
  - `routine`：定时任务的持续会话；`automations.thread_strategy='continue'` 时钉住的就是它；也可多条（每个 automation 一条）。
  - `room`：群聊成员会话，每个 `(room, bot)` 一条，记在 `bot_threads`。
- **请求路由**：`turn_route` 扩展为先解析 Bot（请求显式 `bot_id` → thread.bot_id → workspace 默认 Bot），再由 Bot 给出 `role` 和 `workspace_id`；解析结果与现有 `TurnRoute` 同构，下游零改动。
- **Bot 消息注入**：Bot 发给 Bot 的消息进入目标 Bot 的会话，作为一条 user 角色消息，正文第一块是发送者框（见 §7.1）。

## 7. Bot 间消息（点对点）

### 7.1 注入形态（必须照做的结论）

```
[Message from <bot name>]              ← 发送者框，进正文，模型可见
<body>
```
- 元数据（from_bot、kind、idem、assignment_id）放 `conversation_queries.metadata` / `conversation_responses.metadata`，模型不可见。
- 服务端字段（from、origin）只允许服务端写：`_client_body()` 清洗客户端输入，投递走 service token。
- **展示**：投递回合带 `bot_message_source`（sender id/name、`message_kind`、去掉发送者框的 `body`），history（query 行 metadata）与 live（SSE 首帧 metadata）都有；Chat 页把这条 user 行画成 ds-bot 风格的 `BotMessageRow`（发送者头像/名字 + 正文气泡），发出方向显示为工具活动 "Messaging X · kind" / "Messaged X · delivered"。客户端不能伪造该字段。

### 7.2 投递语义

| 目标状态 | `now`（默认，纠错/补充） | `queue`（新任务） |
|---|---|---|
| 空闲 | 启动一次 run | 启动一次 run（按 inbox 顺序合并） |
| 在跑 | **steering**（复用 `wait_or_steer`） | 写 inbox，等当前 run 结束后的下一轮 |

- `send_message` 返回 `{message_id}` = 已接受；"已执行"请看 assignment/report。
- 顺序：同一 `from→to` 保序（按 `created_at, message_id`）。
- 去重：`idem` 唯一索引；重复返回原 message_id。
- 跨重启：inbox 在 Postgres；投递 worker 用 `FOR UPDATE SKIP LOCKED` 领取，状态 CAS。

### 7.3 防环与预算（三道闸）

1. 上游检查：不能发给"正在等我结果"的 Bot（按 assignment 推断）。
2. 每 root 预算：`root_id` 维度最多 64 条请求 / 96 次执行（可配）。
3. 闭环检测：120s 内 A→B→C→A 三人环 → 房间/私聊禁发 5 分钟。
4. 单轮上限：每个 Bot 每轮最多 2 条（host 4）。

### 7.4 Assignment（协作状态）

```
requested → working → done / failed / cancelled
                 ↘ needs_input → working
```
- `send_message(kind='request')` 或 CoS 的 `delegate_to_analyst` 建 assignment。
- 完成必须有 `outcome` + `evidence`（文件路径、run_id、消息 id）。
- `needs_input` 停车等人/发起者回答，不超时重试。
- 所有 assignment 事件写 `hook_outbox` 通知（复用现有 outbox）。

## 8. 房间与轮次（群聊）

### 8.1 不变量

1. 房间日志只有一份（`room_log`），不进每个成员会话重复存。
2. 成员只收到"自己没读过、且不是自己说的"增量（`bot_threads.seen_seq`）。
3. 同一房间同一时刻只有一个 round；round 内 slots 串行。

### 8.2 轮次

```
用户消息 → room_log(seq) → 开 round
  ├─ responder 生成 slots：everyone（默认，host 先）| mentions（默认 lead）| member:<id>
  ├─ 逐个 slot：
  │   1. 装配该成员上下文（§9）
  │   2. 向它的 room-thread 注入消息；空闲跑 run，忙则 steer/排队（按 round 策略：默认等）
  │   3. 等结果：超时 240s，工作宽限 15s
  │   4. 分类：posted / [PASS] / failed / missed / needs_input
  │   5. 解析回复里的 @Name → 插新 slot（受 §8.4 限额）
  ├─ 轮内用户插话 → 入日志，加入队列（不打断当前成员）
  └─ 轮结束 → room_log 写状态行 + SSE 推送
```

### 8.3 并发的实现

- `room_rounds` 上的 **partial unique index** 保证一房一轮。
- 协调器（`services/rooms/coordinator.py`）：
  - 启动 round：`INSERT ... ON CONFLICT DO NOTHING`（输者读现有 round）。
  - 推进 slot：`UPDATE room_rounds SET slot_cursor=..., lease_owner/lease_until ... WHERE round_id=? AND slot_cursor=? AND lease_owner IS NULL OR lease_until < now()`，CAS 成功者执行。
  - 恢复：扫描器（每 worker）领取 `lease_until` 过期且 `status='running'` 的 round，从 `slot_cursor` 继续；slot 内 run 用 (round_id, slot_index) 幂等键防重复。
- **禁止**进程内存注册表（LangAlpha 多 worker 硬规则）。

### 8.4 参数（初始默认，取自 roostbot 实测）

| 项 | 实现值（`services/rooms/coordinator.py`） |
|---|---|
| 单 slot 等待 | 300s 超时；投递等待 120s（线程忙时排队） |
| 每成员每轮发言 | 2（host 4） |
| 每轮额外 turn（@ 追加） | 4 |
| 房间成员上限 | 12 个 Bot（用户是主持人，不占 slot） |
| 上下文窗口 | 更早 ≤8 条、本轮增量 ≤24 条、单条 ≤1,500 字 |
| 公告 | 2,000 字符 |
| `[PASS]` | 不写日志、不算失败，只推进已读游标 |
| 失败/超时/跳过 | 写一条 system status 行显式呈现 |
| 日志保留 | v1 不裁剪（`room_log` 全量；轮次只读增量） |

## 9. 上下文装配

### 9.1 Bot 私聊（每次 run）

静态 system prompt 增加 `<bot>` 块（来自 `bots` 行，按 (bot, model) 缓存）：

```
<bot>
名字 / 角色 / 人设（persona）
你的工作区：<workspace 名>；你的计算机：<computer 标签>
</bot>
```

发送者框在 user 消息里（§7.1）。`persona` 变更走版本表 + 事件，不用重启。

### 9.2 Room 成员（每次 slot，注入为 `<system-reminder>` 包裹的消息）

复用 `inject_inline_reminders()` 通道，顺序固定：

1. 成员清单 + 谁是 host
2. 群公告（bulletin）
3. 更早上下文（本轮之前最多 8 条）
4. 本轮增量（自己没读过、非自己说的，最多 24 条）
5. 用户引用的会话/文件
6. 谁还在工作
7. 本轮发言顺序 + 自己是第几个
8. 行为指令：一次一条短消息；没有新信息回 `[PASS]`

### 9.3 关键约束

- **room 上下文以 `<system-reminder>` 标记**：进入该成员本轮 synthetic turn 的消息（因此会随 checkpoint 投影），但被标记为上下文而非"用户原话"；成员随后用 `post_to_room` 把答复写回房间。
- 私聊上下文不进群：成员要让群看到自己私聊内容，必须通过工具读取（`read_own_chat`），房间记录"它查了私聊"。
- 大内容不搬运：消息是"指针"，大文件写工作区，消息里给路径 + 摘要；单条上限 64 KB。

## 10. 定时任务与 Wakeup

### 10.1 现状（已验证）

- `automations.thread_strategy`：`new`（每次新会话）/ `continue`（首次创建并钉住会话）。
- 线上：44 个 `new`、7 个 `continue`。
- 复用会话且该线程在跑时，firing 进入 `waiting` 排队（`_wait_for_thread`），不插队。

### 10.2 改造

1. `automations.bot_id` 必填（回填）；`_turn_place()` 先解析 Bot → workspace/thread。
2. Bot 归属的 automation 默认 `thread_strategy='continue'`（API 只接受 `new`/`continue`）。
3. 会话目标（默认 **bot_primary**，可被 automation 覆盖）：
   - `bot_primary`：写进 Bot 私聊（默认；一个会话 = 一个 Bot）
   - `routine`：每个 automation 一条持续会话（`continue` 现状；日报/扫描互不污染）
   - `new`：一次性任务（保留）
4. 触发消息加框：`[Scheduled task: <automation name>]`，作为 user 消息注入；run 的 metadata 记 `automation_id`/`execution_id`。
5. 失败回退：钉住的会话被删/移走时，新建一条并重新钉住；新会话必须仍属于同一 Bot（`bot_id` 过滤）。`bot.thread_fallback` 事件留 v2。
6. Wakeup：保持 thread 级（已经是对的）；wake 的 turn 会自然带上 thread 的 `bot_id`。

### 10.3 存量切换

- 不自动改 `new`（避免静默改变行为）；提供 `scripts/automations_to_persistent.py --dry-run`：
  - 列出候选（按 `new` + 近 30 天有运行 + 非一次性标记）
  - 人工确认后批量 `PATCH {"thread_strategy":"continue"}`
- UI 的「更多选项 → 持续会话 / 每轮新会话」保留。

## 11. Prompt 与工具面

### 11.1 新增 agent 工具（主 agent，按角色/权限开放）

| 工具 | 作用 |
|---|---|
| `list_bots` | 列出同 workspace 的 Bot（名字/角色/状态） |
| `send_bot_message(to, content, kind)` | 给另一个 Bot 发消息；`kind=request` 时建 assignment。**不能叫 `send_message`**：那是发给聊天渠道（微信等）的既有工具（`src/tools/messaging/`），CoS 已绑定 |
| `report_to(assignment_id, outcome, evidence)` | 完成/失败上报，并给发起方发一条 framed report |
| `list_assignments` | 自己参与的 assignment 及状态 |
| `read_room` / `post_to_room` | 房间读写（成员在轮次之外查看/留言；轮次的 `@Name` 追加由协调器处理） |
| `create_bot(name, persona?, model?, skills?, tools_allow?, tools_deny?, approval?)` | 在本 workspace 建一个 analyst Bot；Home 内拒绝，重名/超上限拒绝，模型在创建前校验 |
| `create_room(name, members, responder?, bulletin?, host?)` | 拉本 workspace 的 Bot 建群（成员 1–12、必须 active、host 默认第一个）；用户在群里发消息才开始 round |
| `list_rooms` | 列出本 workspace 的房间（名称、room_id、成员、responder），建群前查重用 |
| `read_own_chat` | 让群里能引用自己的私聊（记录到 room log） |

- 工具描述遵循 `src/tools/AGENTS.md`（每句话改变决策）。
- 工具白名单来自 `bots.tools_allow/deny`，在构建 agent 时过滤（`tools/bots/scope.py`）。
- 防环预算（v1.1）：链深 6、每 root 64 条、每 Bot 每分钟 8 条；`BOT_MESSAGING_ENABLED=0` 是部署级 kill switch（关闭新消息，已在投递中的消息照常完成）。
- 可观测性：`langalpha.bots.messages{status,kind}`、`langalpha.rooms.rounds{status}`、`langalpha.rooms.round.slots{state}`、`langalpha.rooms.round.duration_ms`；运维修复/保留脚本 `scripts/bots_rooms_maintenance.py`（默认 dry-run）。
- **审批级别 v1 语义**：`approval` 入库并在 Bots 页展示；当前分析 Bot 的可用工具里没有会触发 HITL 卡的写操作（下单/交易是独立能力组），所以 v1 不扩展自动审批，`auto/full` 不会绕过任何现有卡片。等 v2 交易/写操作进入 Bot 工具面时再映射到 `StandingApprovalMiddleware`。

### 11.2 模型解析

- Bot 的 `model` 非空 → 优先；空（存量 Bot）→ 线程模型。
- Bot 的模型不可用（下线/无权）→ 拒绝并提示，不静默回退（与线程模型策略一致）。

## 12. API

```
# Bots
GET    /api/v1/bots?workspace_id=
POST   /api/v1/bots
GET    /api/v1/bots/{bot_id}
PATCH  /api/v1/bots/{bot_id}
DELETE /api/v1/bots/{bot_id}            # 软删
POST   /api/v1/threads {workspace_id, bot_id?}  # 用户与指定 Bot 开对话（v1 实际入口；thread 出生即绑定该 Bot）
# 兼容别名 POST /api/v1/bots/{bot_id}/messages 未实现（v2）

# Rooms
GET    /api/v1/rooms?workspace_id=&limit=100&offset=0
POST   /api/v1/rooms
GET    /api/v1/rooms/{room_id}
PATCH  /api/v1/rooms/{room_id}          # name/responder/bulletin
POST   /api/v1/rooms/{room_id}/members  # {member_type, member_id}
DELETE /api/v1/rooms/{room_id}/members/{member_type}/{member_id}
POST   /api/v1/rooms/{room_id}/messages # {content, request_key?} 用户发一句 → 开 round（幂等）
GET    /api/v1/rooms/{room_id}/stream?last_seq=   # SSE：room_message / round_status / room_updated
GET    /api/v1/rooms/{room_id}/log?since_seq=&limit=

# Automations（现有接口扩展）
PATCH  /api/v1/automations/{id}         # 接受 {bot_id, thread_strategy, conversation_thread_id}
```

所有 room 事件走**独立 SSE**，不混入 chat wire（v1）。

## 13. 流式与重放

| 通道 | 来源 | 重放来源 |
|---|---|---|
| Bot 私聊 / routine / room 成员 run | 现有 `workflow:stream:{thread}:{run}`（不变） | checkpoint（不变） |
| Room 页面 | table-poll SSE（每 1.2s 查 `room_log`/`room_rounds`，keepalive 20s） | `room_log` 表（table-sourced） |

- Room 事件（已实现）：`room_message`（每条日志）、`round_status`（轮次状态、slots、slot_cursor，每个 slot 推进时发）、`room_updated`（room 行变更，前端据此重取详情）。`slot_*` 的逐条事件 v1 折叠进 `round_status`。
- 重连：客户端带 `last_seq`，服务端从 `room_log` 回放后继续轮询；无 Redis Stream。
- **不新增 chat wire 事件类型**（避免 `test_event_ledger.py` 的 replay home 债务）；如果未来要在私聊里渲染 room 卡片，必须先登记表和重放来源。

## 14. 多 worker 与失败恢复

- 协调器：PARTIAL UNIQUE + CAS + lease（§8.3）；恢复扫描器每 worker 都跑，幂等。
- 投递 worker：`bot_messages` `FOR UPDATE SKIP LOCKED` + 状态 CAS；`idem` 去重。
- 房间成员 run 的终态：协调器轮询 run 终态后写 room_log；幂等由 `(round_id, metadata.slot)` 唯一索引保证（不是 finalize hook）。
- 服务重启：running round 由扫描器接管；`conversation_responses` 的 `in_progress` 行已有恢复扫描器。
- 验收：v1 只做了单 worker 单测；`scripts/multiworker_gate/` 的 room round 两 worker 用例列为 v2/待补。

## 15. 计费与配额

- 每个 slot = 一次普通 run → 现有 credit gate / burst slot / 并发上限全部生效，无需新计量。
- Room round 预算：成员上限 12 + 每轮 @ 追加上限 4（`MAX_EXTRA_TURNS`）；没有 `room_rounds.metadata.budget` 列，未实现的按根预算走 Bot 消息链（`MAX_ROOT_MESSAGES`）。
- Bot 预算（v2）：按 bot 的日/月 token 上限，超限拒绝新轮次。
- 计费口径：一轮群聊 = N 个 run 的费用；账单按 run 明细可查。

## 16. 安全与权限

- Bot 属于 workspace；room 成员必须同 workspace（v1）。跨 workspace 加群 v2 显式授权。
- Bot 继承 workspace 的信任模型：同电脑工作区互信，room 不是安全边界。
- Bot 消息是不可信 user 内容：不能自动触发高风险工具；审批级别不因"来自 Bot"提升。
- `persona`/模型/工具白名单变更留痕（`bot_versions` + 事件）。
- 密钥永不进 persona / 消息正文；仅 vault 引用。

## 17. 迁移与回填

顺序（一次迁移 + 一个脚本）：

1. `114_bots_rooms.py`：DDL + 回填 Bot + `threads.bot_id` + `automations.bot_id` + `bot_threads`。
2. 回填幂等：`INSERT ... ON CONFLICT DO NOTHING`；thread 更新用 `WHERE bot_id IS NULL`。
3. 旧客户端：不传 bot → 解析 thread.bot_id → workspace 默认 Bot；无 Bot 时按旧逻辑（role from workspace）。
4. 回滚：`115 → 114 → 113` 的 downgrade 会删表/列（包括 `bots`/`rooms`/`bot_*`，**数据会丢**），所以回滚前必须备份；只想回旧代码而不回 schema 时，停在 114/115 并 `alembic stamp` 到旧代码认识的头，避免旧代码 `upgrade head` 找不到新 revision。`automations.bot_id` 随列删除。
5. 运行 `scripts/automations_to_persistent.py --dry-run` 供人工确认（不自动执行）。

## 18. UI

| 页面 | 内容 |
|---|---|
| 工作区页（不变） | 只加一个入口：workspace 卡片上的 Bot 数（可选） |
| Bots 页（新增，Settings 下） | 列表/创建/编辑（身份、人设、模型、技能工具、审批、记忆、暂停/删除） |
| 私聊（现有 ChatAgent） | 头部显示 Bot 头像/名字；thread 由 bot 绑定 |
| Rooms 页（新增） | 群聊：成员头像、@ 补全、round 状态（谁在说/谁跳过）、公告编辑、轮次历史 |
| 自动化页（现有） | 加「所属 Bot」「会话策略（Bot 主会话/独立持续会话/每次新会话）」 |

## 19. 分阶段与工作量

| 阶段 | 内容 | 估时 |
|---|---|---|
| **P0** | 迁移 114 + 回填 + `threads.bot_id` + Bots API/页 + 私聊绑定（行为不变） | 3–4 天 |
| **P1** | 点对点消息（`send_message` + inbox + steering/queue）+ assignments + 防环 + 测试 | 5–7 天 |
| **P2** | Room：log/rounds/coordinator + 上下文注入 + @ 规则 + Room 页/SSE + 恢复扫描 | 10–15 天 |
| **P3** | 定时任务 Bot 归属与会话策略 + routines + 预算 + UI 打磨 | 5–7 天 |

**V1 状态：P0–P3 全部完成**（跨 workspace 群聊、`auto` responder、Bot 自主建群、`approval` 执行映射按 §21 与各节标注留到 v2）。

### 19.1 V1 验收记录（2026-11-08）

| 项 | 结果 |
|---|---|
| 后端全量单测 | `pytest tests/unit` → **28,245 passed**, 165 skipped；6 failed 均为合并前既有的 macOS 环境类失败（`File name too long` / 超时不触发 / unreachable 分支），与本次改动无关 |
| 后端定向套件 | server 16,279、llms+tools 2,979、ptc_agent/agent 5,415（同上 1 个既有失败） |
| 前端 | `tsc -b` exit 0；全量 vitest **925 文件 / 10,355 用例通过**；ESLint 0 error |
| 迁移 | 迁移 114 在 scratch 库上 upgrade 114（含回填）→ downgrade 113 → upgrade head 幂等通过 |
| request lock | 工具面每次变化后重新生成并全绿 |
| Review 修复（V1.1） | 后端 **28,384+ passed**（6 个既有 macOS 环境失败）、前端 **932 文件 / 10,469 用例**；迁移 114/115 在 scratch 库与真库集成测试上验证（含 114 早期版本残留修复与 113→head 循环） |
| 未包含 | 线上部署；Playwright e2e；多 worker 实机 gate（协调器已按 Postgres claim/lease 设计并单测覆盖，gate 用例留 v2） |

## 20. 测试计划

- **单元**：发送者框构造、thread/bot 解析（含 legacy）、slots 顺序与 @ 上限、幂等（idem/round slot）、防环、`[PASS]` 语义、persona 版本。
- **集成**：迁移回填（幂等）、私聊 now/queue/steer、round 快乐路径/超时/跳过/用户插话、automation 写持续会话、fallback（thread 被删）。
- **多 worker**：room round 两 worker 竞争、lease 过期接管、投递 worker 竞争；`scripts/multiworker_gate/` 扩展。
- **流式**：room SSE 回放/空洞 resync。
- **e2e**：建 2 Bot → 私聊互发 → 建群 → 用户一句 → 两 Bot 按 slots 各回一条 → room_log 有序。

## 21. 已拍板的决定（2026-11-08）

| # | 问题 | 决定 |
|---|---|---|
| 1 | Room 成员作用域 | **同一个 workspace**；跨 workspace 留到 v2 显式授权 |
| 2 | 用户是否占 slot | **用户是主持人**，不占 slot；用户发言 = 开一轮 |
| 3 | Bot 记忆作用域 | **继承 workspace 记忆**，v1 不新增 bot 级命名空间 |
| 4 | 群聊 responder | **可配置**：`everyone`（默认，host 先）/ `mentions` / `member:<botId>` |
| 5 | 房间上限 | **12 个 Bot**（另加主持人用户） |
| 6 | 定时任务默认目标 | **Bot 私聊**（`bot_primary`）；`routine` / `new` 为 per-automation 覆盖 |

由此确定的行为默认值：

- 新建 room：`responder='everyone'`，成员上限 12 个 Bot。
- 用户在任何群/私聊发消息：用户消息入日志并开启一轮，用户不进入 slots。
- 群聊上下文的记忆作用域 = 该 room 所属 workspace 的记忆。
- 新建 automation 且归属某个 Bot：`thread_strategy='continue'`，目标为 Bot 私聊。

## 22. 代码改动清单

```
migrations/versions/114_bots_rooms.py                 # 新增
src/server/database/bots.py                           # 新增
src/server/database/rooms.py                          # 新增
src/server/database/bot_messages.py                   # 新增
src/server/database/assignments.py                    # 新增
src/server/services/bots/resolver.py                  # 新增（lifecycle 逻辑在 database/bots.py + app/bots.py）
src/server/services/rooms/{coordinator.py,sweeper.py,context.py,threads.py}  # 新增
src/server/services/bot_messages/{delivery.py,sweeper.py}                    # 新增
src/server/app/bots.py                                # 新增路由
src/server/app/rooms.py                               # 新增路由
src/server/app/threads/messaging.py                   # bot 解析 + 服务端 sender 字段
src/server/services/turn_runtime.py                   # turn_route 接受 bot
src/server/services/automations/{lifecycle.py,file.py} # bot_id + 会话策略
src/server/services/automation_executor.py            # bot 归属 + 默认 persistent
src/ptc_agent/agent/agent.py                          # <bot> prompt 块 + 工具
src/tools/bots/tool.py                                # list_bots/send_bot_message/report_to/list_assignments
src/tools/rooms/tool.py                               # read_room/post_to_room/read_own_chat
src/tools/bots/scope.py                               # tools_allow/deny 收窄
scripts/automations_to_persistent.py                  # 存量切换（dry-run/apply）
scripts/bots_rooms_maintenance.py                     # lease/retry/retention/assignment 修复
web/src/pages/Bots/**                                 # 新增
web/src/pages/Rooms/**                                # 新增
web/src/locales/{en-US,zh-CN}.json                    # i18n
tests/unit/server/services/test_rooms.py              # 新增
tests/unit/server/app/test_rooms_api.py               # 新增
web/src/pages/Rooms/__tests__/RoomPanel.test.tsx      # 新增
# 集成测试 tests/integration/test_rooms_round.py 未建（见 §25）
```

## 23. 与 roostbot 的差异

| 项 | roostbot | LangAlpha 移植 |
|---|---|---|
| 租户 | workspace（团队） | user（LangAlpha workspace = 项目文件夹，不是租户） |
| Bot 执行环境 | OpenShell 回合沙箱 | 现有 computer/workspace 沙箱 |
| 凭据 | 出站注入（supervisor） | 现有 vault/env（保持现状，不在此改造） |
| 会话 | DSH 长活 session | LangAlpha thread（checkpoint 为单位） |
| 消息投递 | 控制面转发 | 现有 `wait_or_steer` + inbox |
| 群聊日志 | events 表投影 | 新 `room_log`（不混 chat wire） |

## 24. Bot 头像：确定性 BotMark

Bot 的头像是**身份标识**，不是装饰：同一个 Bot 在 Chat 头部、消息行、Bots 列表、房间成员、自动化选择器里必须是同一张脸，且不设置头像也要有稳定可辨的身份。

### 24.1 数据形态

`bots.avatar` 仍是开放的 JSONB，UI 识别四种：

```
{ kind: "image", url }                                    // 上传/外链
{ kind: "emoji", emoji }                                  // 快捷表情
{ kind: "mark", shape, color, glyph? }                    // 程序化
{} / 缺省                                                  // 渲染时按 bot_id 推导
```

旧数据里的 `{ url }` / `{ emoji }` 继续可用（等价于 image / emoji）。**不做 DB 回填**：缺省时渲染层推导，存量 Bot 立刻获得确定性头像。

### 24.2 推导规则（`web/src/lib/botMark.ts`）

- `fnv1a(bot_id)` → 颜色下标 `% 8`；`fnv1a(bot_id + ":shape")` → 形状下标 `% 4`
- 形状：circle / squircle / hex / blob；glyph = 名字首字（CJK 保留、拉丁转大写、emoji 保留），缺失回退 `·`
- 同一 bot_id 在任意尺寸/主题下推导结果一致

### 24.3 视觉

- 色板：品牌 8 色（amber / teal / blue / violet / rose / moss / clay / slate），实色填充，白色 glyph 对比度 ≥4.5:1；两套主题共用同一组实色
- 形状：hex/blob 用内联 SVG，circle/squircle 用 CSS 圆角；不做动画
- 尺寸：16（Chat 头部、房间日志）、20（房间成员 chips）、22（Bot 消息行）、36（Bots 列表与编辑预览）
- a11y：`role="img"` + `aria-label=Bot 名字`，纯装饰时 `aria-hidden`

### 24.4 使用点（全部走同一个 `BotMark`）

Chat 头部（`ThreadBotChip`）、Bot 消息行（`BotMessageRow`）、Bots 列表与编辑预览、房间成员 chips 与日志发送者、自动化表单的 Bot 选择、房间轮次"正在发言"行。各处的本地头像实现删除，不再重复。

### 24.5 编辑

Bots 编辑弹窗提供：颜色（8 色块）、形状（4 选 1）、可选 glyph、emoji、图片 URL，以及"使用自动头像"重置；预览实时。只有用户改动过才 PATCH `avatar`，未改动时保留未知键与已有 image/emoji。

### 24.6 为什么不做 ds-bot 的卡通角色

ds-bot 的 SVG 角色 + 表情动画是它产品性格的一部分；LangAlpha 是金融研究工作台，几何 mark 更安静、和现有 token 一致，也避免了角色系统长期维护成本。未来若要升级，只需替换 `BotMark` 的 mark 分支，调用点不变。

## 25. V1.1：整体 review 修复记录（2026-11-08）

对分支做了一次三线深审（后端、前端、端到端/运维）。本节记录已修项和仍明确留到 v2 的项，避免文档再次高估实现。

### 25.1 已修（带提交）

| 问题 | 修复 |
|---|---|
| Home 回合被解析成 analyst（P0） | resolver 识别 Home → CoS；迁移排除 Home analyst；115 修复已迁移库残留；新线程 stamp 身份 |
| bots/rooms 路由缺 reach 鉴权（P0） | 路由取 Principal + `assert_reaches`；列表按 `computer_id` 过滤；computer token 不再顺手创建 Home |
| Bot 互发无预算/环控（P0） | `bot_message_source` 带 root/depth；投递进 graph configurable；`send_bot_message` 深度 6 / 每链 64 条 / 每分钟 8 条上限 + `BOT_MESSAGING_ENABLED` kill switch |
| 迁移 114 锁与部署顺序（P0） | `conversation_threads.bot_id`/`automations.bot_id` 的 ALTER 进 autocommit block，索引 `CONCURRENTLY`；115 补 Home 修复与所需索引；部署步骤见 §17/§26 |
| 投递重试烧 attempts / 无 CAS（P1） | `delivering_until` lease + `claim_message`；`set_status` CAS；busy/interrupted 线程释放 attempt；`fail_exhausted_messages` |
| 房间丢尾部消息/重复回复/seen 不推进（P1） | 最新 N 条窗口；PASS/missed 推进 seen；轮末为迟到用户消息补一轮；slot 回复 `(round_id, metadata.slot)` 幂等；lease 保存失败即停 |
| 私有线程任意/共享（P1） | 只信 `primary_thread_id`，advisory lock 下铸造专用线程；115 清理旧 private 映射；`bind_thread_bot` 只在真正拿到线程时插映射 |
| 删除默认 Bot 后无法重建（P1） | live-name 唯一索引改为 partial；删除后 resolver 强制重绑线程 |
| automations 身份/paused/模型（P1） | `turn_bot` 进 turn；新线程 stamp；paused/deleted bot 的 firing 标记 `bot_unavailable` 跳过；移动 workspace 重新校验 bot；模型在 create/update 校验 |
| assignment 幂等 / 大 evidence（P1） | `assignments.idem`；evidence 先校验再关闭；report 投递异常容错 |
| 推送风暴 / 分享泄露（P2） | `suppress_push`（server-only，客户端伪造被清除）覆盖 room slot 与 bot 投递；`bot_message_source` 进 share 私有键 |
| 前端错误文案/SSE/缓存/加载态/头像/UX/i18n | 见提交 `41d262288`：failureText、SSE 4xx 停止与重试、cursor/Cache 失效、deep link、avatar 回退、report 状态映射、i18n 等 |
| 运维/可观测性 | delivery 终态日志；`scripts/bots_rooms_maintenance.py`（lease/retry/retention/stale assignment）。 |

### 25.2 明确留到 v2（未做，不要再按已实现引用）

- `POST /bots/{id}/messages` 这个别名路由（实际入口是 `POST /threads` 带 `bot_id` + Bots 页 Chat 按钮，已实现）。
- `approval` 的执行映射；`skills`/`memory_scopes` 目前只存储/展示，未进入 agent 构建。
- `bot.updated` / `bot.thread_fallback` 事件；`hook_outbox` 复用。
- root 预算的 96/64 双阈值细节（v1 为 6 深 / 64 条 / 8 条每分钟）；`room_rounds.metadata.budget` 列。
- room round 的 live API 集成测试（现有的是 114/115 真库迁移测试 `tests/integration/test_114_bots_rooms_migration.py`）与多 worker gate 的 room 用例、Playwright e2e。
- 房间列表分页、`room_log` 自动保留策略（脚本可手动执行）。`latest_run_reply` 仍按"最新终态 run"取答复，未按本次 run_id 锁定。
- ds-bot 的 per-pair Exchange 抽屉、Bot 卡通角色头像（现为确定性几何 BotMark）。
