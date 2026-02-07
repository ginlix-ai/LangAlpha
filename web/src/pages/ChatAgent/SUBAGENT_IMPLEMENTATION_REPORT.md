# Subagent Handling Implementation - Technical Report

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Floating Card State Management](#floating-card-state-management)
3. [Live Streaming Event Processing](#live-streaming-event-processing)
4. [History Loading Event Processing](#history-loading-event-processing)
5. [Variable Lifetime Management](#variable-lifetime-management)
6. [Event Flow Diagrams](#event-flow-diagrams)
7. [Code References](#code-references)

---

## Architecture Overview

The subagent handling system is built on a **layered architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    ChatView Component                        │
│  - Orchestrates UI rendering                                │
│  - Connects hooks and components                            │
│  - Handles user interactions                                 │
└──────────────┬──────────────────────────────────────────────┘
               │
               ├─────────────────┬─────────────────────────────┐
               │                 │                             │
┌──────────────▼──────────┐  ┌──▼──────────────────────────┐ │
│   useFloatingCards Hook  │  │  useChatMessages Hook       │ │
│  - Card state management  │  │  - Event routing           │ │
│  - Position/z-index       │  │  - Message state           │ │
│  - Minimize/maximize      │  │  - History loading         │ │
└──────────────┬───────────┘  └──┬─────────────────────────┘ │
               │                  │                            │
               │                  ├──────────────┬─────────────┘
               │                  │              │
┌──────────────▼──────────┐  ┌───▼──────────┐ ┌─▼──────────────┐
│  FloatingCard Component │  │ streamEvent │ │historyEvent   │
│  - Draggable UI         │  │ Handlers    │ │ Handlers       │
│  - Minimize/maximize UI │  │ - Live SSE  │ │ - Replay SSE   │
└─────────────────────────┘  └─────────────┘ └───────────────┘
```

### Key Design Principles

1. **Separation of Concerns**: Floating card state is managed separately from message state
2. **Event-Driven**: All updates flow through event handlers
3. **Lazy Loading**: History is processed but cards are only created on demand
4. **State Preservation**: Card positions and messages persist across updates

---

## Floating Card State Management

### State Structure

The floating card state is managed in `useFloatingCards.js`:

```javascript
// State structure for each card
{
  [cardId]: {
    title: string,                    // Display title
    isMinimized: boolean,            // Minimized state
    position: { x: number, y: number }, // Screen position
    zIndex: number,                  // Z-index for layering
    minimizeOrder: number | null,    // Order when minimized
    hasUnreadUpdate: boolean,        // Visual indicator for updates
    // Card-specific data:
    todoData?: { ... },             // For todo list cards
    subagentData?: {                 // For subagent cards
      taskId: string,
      description: string,
      type: string,
      status: 'active' | 'completed',
      toolCalls: number,
      currentTool: string,
      messages: Array<Message>,
      isHistory: boolean,
    }
  }
}
```

### Key Functions

#### `updateSubagentCard(taskId, subagentDataUpdate)`
**Location**: `useFloatingCards.js:265-343`

**Purpose**: Creates or updates a subagent floating card

**Key Behaviors**:
1. **Card ID**: Uses `subagent-${taskId}` as the card ID
2. **Position Preservation**: Preserves existing position object reference to prevent position reset
3. **Message Merging**: Merges new messages with existing messages (doesn't replace)
4. **Update Indicator**: Sets `hasUnreadUpdate` only if card is minimized
5. **Default Position**: Centers horizontally at `windowWidth / 1.25 - 260`, stacks vertically

**Code Flow**:
```javascript
updateSubagentCard(taskId, subagentDataUpdate) {
  const cardId = `subagent-${taskId}`;
  
  if (prev[cardId]) {
    // Update existing card
    // Preserve position reference
    // Merge subagentData
    // Set hasUnreadUpdate if minimized
  } else {
    // Create new card
    // Set default position
    // Initialize subagentData
  }
}
```

#### Position Management

**Critical Detail**: The position object reference is preserved to prevent `FloatingCard` from resetting position on updates:

```javascript
// Line 306: Preserve exact same object reference
position: existingCard.position, // Don't create new object!
```

This works because `FloatingCard` uses `useEffect` to watch `initialPosition` prop. If the reference changes, it resets position. By preserving the reference, we prevent unwanted resets.

---

## Live Streaming Event Processing

### Event Flow

```
SSE Stream
    │
    ├─► isSubagentEvent(event)?
    │   │
    │   ├─► YES ──► Route to subagent handlers
    │   │           │
    │   │           ├─► subagent_status ──► handleSubagentStatus
    │   │           ├─► message_chunk ──► handleSubagentMessageChunk
    │   │           ├─► tool_calls ──► handleSubagentToolCalls
    │   │           └─► tool_call_result ──► handleSubagentToolCallResult
    │   │
    │   └─► NO ────► Route to main agent handlers
    │
    └─► Update floating card via updateSubagentCard()
```

### Event Detection

**Location**: `streamEventHandlers.js:557-559`

```javascript
export function isSubagentEvent(event) {
  return event.agent && typeof event.agent === 'string' && event.agent.startsWith('tools:');
}
```

**Key Distinction**:
- **Main Agent**: `event.agent === "model:..."`
- **Subagent**: `event.agent === "tools:..."`

### Event Routing

**Location**: `useChatMessages.js:807-923`

The main event router in `handleSendMessage` callback:

```javascript
// Check if subagent event
const isSubagent = isSubagentEvent(event);

// Handle subagent_status events
if (eventType === 'subagent_status') {
  handleSubagentStatus({ subagentStatus, updateSubagentCard });
  return; // Don't process in main chat
}

// Handle other subagent events
if (isSubagent) {
  const taskId = getTaskIdFromEvent(event);
  if (taskId && updateSubagentCard) {
    // Route to appropriate handler
    if (eventType === 'message_chunk') {
      handleSubagentMessageChunk({ ... });
    } else if (eventType === 'tool_calls') {
      handleSubagentToolCalls({ ... });
    } else if (eventType === 'tool_call_result') {
      handleSubagentToolCallResult({ ... });
    }
  }
  return; // Don't process in main chat
}
```

### Subagent State Management (Live Streaming)

**Location**: `useChatMessages.js:750-758`

Each subagent task maintains its own state refs:

```javascript
const subagentStateRefs = {}; // Populated as subagents are detected

// Structure for each task:
subagentStateRefs[taskId] = {
  contentOrderCounterRef: { current: 0 },
  currentReasoningIdRef: { current: null },
  currentToolCallIdRef: { current: null },
  messages: [], // Array of message objects
};
```

**Lifetime**: Created on first subagent event, persists for the duration of the streaming session.

### Handler Details

#### 1. `handleSubagentStatus`
**Location**: `streamEventHandlers.js:485-550`

**Purpose**: Creates/updates cards when subagent status changes

**Key Behaviors**:
- Validates task IDs (skips tasks without IDs)
- Updates both `active_tasks` and `completed_tasks`
- **Preserves messages** from previous updates (doesn't overwrite)
- Sets status: `'active'` or `'completed'`

**Code**:
```javascript
active_tasks.forEach((task) => {
  if (!task || !task.id) return; // Skip invalid tasks
  updateSubagentCard(taskId, {
    taskId,
    description: task.description || '',
    type: task.type || 'general-purpose',
    toolCalls: task.tool_calls || 0,
    currentTool: task.current_tool || '',
    status: 'active',
    // Don't set messages - preserve existing
  });
});
```

#### 2. `handleSubagentMessageChunk`
**Location**: `streamEventHandlers.js:574-782`

**Purpose**: Processes message chunks (reasoning, text) for subagents

**State Management**:
- Gets or creates `subagentStateRefs[taskId]`
- Maintains message array in refs (not React state)
- Updates floating card after each chunk

**Message Structure**:
```javascript
{
  id: assistantMessageId,
  role: 'assistant',
  contentSegments: [
    { type: 'reasoning', reasoningId, order },
    { type: 'text', content, order },
  ],
  reasoningProcesses: {
    [reasoningId]: {
      content: string,
      isReasoning: boolean,
      reasoningComplete: boolean,
      order: number,
    }
  },
  toolCallProcesses: { ... },
  content: string, // Accumulated text
  contentType: 'text',
}
```

**Edge Cases Handled**:
- Reasoning content arrives before "start" signal → Creates reasoning process
- Message doesn't exist → Creates new message

#### 3. `handleSubagentToolCalls`
**Location**: `streamEventHandlers.js:794-895`

**Purpose**: Processes tool call events for subagents

**Key Behaviors**:
- Creates tool call segments and processes
- **Updates `currentTool`** in floating card (shows what's running)
- Sets `isInProgress: true` when tool call starts

**Status Update**:
```javascript
// Line 890-893: Update currentTool to show running tool
updateSubagentCard(taskId, { 
  messages: taskRefs.messages,
  currentTool: currentToolName, // Shows "read_file", "write_file", etc.
});
```

#### 4. `handleSubagentToolCallResult`
**Location**: `streamEventHandlers.js:908-1094`

**Purpose**: Processes tool call results for subagents

**Key Behaviors**:
- Finds message containing the tool call (by `toolCallId`)
- Updates tool call with result
- **Clears `currentTool`** if no tools are in progress

**Status Update Logic**:
```javascript
// Lines 1074-1092: Check for in-progress tools
let hasInProgressTool = false;
for (const msg of updatedMessages) {
  for (const [tcId, tcProcess] of Object.entries(msg.toolCallProcesses || {})) {
    if (tcProcess.isInProgress && !tcProcess.isComplete) {
      hasInProgressTool = true;
      currentToolName = tcProcess.toolName;
      break;
    }
  }
}

// Clear currentTool if no tools running
updateSubagentCard(taskId, { 
  messages: updatedMessages,
  currentTool: hasInProgressTool ? currentToolName : '',
});
```

### Agent-to-Task Mapping

**Location**: `useChatMessages.js:80-82, 762-783`

**Purpose**: Maps `event.agent` (e.g., `"tools:..."`) to `taskId` (e.g., `"Task-1"`)

**Storage**:
```javascript
const agentToTaskMapRef = useRef(new Map()); // Map<agentId, taskId>
const activeSubagentTasksRef = useRef(new Map()); // Map<taskId, taskInfo>
```

**Mapping Strategy**:
1. Check `agentToTaskMapRef` for existing mapping
2. If only one active task, use it (single-task fallback)
3. Cache mapping for future events

**Code**:
```javascript
const getTaskIdFromEvent = (event) => {
  if (!event.agent) return null;
  
  const agentId = event.agent;
  if (agentToTaskMapRef.current.has(agentId)) {
    return agentToTaskMapRef.current.get(agentId);
  }
  
  // Single-task fallback
  const activeTasks = Array.from(activeSubagentTasksRef.current.keys());
  if (activeTasks.length === 1) {
    const taskId = activeTasks[0];
    agentToTaskMapRef.current.set(agentId, taskId);
    return taskId;
  }
  
  return activeTasks[0] || null;
};
```

---

## History Loading Event Processing

### Overview

History loading processes events in **two phases**:

1. **Collection Phase**: Store subagent events separately from main agent events
2. **Processing Phase**: Build message structures from stored events (lazy loading)

### Event Flow

```
History Replay (replayThreadHistory)
    │
    ├─► isSubagentHistoryEvent(event)?
    │   │
    │   ├─► YES ──► Store in subagentHistoryByTaskId[taskId].events
    │   │           (Don't process in main chat)
    │   │
    │   └─► NO ────► Process in main chat (normal history flow)
    │
    └─► After replay completes:
        │
        └─► For each taskId in subagentHistoryByTaskId:
            │
            ├─► Create temp refs structure
            ├─► Process stored events using subagent handlers
            ├─► Build final messages array
            └─► Store in subagentHistoryRef.current[taskId]
                (Available for lazy loading)
```

### Event Detection

**Location**: `historyEventHandlers.js:11-13`

```javascript
export function isSubagentHistoryEvent(event) {
  return event.agent && typeof event.agent === 'string' && event.agent.startsWith('tools:');
}
```

### Collection Phase

**Location**: `useChatMessages.js:155-260`

**Data Structures**:
```javascript
// Per-history-load storage
const subagentHistoryByTaskId = new Map(); // Map<taskId, { messages, events, description, type }>
const agentToTaskMap = new Map(); // Map<agentId, taskId> (for this history load)
```

**Event Processing**:
```javascript
// Handle subagent_status events - build mapping
if (eventType === 'subagent_status') {
  [...activeTasks, ...completedTasks].forEach((task) => {
    if (task && task.id && task.agent) {
      agentToTaskMap.set(task.agent, task.id);
      // Initialize storage
      subagentHistoryByTaskId.set(task.id, {
        messages: [],
        events: [],
        description: task.description || '',
        type: task.type || 'general-purpose',
      });
    }
  });
  return; // Don't process in main chat
}

// Handle other subagent events - store them
if (isSubagent) {
  let taskId = agentToTaskMap.get(event.agent);
  
  // Single-task fallback (if only one task known)
  if (!taskId && subagentHistoryByTaskId.size === 1) {
    const [onlyTaskId] = Array.from(subagentHistoryByTaskId.keys());
    taskId = onlyTaskId;
  }
  
  if (taskId) {
    subagentHistoryByTaskId.get(taskId).events.push(event);
  }
  return; // Don't process in main chat
}
```

**Single-Task Fallback**: If only one subagent task is known, all subagent events are attributed to it. This handles cases where `subagent_status` events are missing from history.

### Processing Phase

**Location**: `useChatMessages.js:517-623`

**Purpose**: Build message structures from stored events

**Key Design**: Uses **no-op updater** to prevent floating cards from being created during history load:

```javascript
// History-specific no-op updater
const historyUpdateSubagentCard = () => {};
```

**Processing Loop**:
```javascript
for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
  // Create temporary refs (similar to live streaming)
  const tempSubagentStateRefs = {
    [taskId]: {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: [],
    },
  };
  
  // Process each stored event
  for (const event of subagentHistory.events) {
    if (eventType === 'message_chunk') {
      handleSubagentMessageChunk({
        ...,
        refs: { subagentStateRefs: tempSubagentStateRefs },
        updateSubagentCard: historyUpdateSubagentCard, // No-op!
      });
    } else if (eventType === 'tool_calls') {
      handleSubagentToolCalls({ ... });
    } else if (eventType === 'tool_call_result') {
      handleSubagentToolCallResult({ ... });
    }
  }
  
  // Store final result
  subagentHistoryRef.current[taskId] = {
    taskId,
    description: taskMetadata?.description || '',
    type: taskMetadata?.type || 'general-purpose',
    messages: finalMessages,
    status: 'completed',
    toolCalls: 0,
    currentTool: '',
  };
}
```

**Result**: Messages are built in memory but no floating cards are created. Cards are created lazily when user clicks "Open subagent details".

### Lazy Loading

**Location**: `ChatView.jsx:211-246`

When user clicks "Open subagent details" button:

```javascript
onOpenSubagentTask={(subagentInfo) => {
  const { subagentId, description, type, status } = subagentInfo;
  
  // Try to load history
  const history = getSubagentHistory(subagentId);
  
  // Use history if available, otherwise use current info
  const finalDescription = history?.description || description || '';
  const finalMessages = history?.messages || [];
  const isHistoryCard = !!history;
  
  // Open floating card with history data
  updateSubagentCard(subagentId, {
    taskId: subagentId,
    description: finalDescription,
    type: finalType,
    status: finalStatus,
    messages: finalMessages,
    isHistory: isHistoryCard, // Flag for UI rendering
  });
}}
```

**UI Difference**: When `isHistory: true`, `SubagentCardContent` hides:
- Task ID and type header
- Status indicator

---

## Variable Lifetime Management

### React State vs Refs

**State** (Re-renders on change):
- `floatingCards` - Card state (position, minimize, etc.)
- `messages` - Main chat messages

**Refs** (Persist across renders, no re-render):
- `subagentStateRefs` - Per-task message state (live streaming)
- `agentToTaskMapRef` - Agent-to-task mapping (persists across sessions)
- `activeSubagentTasksRef` - Active tasks tracking
- `subagentHistoryRef` - Processed history (lazy loading)

### Lifetime Diagram

```
Component Mount
    │
    ├─► useFloatingCards()
    │   └─► useState(floatingCards) ──► Persists until unmount
    │
    ├─► useChatMessages()
    │   ├─► useState(messages) ──► Persists until unmount
    │   ├─► useRef(agentToTaskMapRef) ──► Persists across renders
    │   ├─► useRef(activeSubagentTasksRef) ──► Persists across renders
    │   └─► useRef(subagentHistoryRef) ──► Persists across renders
    │
    └─► Live Streaming Session
        │
        ├─► subagentStateRefs[taskId] ──► Created on first event
        │   └─► Persists for session duration
        │
        └─► History Loading
            │
            ├─► subagentHistoryByTaskId ──► Local to loadConversationHistory()
            │   └─► Discarded after processing
            │
            └─► subagentHistoryRef.current[taskId] ──► Stored after processing
                └─► Persists until component unmount
```

### Critical Ref Usage

#### 1. `subagentStateRefs` (Live Streaming)
**Location**: `useChatMessages.js:750`

**Why Ref?**: Messages are built incrementally across multiple event handlers. Using refs avoids unnecessary re-renders and allows handlers to mutate state directly.

**Structure**:
```javascript
subagentStateRefs[taskId] = {
  contentOrderCounterRef: { current: 0 },
  currentReasoningIdRef: { current: null },
  currentToolCallIdRef: { current: null },
  messages: [], // Array of message objects
};
```

**Updates**: Handlers mutate `messages` array directly, then call `updateSubagentCard` to trigger React state update.

#### 2. `subagentHistoryRef` (History)
**Location**: `useChatMessages.js:86, 611-619`

**Why Ref?**: History is processed once and stored for lazy loading. No need to trigger re-renders until user opens card.

**Structure**:
```javascript
subagentHistoryRef.current = {
  [taskId]: {
    taskId,
    description,
    type,
    messages: [...], // Fully built message array
    status: 'completed',
    toolCalls: 0,
    currentTool: '',
  }
};
```

**Access**: Exposed via `getSubagentHistory(taskId)` function.

#### 3. `agentToTaskMapRef` (Mapping)
**Location**: `useChatMessages.js:82`

**Why Ref?**: Mapping persists across streaming sessions and history loads. Needs to survive re-renders.

**Updates**:
- Live streaming: Updated in `handleSubagentStatus` and `getTaskIdFromEvent`
- History loading: Seeded from ref, updated during replay

---

## Event Flow Diagrams

### Live Streaming Flow

```
User sends message
    │
    └─► handleSendMessage()
        │
        ├─► Create assistant message placeholder
        ├─► sendChatMessageStream()
        │   │
        │   └─► SSE Event Stream
        │       │
        │       ├─► subagent_status
        │       │   └─► handleSubagentStatus()
        │       │       └─► updateSubagentCard()
        │       │           └─► Floating card created/updated
        │       │
        │       ├─► message_chunk (subagent)
        │       │   └─► isSubagentEvent() → YES
        │       │       └─► handleSubagentMessageChunk()
        │       │           ├─► Update subagentStateRefs[taskId].messages
        │       │           └─► updateSubagentCard()
        │       │
        │       ├─► tool_calls (subagent)
        │       │   └─► isSubagentEvent() → YES
        │       │       └─► handleSubagentToolCalls()
        │       │           ├─► Update subagentStateRefs[taskId].messages
        │       │           ├─► Set currentTool
        │       │           └─► updateSubagentCard()
        │       │
        │       └─► tool_call_result (subagent)
        │           └─► isSubagentEvent() → YES
        │               └─► handleSubagentToolCallResult()
        │                   ├─► Update subagentStateRefs[taskId].messages
        │                   ├─► Clear currentTool if no tools running
        │                   └─► updateSubagentCard()
```

### History Loading Flow

```
Component mounts / threadId changes
    │
    └─► loadConversationHistory()
        │
        ├─► Initialize storage
        │   ├─► subagentHistoryByTaskId = new Map()
        │   └─► agentToTaskMap = new Map()
        │
        ├─► replayThreadHistory()
        │   │
        │   └─► For each event:
        │       │
        │       ├─► subagent_status
        │       │   └─► Build agentToTaskMap
        │       │   └─► Initialize subagentHistoryByTaskId[taskId]
        │       │
        │       └─► Other subagent events
        │           └─► isSubagentHistoryEvent() → YES
        │               └─► Store in subagentHistoryByTaskId[taskId].events
        │
        └─► After replay completes:
            │
            └─► For each taskId:
                │
                ├─► Create tempSubagentStateRefs[taskId]
                ├─► Process stored events:
                │   ├─► handleSubagentMessageChunk(..., historyUpdateSubagentCard)
                │   ├─► handleSubagentToolCalls(..., historyUpdateSubagentCard)
                │   └─► handleSubagentToolCallResult(..., historyUpdateSubagentCard)
                │
                └─► Store in subagentHistoryRef.current[taskId]
                    └─► Available for lazy loading
```

### Lazy Loading Flow

```
User clicks "Open subagent details"
    │
    └─► onOpenSubagentTask(subagentInfo)
        │
        ├─► getSubagentHistory(subagentId)
        │   └─► Returns subagentHistoryRef.current[taskId] or null
        │
        ├─► Merge history with current info
        │   ├─► description = history?.description || current
        │   ├─► messages = history?.messages || []
        │   └─► isHistory = !!history
        │
        └─► updateSubagentCard(taskId, { ... })
            └─► Floating card created/updated
                └─► SubagentCardContent renders with isHistory flag
```

---

## Code References

### Core Files

1. **`useFloatingCards.js`** (363 lines)
   - Floating card state management
   - `updateSubagentCard()`: Lines 265-343
   - Position preservation: Line 306
   - Default positioning: Lines 271-283

2. **`useChatMessages.js`** (1080 lines)
   - Main event routing: Lines 791-1037
   - History loading: Lines 133-644
   - Subagent state refs: Lines 750-758
   - Agent-to-task mapping: Lines 80-82, 762-783
   - History storage: Lines 86, 611-619
   - Lazy loading access: Lines 1077-1078

3. **`streamEventHandlers.js`** (1095 lines)
   - `isSubagentEvent()`: Lines 557-559
   - `handleSubagentStatus()`: Lines 485-550
   - `handleSubagentMessageChunk()`: Lines 574-782
   - `handleSubagentToolCalls()`: Lines 794-895
   - `handleSubagentToolCallResult()`: Lines 908-1094

4. **`historyEventHandlers.js`** (588 lines)
   - `isSubagentHistoryEvent()`: Lines 11-13
   - History event filtering: Used in `useChatMessages.js:169-260`

5. **`ChatView.jsx`** (305 lines)
   - Component orchestration: Lines 28-304
   - Lazy loading handler: Lines 211-246
   - Floating card rendering: Lines 258-299

6. **`SubagentCardContent.jsx`** (159 lines)
   - UI rendering: Lines 22-156
   - History mode: Lines 57-67, 77-82

### Key Functions Summary

| Function | Location | Purpose |
|----------|----------|---------|
| `updateSubagentCard` | `useFloatingCards.js:265` | Create/update subagent floating card |
| `isSubagentEvent` | `streamEventHandlers.js:557` | Detect subagent events in live stream |
| `isSubagentHistoryEvent` | `historyEventHandlers.js:11` | Detect subagent events in history |
| `handleSubagentStatus` | `streamEventHandlers.js:485` | Process subagent status updates |
| `handleSubagentMessageChunk` | `streamEventHandlers.js:574` | Process subagent message chunks |
| `handleSubagentToolCalls` | `streamEventHandlers.js:794` | Process subagent tool calls |
| `handleSubagentToolCallResult` | `streamEventHandlers.js:908` | Process subagent tool results |
| `loadConversationHistory` | `useChatMessages.js:133` | Load and process history |
| `getSubagentHistory` | `useChatMessages.js:1077` | Retrieve stored history |
| `getTaskIdFromEvent` | `useChatMessages.js:762` | Map agent ID to task ID |

---

## Summary

The subagent handling system uses a **three-layer architecture**:

1. **State Layer** (`useFloatingCards`): Manages card UI state
2. **Event Layer** (`useChatMessages`): Routes events to handlers
3. **Handler Layer** (`streamEventHandlers`, `historyEventHandlers`): Processes specific event types

**Key Design Decisions**:
- **Refs for subagent state**: Avoids re-renders during incremental message building
- **Lazy history loading**: Processes history but only creates cards on demand
- **Position preservation**: Preserves object references to prevent position resets
- **Single-task fallback**: Handles missing mappings in history
- **No-op updater**: Prevents card creation during history processing

**Variable Lifetimes**:
- **React State**: UI state (cards, messages) - triggers re-renders
- **Refs**: Internal state (subagent messages, mappings) - persists without re-renders
- **Local Variables**: Temporary storage (history processing) - discarded after use

This architecture ensures:
- ✅ Subagent events don't duplicate in main chat
- ✅ Floating cards persist and maintain position
- ✅ History is loaded efficiently (lazy loading)
- ✅ Status updates reflect real-time state
- ✅ Code is maintainable and well-separated
