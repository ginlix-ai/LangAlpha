# ChatAgent Hooks Architecture & Orchestration

## Overview

The `useChatMessages` hook has been refactored into a modular architecture where specialized utility modules handle specific concerns. This document explains how these modules orchestrate together to manage chat functionality.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         useChatMessages Hook                     │
│  (Main Orchestrator - ~400 lines)                                │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   State      │  │    Refs      │  │   Effects    │         │
│  │ Management   │  │  Management  │  │  Management  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│         │                 │                 │                    │
│         └─────────────────┼─────────────────┘                    │
│                           │                                        │
│         ┌─────────────────┴─────────────────┐                    │
│         │                                     │                    │
│    ┌────▼────┐                          ┌────▼────┐             │
│    │  Send   │                          │  Load   │             │
│    │ Message │                          │ History │             │
│    └────┬────┘                          └────┬────┘             │
└─────────┼────────────────────────────────────┼──────────────────┘
          │                                    │
          │                                    │
    ┌─────▼────────────────────────────────────▼─────┐
    │                                                 │
    │         Utility Modules Layer                   │
    │                                                 │
    ├─────────────────────────────────────────────────┤
    │                                                 │
    │  ┌──────────────────┐  ┌──────────────────┐   │
    │  │ threadStorage    │  │ messageHelpers   │   │
    │  │ - getStored      │  │ - createUser     │   │
    │  │ - setStored      │  │ - createAssistant│   │
    │  │ - removeStored   │  │ - updateMessage  │   │
    │  └──────────────────┘  └──────────────────┘   │
    │                                                 │
    │  ┌──────────────────┐  ┌──────────────────┐   │
    │  │ recentlySent     │  │ streamEvent      │   │
    │  │ Tracker          │  │ Handlers         │   │
    │  │ - track()        │  │ - reasoning      │   │
    │  │ - isRecentlySent │  │ - text           │   │
    │  │ - clear()        │  │ - tool calls     │   │
    │  └──────────────────┘  └──────────────────┘   │
    │                                                 │
    │  ┌──────────────────┐                         │
    │  │ historyEvent     │                         │
    │  │ Handlers         │                         │
    │  │ - user message   │                         │
    │  │ - reasoning      │                         │
    │  │ - text           │                         │
    │  │ - tool calls     │                         │
    │  └──────────────────┘                         │
    │                                                 │
    └─────────────────────────────────────────────────┘
```

## Module Responsibilities

### 1. **threadStorage.js** - Persistence Layer
**Purpose**: Manages thread ID persistence in localStorage

```javascript
// Example usage in useChatMessages:
useEffect(() => {
  if (workspaceId && threadId && threadId !== '__default__') {
    setStoredThreadId(workspaceId, threadId);  // Auto-save on change
  }
}, [workspaceId, threadId]);
```

**Key Functions**:
- `getStoredThreadId(workspaceId)` - Retrieves thread ID for a workspace
- `setStoredThreadId(workspaceId, threadId)` - Saves thread ID
- `removeStoredThreadId(workspaceId)` - Cleans up on workspace deletion

### 2. **messageHelpers.js** - Message Factory
**Purpose**: Creates and manipulates message objects

```javascript
// Example: Creating a new user message
const userMessage = createUserMessage("Hello!");
// Returns: { id: "user-1234567890", role: "user", content: "Hello!", ... }

// Example: Updating an assistant message
setMessages(prev => updateMessage(prev, assistantId, (msg) => ({
  ...msg,
  content: msg.content + " new text",
  isStreaming: false
})));
```

**Key Functions**:
- `createUserMessage(message)` - Creates user message
- `createAssistantMessage(id)` - Creates assistant placeholder
- `updateMessage(messages, id, updater)` - Updates specific message
- `insertMessage(messages, index, message)` - Inserts at position
- `appendMessage(messages, message)` - Appends to end

### 3. **recentlySentTracker.js** - Duplicate Prevention
**Purpose**: Tracks recently sent messages to prevent duplicates

```javascript
// Example: Tracking a sent message
recentlySentTracker.track("Hello!", new Date(), "user-123");

// Example: Checking for duplicates
if (recentlySentTracker.isRecentlySent("Hello!")) {
  // Skip adding this message from history
}
```

**Key Functions**:
- `track(content, timestamp, id)` - Records a sent message
- `isRecentlySent(content)` - Checks if message was recently sent
- `clear()` - Clears all tracked messages

### 4. **streamEventHandlers.js** - Live Streaming Logic
**Purpose**: Handles events from active message streaming (SSE)

**Key Functions**:
- `handleReasoningSignal()` - Processes reasoning start/complete
- `handleReasoningContent()` - Accumulates reasoning text
- `handleTextContent()` - Processes text chunks
- `handleToolCalls()` - Processes tool call events
- `handleToolCallResult()` - Processes tool results

### 5. **historyEventHandlers.js** - History Replay Logic
**Purpose**: Handles events from history replay (SSE stream of past conversations)

**Key Functions**:
- `handleHistoryUserMessage()` - Creates user/assistant message pairs
- `handleHistoryReasoningSignal()` - Replays reasoning signals
- `handleHistoryReasoningContent()` - Replays reasoning content
- `handleHistoryTextContent()` - Replays text content
- `handleHistoryToolCalls()` - Replays tool calls
- `handleHistoryToolCallResult()` - Replays tool results

## Workflow Examples

### Workflow 1: Sending a New Message

```
┌─────────────────────────────────────────────────────────────┐
│ User Types Message & Clicks Send                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ handleSendMessage() in useChatMessages                      │
│                                                              │
│ 1. Create user message using messageHelpers                │
│    const userMessage = createUserMessage(message);          │
│                                                              │
│ 2. Track message using recentlySentTracker                 │
│    recentlySentTracker.track(message, timestamp, id);      │
│                                                              │
│ 3. Add user message to state                                │
│    setMessages(prev => appendMessage(prev, userMessage));   │
│                                                              │
│ 4. Create assistant placeholder                             │
│    const assistantMsg = createAssistantMessage();           │
│    setMessages(prev => appendMessage(prev, assistantMsg)); │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ sendChatMessageStream() API Call                           │
│                                                              │
│ - Sends message to backend                                  │
│ - Receives SSE stream events                                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Stream Event Processing (for each SSE event)                │
│                                                              │
│ Event Type: message_chunk                                   │
│   ├─ contentType: reasoning_signal                          │
│   │  └─> handleReasoningSignal()                            │
│   │      - Creates reasoning process                        │
│   │      - Updates message state                            │
│   │                                                          │
│   ├─ contentType: reasoning                                 │
│   │  └─> handleReasoningContent()                           │
│   │      - Accumulates reasoning text                       │
│   │      - Updates message state                            │
│   │                                                          │
│   ├─ contentType: text                                      │
│   │  └─> handleTextContent()                                │
│   │      - Adds text segment                                │
│   │      - Updates message content                          │
│   │                                                          │
│ Event Type: tool_calls                                      │
│   └─> handleToolCalls()                                    │
│       - Creates tool call process                           │
│       - Updates message state                               │
│                                                              │
│ Event Type: tool_call_result                                │
│   └─> handleToolCallResult()                                │
│       - Updates tool call with result                       │
│       - Marks tool call as complete                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Final State Update                                          │
│                                                              │
│ - Mark message as complete (isStreaming: false)             │
│ - Update thread ID if provided                              │
│ - Save thread ID to localStorage (via threadStorage)        │
└─────────────────────────────────────────────────────────────┘
```

**Code Flow Example**:

```javascript
// In useChatMessages.js - handleSendMessage()
const handleSendMessage = async (message, planMode, additionalContext) => {
  // Step 1: Create user message
  const userMessage = createUserMessage(message);
  
  // Step 2: Track to prevent duplicates
  recentlySentTrackerRef.current.track(
    message.trim(), 
    userMessage.timestamp, 
    userMessage.id
  );
  
  // Step 3: Add to state
  setMessages(prev => appendMessage(prev, userMessage));
  
  // Step 4: Create assistant placeholder
  const assistantMessage = createAssistantMessage();
  setMessages(prev => appendMessage(prev, assistantMessage));
  
  // Step 5: Stream with event handlers
  await sendChatMessageStream(message, workspaceId, threadId, ..., (event) => {
    if (eventType === 'message_chunk') {
      if (contentType === 'reasoning_signal') {
        handleReasoningSignal({ assistantMessageId, signalContent, refs, setMessages });
      } else if (contentType === 'text') {
        handleTextContent({ assistantMessageId, content, finishReason, refs, setMessages });
      }
      // ... other handlers
    }
  });
};
```

### Workflow 2: Loading Conversation History

```
┌─────────────────────────────────────────────────────────────┐
│ Component Mounts or Thread Changes                          │
│                                                              │
│ useEffect([workspaceId, threadId]) triggers                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ loadConversationHistory() in useChatMessages                │
│                                                              │
│ 1. Validate inputs (workspaceId, threadId)                │
│ 2. Set loading state                                        │
│ 3. Initialize tracking maps                                 │
│    - assistantMessagesByPair: Map<pairIndex, messageId>     │
│    - pairStateByPair: Map<pairIndex, state>                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ replayThreadHistory() API Call                             │
│                                                              │
│ - Calls backend API to replay thread history                │
│ - Receives SSE stream of historical events                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ History Event Processing (for each SSE event)               │
│                                                              │
│ Event Type: user_message                                    │
│   └─> handleHistoryUserMessage()                           │
│       ├─ Check if duplicate (recentlySentTracker)         │
│       ├─ If duplicate & streaming: map to current message │
│       ├─ If not duplicate: create user message             │
│       └─ Create assistant placeholder                       │
│                                                              │
│ Event Type: message_chunk                                   │
│   ├─ contentType: reasoning_signal                          │
│   │  └─> handleHistoryReasoningSignal()                    │
│   │      - Creates reasoning process (already complete)    │
│   │                                                          │
│   ├─ contentType: reasoning                                │
│   │  └─> handleHistoryReasoningContent()                   │
│   │      - Accumulates reasoning text                       │
│   │                                                          │
│   ├─ contentType: text                                      │
│   │  └─> handleHistoryTextContent()                        │
│   │      - Adds text segment                                │
│   │      - Updates message content                          │
│                                                              │
│ Event Type: tool_calls                                      │
│   └─> handleHistoryToolCalls()                              │
│       - Creates tool call process (already complete)        │
│                                                              │
│ Event Type: tool_call_result                                │
│   └─> handleHistoryToolCallResult()                        │
│       - Updates tool call with result                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ History Loading Complete                                    │
│                                                              │
│ - All history messages inserted before new messages         │
│ - newMessagesStartIndexRef tracks insertion point          │
│ - Loading state cleared                                     │
└─────────────────────────────────────────────────────────────┘
```

**Code Flow Example**:

```javascript
// In useChatMessages.js - loadConversationHistory()
const loadConversationHistory = async () => {
  // Initialize tracking
  const assistantMessagesByPair = new Map();
  const pairStateByPair = new Map();
  
  await replayThreadHistory(threadId, (event) => {
    const eventType = event.event;
    
    // Handle user_message events
    if (eventType === 'user_message' && event.content && hasPairIndex) {
      handleHistoryUserMessage({
        event,
        pairIndex: event.pair_index,
        assistantMessagesByPair,
        pairStateByPair,
        refs: {
          recentlySentTracker: recentlySentTrackerRef.current,
          currentMessageRef,
          newMessagesStartIndexRef,
          historyMessagesRef,
        },
        messages,
        setMessages,
      });
    }
    
    // Handle message_chunk events
    if (eventType === 'message_chunk' && event.role === 'assistant') {
      const assistantId = assistantMessagesByPair.get(event.pair_index);
      const pairState = pairStateByPair.get(event.pair_index);
      
      if (event.content_type === 'reasoning_signal') {
        handleHistoryReasoningSignal({
          assistantMessageId: assistantId,
          signalContent: event.content,
          pairIndex: event.pair_index,
          pairState,
          setMessages,
        });
      }
      // ... other content types
    }
  });
};
```

### Workflow 3: Thread ID Management

```
┌─────────────────────────────────────────────────────────────┐
│ Component Initializes                                       │
│                                                              │
│ useChatMessages(workspaceId, initialThreadId)               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Initial Thread ID Resolution                                │
│                                                              │
│ if (initialThreadId) {                                      │
│   use initialThreadId  // From URL params                   │
│ } else {                                                     │
│   getStoredThreadId(workspaceId)  // From localStorage      │
│ }                                                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Thread ID Changes (via useEffect)                          │
│                                                              │
│ When threadId changes:                                      │
│   1. setStoredThreadId(workspaceId, threadId)              │
│      └─> Saves to localStorage                              │
│                                                              │
│ When workspaceId changes:                                    │
│   1. Load new threadId from localStorage                    │
│   2. If switching to different thread:                     │
│      - Clear messages                                       │
│      - Reset refs                                           │
│      - Clear recentlySentTracker                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Thread ID Updates from Stream                               │
│                                                              │
│ During message streaming:                                  │
│   if (event.thread_id && event.thread_id !== current) {    │
│     setThreadId(event.thread_id)                           │
│     setStoredThreadId(workspaceId, event.thread_id)        │
│   }                                                          │
└─────────────────────────────────────────────────────────────┘
```

## Key Orchestration Patterns

### Pattern 1: Event Handler Composition

Event handlers are **pure functions** that receive all dependencies as parameters:

```javascript
// Handler signature pattern
function handleEvent({
  assistantMessageId,    // Which message to update
  content,                // Event data
  refs,                  // Refs for state tracking
  setMessages,           // State setter
  // ... other params
}) {
  // Handler logic
  setMessages(prev => updateMessage(prev, assistantMessageId, (msg) => {
    // Update logic
  }));
}
```

**Benefits**:
- Testable in isolation
- No hidden dependencies
- Easy to mock for testing
- Clear data flow

### Pattern 2: State Update Abstraction

Message updates use helper functions for consistency:

```javascript
// Instead of inline map operations:
setMessages(prev => prev.map(msg => {
  if (msg.id !== messageId) return msg;
  return { ...msg, content: msg.content + newText };
}));

// Use helper:
setMessages(prev => updateMessage(prev, messageId, (msg) => ({
  ...msg,
  content: msg.content + newText
})));
```

**Benefits**:
- Consistent update patterns
- Less boilerplate
- Easier to refactor
- Clearer intent

### Pattern 3: Ref-Based State Tracking

Streaming state is tracked via refs to avoid re-renders:

```javascript
// Refs for streaming state
const contentOrderCounterRef = useRef(0);
const currentReasoningIdRef = useRef(null);
const currentToolCallIdRef = useRef(null);

// Used in handlers:
handleReasoningSignal({
  assistantMessageId,
  signalContent,
  refs: {
    contentOrderCounterRef,
    currentReasoningIdRef,
  },
  setMessages,
});
```

**Benefits**:
- No unnecessary re-renders
- Persistent across renders
- Fast access
- Clear separation of concerns

## Module Interaction Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                    useChatMessages Hook                      │
│                                                              │
│  State: messages, threadId, isLoading, etc.                 │
│  Refs: contentOrderCounterRef, currentReasoningIdRef, etc.   │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Uses
             │
    ┌────────┴────────┬──────────────┬──────────────┐
    │                 │              │              │
    ▼                 ▼              ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│thread    │   │message   │   │recently  │   │stream    │
│Storage   │   │Helpers   │   │Sent      │   │Event     │
│          │   │          │   │Tracker   │   │Handlers  │
└──────────┘   └──────────┘   └──────────┘   └──────────┘
    │                 │              │              │
    │                 │              │              │
    │                 │              │              │
    ▼                 ▼              ▼              ▼
┌──────────────────────────────────────────────────────────┐
│              History Event Handlers                       │
│  (Uses same messageHelpers and recentlySentTracker)     │
└──────────────────────────────────────────────────────────┘
```

## Benefits of This Architecture

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Testability**: Pure functions can be tested in isolation
3. **Maintainability**: Changes to one feature don't affect others
4. **Reusability**: Handlers can be reused for both streaming and history
5. **Readability**: Main hook is now ~400 lines instead of ~1200
6. **Type Safety**: Clear function signatures make types easier to add

## Example: Adding a New Event Type

If you need to handle a new event type (e.g., `code_execution`):

1. **Add handler to `streamEventHandlers.js`**:
```javascript
export function handleCodeExecution({ assistantMessageId, code, refs, setMessages }) {
  // Handler logic
  setMessages(prev => updateMessage(prev, assistantMessageId, (msg) => ({
    ...msg,
    codeExecutions: [...(msg.codeExecutions || []), code]
  })));
}
```

2. **Add handler to `historyEventHandlers.js`** (if needed):
```javascript
export function handleHistoryCodeExecution({ assistantMessageId, code, pairState, setMessages }) {
  // Similar logic for history
}
```

3. **Use in `useChatMessages.js`**:
```javascript
// In handleSendMessage stream handler:
if (contentType === 'code_execution') {
  handleCodeExecution({ assistantMessageId, code: event.content, refs, setMessages });
}

// In loadConversationHistory:
if (contentType === 'code_execution') {
  handleHistoryCodeExecution({ assistantMessageId, code: event.content, pairState, setMessages });
}
```

This modular approach makes it easy to extend functionality without touching the core hook logic.
