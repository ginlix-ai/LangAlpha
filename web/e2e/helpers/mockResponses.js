/**
 * Default mock responses and data factories for E2E tests.
 * Event shapes validated against real SSE captures from production.
 */

// All endpoints the app calls on page load, with minimal valid responses.
export const defaultResponses = {
  'GET /users/me': {
    user_id: 'local-dev-user',
    name: 'Test User',
    email: 'test@test.com',
    onboarding_completed: true,
    has_api_key: true,
    has_oauth_token: false,
  },
  'GET /users/me/preferences': {
    theme: 'dark',
    locale: 'en-US',
    timezone: 'America/New_York',
    // Onboarding state of a fully-caught-up returning user, so the welcome
    // guides added in #264 (page-intro modals + getting-started card) never
    // pop over the UI under test and intercept clicks. Without this, every
    // page intro is eligible (empty pageIntrosSeen) and its [role="dialog"]
    // overlay blocks Playwright clicks -> 30s timeouts. Page-intro ids come
    // from src/pages/Onboarding/registry/pageIntros.ts -- add new ids here
    // when intros are added. lastSeenReleaseVersion stays null because
    // unseenReleases() returns [] for null (no What's-New backlog); firstRunAt
    // is stamped so the provider treats this as an established user.
    // Tests that exercise onboarding (e.g. the personalization banner) override
    // the relevant fields locally.
    other_preference: {
      onboarding: {
        version: 1,
        pageIntrosSeen: { chat: 1, thread: 1, dashboard: 1 },
        gettingStartedDoneAt: {},
        gettingStartedDismissedAt: 1,
        lastSeenReleaseVersion: null,
        firstRunAt: 1,
      },
    },
  },
  'GET /workspaces': { workspaces: [], total: 0, limit: 20, offset: 0 },
  'POST /workspaces/flash': {
    workspace_id: 'ws-flash',
    name: 'Flash',
    status: 'flash',
    config: {},
  },
  'GET /models': {
    model_metadata: {
      'claude-sonnet-4-20250514': {
        display_name: 'Claude Sonnet 4',
        provider: 'anthropic',
      },
    },
  },
  'GET /skills': { skills: [] },
  'GET /market-data/snapshots/indexes': { snapshots: [] },
  'GET /market-data/intraday/indexes/*': { data: [] },
  'GET /news': { results: [], count: 0, next_cursor: null },
  'GET /insights/today': { insights: [] },
  'GET /users/me/watchlists': {
    watchlists: [{ watchlist_id: 'wl-1', name: 'Default' }],
    total: 1,
  },
  'GET /users/me/watchlists/*/items': { items: [], total: 0 },
  'GET /users/me/portfolio': { holdings: [] },
  'GET /market-data/snapshots/stocks': { snapshots: [] },
  'GET /calendar/earnings': { data: [], count: 0 },
};

// ── Sample data factories ──

export const sampleWorkspace = (overrides = {}) => ({
  workspace_id: 'ws-1',
  name: 'Research',
  description: 'Main workspace',
  status: 'ready',
  config: {},
  created_at: '2025-01-01T00:00:00Z',
  updated_at: '2025-01-01T00:00:00Z',
  ...overrides,
});

export const sampleThread = (overrides = {}) => ({
  thread_id: 'th-1',
  workspace_id: 'ws-1',
  title: 'Test conversation',
  created_at: '2025-01-01T00:00:00Z',
  ...overrides,
});

export const samplePortfolioHolding = (overrides = {}) => ({
  user_portfolio_id: 'ph-1',
  symbol: 'AAPL',
  quantity: '10',
  average_cost: '150.00',
  name: 'Apple Inc.',
  exchange: 'NASDAQ',
  instrument_type: 'stock',
  notes: '',
  ...overrides,
});

export const sampleWatchlistItem = (overrides = {}) => ({
  watchlist_item_id: 'wi-1',
  symbol: 'TSLA',
  name: 'Tesla Inc.',
  exchange: 'NASDAQ',
  instrument_type: 'stock',
  ...overrides,
});

export const sampleNewsArticle = (overrides = {}) => ({
  id: 'news-1',
  title: 'Markets Rally on Strong Earnings',
  published_at: '2025-01-01T12:00:00Z',
  source: { name: 'Reuters', favicon_url: '' },
  image_url: '',
  tickers: ['AAPL'],
  has_sentiment: true,
  ...overrides,
});

export const sampleIndexSnapshot = (symbol, price, change) => ({
  symbol,
  name: symbol,
  price,
  change,
  change_percent: ((change / (price - change)) * 100).toFixed(2),
  previous_close: price - change,
});

// ── SSE event factories (shapes match real captured events) ──

export const sseEvents = {
  userMessage: (content, turnIndex = 0) => ({
    event: 'user_message',
    data: {
      thread_id: 'th-1',
      turn_index: turnIndex,
      content,
      timestamp: new Date().toISOString(),
      metadata: { msg_type: 'ptc', workspace_id: 'ws-1' },
    },
  }),

  messageChunk: (content, contentType = 'text', turnIndex = 0) => ({
    event: 'message_chunk',
    data: {
      thread_id: 'th-1',
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      content,
      content_type: contentType,
      turn_index: turnIndex,
    },
  }),

  finishStop: (turnIndex = 0) => ({
    event: 'message_chunk',
    data: {
      thread_id: 'th-1',
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      content: '',
      content_type: 'text',
      finish_reason: 'stop',
      turn_index: turnIndex,
    },
  }),

  finishToolCalls: (turnIndex = 0) => ({
    event: 'message_chunk',
    data: {
      thread_id: 'th-1',
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      content: '',
      content_type: 'text',
      finish_reason: 'tool_calls',
      turn_index: turnIndex,
    },
  }),

  toolCalls: (calls, turnIndex = 0) => ({
    event: 'tool_calls',
    data: {
      thread_id: 'th-1',
      agent: 'model:test',
      id: 'lc_run--test',
      role: 'assistant',
      tool_calls: calls.map((c) => ({
        name: c.name,
        args: c.args || {},
        id: c.id || `toolu_${Date.now()}`,
        type: 'tool_call',
      })),
      finish_reason: 'tool_calls',
      turn_index: turnIndex,
    },
  }),

  toolCallResult: (toolCallId, content, artifact = null, turnIndex = 0) => ({
    event: 'tool_call_result',
    data: {
      thread_id: 'th-1',
      agent: 'tools',
      id: `result-${toolCallId}`,
      role: 'assistant',
      content,
      content_type: 'text',
      tool_call_id: toolCallId,
      turn_index: turnIndex,
      ...(artifact ? { artifact } : {}),
    },
  }),

  interrupt: (interruptId, plan = 'Here is my plan:\n1. Step one\n2. Step two') => ({
    event: 'interrupt',
    data: {
      thread_id: 'th-1',
      turn_index: 0,
      interrupts: [
        {
          id: interruptId,
          type: 'plan_approval',
          action_request: {
            action: 'SubmitPlan',
            args: { plan },
          },
        },
      ],
    },
  }),

  artifact: (type, payload) => ({
    event: 'artifact',
    data: {
      artifact_type: type,
      artifact_id: `art-${Date.now()}`,
      agent: 'ptc',
      timestamp: new Date().toISOString(),
      status: 'completed',
      payload,
    },
  }),

  todoUpdate: (todos) => ({
    event: 'artifact',
    data: {
      artifact_type: 'todo_update',
      artifact_id: `todo-${Date.now()}`,
      agent: 'ptc',
      timestamp: new Date().toISOString(),
      status: 'completed',
      payload: {
        todos,
        total: todos.length,
        completed: todos.filter((t) => t.status === 'completed').length,
        in_progress: todos.filter((t) => t.status === 'in_progress').length,
        pending: todos.filter((t) => t.status === 'pending').length,
      },
    },
  }),

  replayDone: (threadId = 'th-1') => ({
    event: 'replay_done',
    data: { thread_id: threadId },
  }),

  error: (message) => ({
    event: 'error',
    data: { error: message },
  }),

  workspaceStatus: (status) => ({
    event: 'workspace_status',
    data: { status },
  }),

  contextWindow: (inputTokens = 1000, outputTokens = 100) => ({
    event: 'context_window',
    data: {
      thread_id: 'th-1',
      agent: 'model:test',
      action: 'token_usage',
      signal: 'complete',
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      total_tokens: inputTokens + outputTokens,
      threshold: 120000,
    },
  }),

  keepalive: () => ({
    event: 'keepalive',
    data: { status: 'alive' },
  }),

  creditUsage: (totalCredits = 1.5) => ({
    event: 'credit_usage',
    data: {
      thread_id: 'th-1',
      tokens: {
        input_tokens: 10000,
        output_tokens: 500,
        total_tokens: 10500,
      },
      total_credits: totalCredits,
      timestamp: new Date().toISOString(),
    },
  }),

  createWorkspaceInterrupt: (interruptId, name, description) => ({
    event: 'interrupt',
    data: {
      thread_id: 'th-1',
      interrupt_id: interruptId,
      finish_reason: 'interrupt',
      action_requests: [{
        type: 'create_workspace',
        workspace_name: name,
        workspace_description: description,
      }],
    },
  }),

  startQuestionInterrupt: (interruptId, workspaceId, question) => ({
    event: 'interrupt',
    data: {
      thread_id: 'th-1',
      interrupt_id: interruptId,
      finish_reason: 'interrupt',
      action_requests: [{
        type: 'start_question',
        workspace_id: workspaceId,
        question: question,
      }],
    },
  }),

  navigateToWorkspaceResult: (toolCallId, workspaceId, question) => ({
    event: 'tool_call_result',
    data: {
      thread_id: 'th-1',
      agent: 'tools',
      id: `result-${toolCallId}`,
      role: 'assistant',
      content: JSON.stringify({ success: true, workspace_id: workspaceId, question, action: 'navigate_to_workspace' }),
      content_type: 'text',
      tool_call_id: toolCallId,
    },
  }),

  createWorkspaceResult: (toolCallId, workspaceId, workspaceName) => ({
    event: 'tool_call_result',
    data: {
      thread_id: 'th-1',
      agent: 'tools',
      id: `result-${toolCallId}`,
      role: 'assistant',
      content: JSON.stringify({ success: true, workspace_id: workspaceId, workspace_name: workspaceName }),
      content_type: 'text',
      tool_call_id: toolCallId,
    },
  }),

  askUserInterrupt: (interruptId, questionId, question, options = null) => ({
    event: 'interrupt',
    data: {
      thread_id: 'th-1',
      interrupt_id: interruptId,
      finish_reason: 'interrupt',
      action_requests: [{
        type: 'ask_user_question',
        question_id: questionId,
        question: question,
        ...(options ? { options } : {}),
      }],
    },
  }),
};
