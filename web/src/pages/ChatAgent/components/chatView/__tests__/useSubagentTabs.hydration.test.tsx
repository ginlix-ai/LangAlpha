import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { renderHook, act, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { useCardState } from '../../../hooks/useCardState';
import { findWorkflowChildOwner } from '../../../session/subagents/workflowRunState';
import { useSubagentTabs } from '../useSubagentTabs';

// The status probe is a separate round trip from the transcript fetch; it is
// deliberately the SLOWER of the two here, since nothing orders them in the app.
vi.mock('../../../utils/api', () => ({
  getSubagentTaskStatus: vi.fn(async () => {
    await new Promise((r) => setTimeout(r, 50));
    return { status: 'completed', error: null };
  }),
}));

const CHILD = 'task:o5WVKA';

/** Transcript the lazy hydration endpoint materializes for a workflow child. */
const HYDRATED_MESSAGES = [
  { role: 'user', content: 'Research AAPL' },
  { role: 'assistant', content: 'AAPL brief', isStreaming: false },
];

interface HistoryEntry {
  taskId: string;
  description: string;
  type: string;
  status: string;
  messages: unknown[];
  ownerTaskId?: string;
}

function harness() {
  const history: Record<string, HistoryEntry> = {};
  let resolveHydrate: (() => void) | undefined;

  const hydrateTaskTranscript = vi.fn(
    async (agentId: string, meta?: { description?: string; type?: string; status?: string }) => {
      await new Promise<void>((r) => { resolveHydrate = r; });
      history[agentId] = {
        taskId: agentId,
        description: meta?.description || '',
        type: meta?.type || 'general-purpose',
        status: meta?.status || 'completed',
        messages: HYDRATED_MESSAGES,
      };
      return true;
    },
  );

  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <MemoryRouter>{children}</MemoryRouter>
  );

  const rendered = renderHook(
    ({ initialTaskId }: { initialTaskId?: string }) => {
      const cardState = useCardState();
      const [activeAgentId, setActiveAgentId] = React.useState('main');
      const activeAgentIdRef = React.useRef('main');
      activeAgentIdRef.current = activeAgentId;
      const tabs = useSubagentTabs({
        threadId: 'thread-1',
        workspaceId: 'ws-1',
        initialTaskId,
        isLoadingHistory: false,
        activeAgentId,
        setActiveAgentId,
        cards: cardState.cards,
        updateSubagentCard: cardState.updateSubagentCard,
        getSubagentHistory: ((id: string) =>
          history[id] ? { ...history[id], agentId: id } : null) as never,
        resolveSubagentIdToAgentId: ((id: string) => id) as never,
        hydrateTaskTranscript: hydrateTaskTranscript as never,
        saveScrollPosition: () => {},
        scrollPositionsRef: { current: {} },
        skipSubagentAutoScrollRef: { current: false },
        activeAgentIdRef,
        resolvedThreadIdRef: { current: 'thread-1' },
      });
      return { ...tabs, cards: cardState.cards };
    },
    { wrapper, initialProps: {} as { initialTaskId?: string } },
  );

  return { rendered, history, flushHydrate: () => resolveHydrate?.() };
}

describe('useSubagentTabs — workflow-child transcript hydration', () => {
  beforeEach(() => vi.clearAllMocks());

  it('lands the lazily hydrated transcript on a deep-linked child card', async () => {
    const { rendered, flushHydrate } = harness();

    // Deep link / reload straight onto a workflow child: replay projected no
    // lane for it, so the card is created empty and the transcript hydrates.
    rendered.rerender({ initialTaskId: 'o5WVKA' });

    await waitFor(() =>
      expect(rendered.result.current.cards[`subagent-${CHILD}`]).toBeDefined(),
    );

    await act(async () => {
      flushHydrate();
      await Promise.resolve();
    });

    await waitFor(() => {
      const sd = rendered.result.current.cards[`subagent-${CHILD}`]?.subagentData;
      expect(sd?.messages).toHaveLength(HYDRATED_MESSAGES.length);
    });
  });

  it('lands the transcript when the child is opened from the workflow drill-in', async () => {
    const { rendered, flushHydrate } = harness();

    act(() => {
      rendered.result.current.handleOpenSubagentTask({
        subagentId: CHILD,
        description: 'AAPL',
        type: 'research',
        status: 'completed',
        ownerTaskId: 'task:8A1Yqw',
      } as never);
    });

    await act(async () => {
      flushHydrate();
      await Promise.resolve();
    });

    await waitFor(() => {
      const sd = rendered.result.current.cards[`subagent-${CHILD}`]?.subagentData;
      expect(sd?.messages).toHaveLength(HYDRATED_MESSAGES.length);
    });
  });

  it('keeps a live streaming card active across a refresh', async () => {
    const { rendered } = harness();

    // Seed a card the way a live subagent stream does: active, non-terminal.
    act(() => {
      rendered.result.current.handleOpenSubagentTask({
        subagentId: 'task:live1',
        description: 'live child',
        type: 'research',
        status: 'running',
      } as never);
    });

    await waitFor(() =>
      expect(
        rendered.result.current.cards['subagent-task:live1']?.subagentData?.isActive,
      ).toBe(true),
    );

    // A second open must not settle a card that is still streaming.
    act(() => {
      rendered.result.current.handleSelectAgent('task:live1');
    });

    expect(
      rendered.result.current.cards['subagent-task:live1']?.subagentData?.isActive,
    ).toBe(true);
  });
});

describe('findWorkflowChildOwner', () => {
  it('resolves a child to its dispatching run', () => {
    const entries = {
      'task:8A1Yqw': {
        workflowRun: {
          children: [
            { childTaskId: 'erHb5Q', label: 'MSFT', subagentType: 'research' },
            { childTaskId: 'o5WVKA', label: 'AAPL', subagentType: 'research' },
          ],
        },
      },
      'task:plain': {},
    } as never;

    expect(findWorkflowChildOwner(entries, 'o5WVKA')).toMatchObject({
      ownerTaskId: 'task:8A1Yqw',
      child: { label: 'AAPL', subagentType: 'research' },
    });
    expect(findWorkflowChildOwner(entries, 'nope')).toBeNull();
    expect(findWorkflowChildOwner(null, 'o5WVKA')).toBeNull();
  });
});
