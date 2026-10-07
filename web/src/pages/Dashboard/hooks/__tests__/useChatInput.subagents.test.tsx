/**
 * The dashboard's composers start their PTC thread in the chat view, so the
 * Subagents pick the composer holds rides the navigation there, and only a
 * pick: left alone, the new thread gets the user's default from the server.
 * Rendered with the real composer; only the data around it is stubbed.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type React from 'react';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';
import type { WidgetRenderProps } from '../../widgets/types';

const mocks = vi.hoisted(() => ({
  workspaces: [{ workspace_id: 'ws-1', name: 'Workspace 1', status: 'active' }],
  getFlashWorkspace: vi.fn(),
}));

vi.mock('@/hooks/useIsMobile', () => ({
  useIsMobile: () => false,
  getIsMobileSnapshot: () => false,
}));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: mocks.workspaces, total: 1 } }),
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getWorkspaceThreads: vi.fn().mockResolvedValue({ threads: [], total: 0 }),
  getFlashWorkspace: mocks.getFlashWorkspace,
}));

// Read from the cache, so a test sets the user's default the way a
// preferences read would.
vi.mock('@/hooks/usePreferences', async () => {
  const { useQuery } = await import('@tanstack/react-query');
  const { queryKeys: keys } = await import('@/lib/queryKeys');
  return {
    usePreferences: () => {
      const { data } = useQuery({ queryKey: keys.user.preferences(), queryFn: () => null, enabled: false });
      return { preferences: data ?? null, isLoading: false, isLoaded: data !== undefined };
    },
  };
});

import ChatInputCard from '../../components/ChatInputCard';
import ConversationWidget from '../../widgets/definitions/ConversationWidget';

function ConversationComposer() {
  const props = { instance: { id: 'conversation-1' }, updateConfig: vi.fn() } as unknown as WidgetRenderProps<Record<string, never>>;
  return <ConversationWidget {...props} />;
}

function ChatPage() {
  const state = useLocation().state as { subagentsAllowed?: boolean } | null;
  return <div data-testid="chat-page">{String(state?.subagentsAllowed)}</div>;
}

function renderComposer(Composer: React.ComponentType, subagentsDefault: boolean) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.user.preferences(), { other_preference: { subagents_default: subagentsDefault } });
  // A send waits for the flags; none are on here.
  queryClient.setQueryData(queryKeys.features.list(), []);
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/dashboard']}>
        <Routes>
          <Route path="/dashboard" element={<Composer />} />
          <Route path="/chat/t/:threadId" element={<ChatPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const pill = () => screen.queryByRole('button', { name: /^Subagents$/ });

async function send(text: string) {
  fireEvent.change(screen.getByRole('textbox'), { target: { value: text } });
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
  });
}

describe.each([
  ['the floating composer', ChatInputCard],
  ['the conversation widget', ConversationComposer],
])('%s: the Subagents toggle', (_label, Composer) => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    mocks.getFlashWorkspace.mockReset();
    mocks.getFlashWorkspace.mockResolvedValue({ workspace_id: 'flash-ws' });
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('shows the default in PTC, and a flip rides the navigation into the new thread', async () => {
    renderComposer(Composer, true);
    expect(pill()).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(pill()!);
    expect(pill()).toHaveAttribute('aria-pressed', 'false');
    await send('Build me a DCF');

    expect(await screen.findByTestId('chat-page')).toHaveTextContent('false');
  });

  it('shows the default, and sends none when left alone', async () => {
    renderComposer(Composer, false);
    expect(pill()).toHaveAttribute('aria-pressed', 'false');
    await send('Build me a DCF');

    expect(await screen.findByTestId('chat-page')).toHaveTextContent('undefined');
  });

  it('is hidden in Flash, whose send names none', async () => {
    renderComposer(Composer, true);
    fireEvent.click(pill()!);
    fireEvent.click(screen.getByRole('button', { name: 'PTC' }));
    expect(pill()).toBeNull();
    await send('What is AAPL doing?');

    expect(await screen.findByTestId('chat-page')).toHaveTextContent('undefined');
  });
});
