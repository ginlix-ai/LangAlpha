/**
 * The workspace page's composer starts its thread in the chat view, so the
 * Subagents pick the composer holds rides the navigation there, Home's
 * included. Rendered with the real composer; only the data around it is
 * stubbed.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';

const mocks = vi.hoisted(() => ({ workspaceId: '7f3c1e2a-9b4d-4c6e-8a1f-2d3b4c5e6f70', isHome: false }));
const WORKSPACE_ID = mocks.workspaceId;

vi.mock('@/hooks/useWorkspace', () => ({
  useWorkspace: () => ({
    data: { workspace_id: mocks.workspaceId, name: 'Research', status: 'running', is_home: mocks.isHome },
    error: null,
  }),
}));
vi.mock('../../hooks/useWorkspaceFiles', () => ({
  useWorkspaceFiles: () => ({ files: [], loading: false, error: null, refresh: vi.fn() }),
}));
vi.mock('../FilePanel', () => ({ default: () => null, SYSTEM_DIR_PREFIXES: [] }));
vi.mock('../SandboxSettingsPanel', () => ({ default: () => null }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getWorkspaceThreads: vi.fn().mockResolvedValue({ threads: [], total: 0 }),
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

import ThreadGallery from '../ThreadGallery';

function ChatPage() {
  const state = useLocation().state as { subagentsAllowed?: boolean; workspaceId?: string } | null;
  return (
    <div data-testid="chat-page" data-workspace={state?.workspaceId}>
      {String(state?.subagentsAllowed)}
    </div>
  );
}

function renderGallery() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.user.preferences(), { other_preference: { subagents_default: true } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[`/chat/${WORKSPACE_ID}`]}>
        <Routes>
          <Route
            path="/chat/:workspaceId"
            element={<ThreadGallery workspaceId={WORKSPACE_ID} onBack={vi.fn()} onThreadSelect={vi.fn()} />}
          />
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

describe('ThreadGallery: the Subagents toggle', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    mocks.isHome = false;
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('shows the default on a PTC workspace, and a flip rides the navigation into the new thread', async () => {
    renderGallery();
    await screen.findByRole('textbox');
    expect(pill()).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(pill()!);
    expect(pill()).toHaveAttribute('aria-pressed', 'false');
    await send('Build me a DCF');

    const page = await screen.findByTestId('chat-page');
    expect(page).toHaveTextContent('false');
    expect(page).toHaveAttribute('data-workspace', WORKSPACE_ID);
  });

  it("shows on Home too, and a flip rides into Home's new thread", async () => {
    mocks.isHome = true;
    renderGallery();
    await screen.findByRole('textbox');
    expect(pill()).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(pill()!);
    await send('What is AAPL doing?');

    const page = await screen.findByTestId('chat-page');
    expect(page).toHaveAttribute('data-workspace', WORKSPACE_ID);
    expect(page).toHaveTextContent('false');
  });
});
