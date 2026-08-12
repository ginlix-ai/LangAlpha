/**
 * During a manual compaction (/compact or /offload) the turn runs with
 * isLoading=false, so the Stop button is surfaced via isCompacting instead.
 * It is the SOLE control in that state, so it must be reachable by an
 * accessible name. These tests assert which of the Send/Stop buttons renders.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([
    { name: 'Comps Analysis', command: 'comps-analysis', description: 'Comparable company analysis' },
    { name: 'DCF Model', command: 'dcf-model', description: 'DCF valuation' },
  ]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('@/lib/modelCapabilities', () => ({
  supportsXhighEffort: () => false,
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

function renderInput(
  props: {
    onSend?: ReturnType<typeof vi.fn>;
    onAction?: ReturnType<typeof vi.fn>;
    onStop?: ReturnType<typeof vi.fn>;
    isLoading?: boolean;
    isCompacting?: boolean;
  } = {},
) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const onSend = props.onSend ?? vi.fn();
  const onAction = props.onAction ?? vi.fn();
  const utils = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput
          onSend={onSend}
          onAction={onAction}
          onStop={props.onStop}
          isLoading={props.isLoading ?? false}
          isCompacting={props.isCompacting ?? false}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...utils, onSend, onAction };
}

// i18next is not initialized in these unit tests (the global setup doesn't
// import @/i18n), so react-i18next's t() returns the bare key. Match either
// the key or the resolved English string so the test stays correct under
// both setups.
const stopLabel = (content: string) => content === 'chat.stop' || content === 'Stop';

describe('ChatInput — compaction Stop button', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    // jsdom doesn't implement scrollIntoView; the slash menu's active-item
    // scroll effect calls it.
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('shows an accessible Stop button (and hides Send) while compacting with onStop', async () => {
    renderInput({ isLoading: false, isCompacting: true, onStop: vi.fn() });

    // The Stop button is the sole send/stop control during compaction; it must
    // carry an accessible name. findBy flushes the component's async skills/
    // model-metadata effects so no act() warning fires.
    expect(await screen.findByLabelText(stopLabel)).toBeInTheDocument();
    // The Send button must not be present in this state.
    expect(screen.queryByLabelText('Send message')).not.toBeInTheDocument();
  });

  it('shows the Send button when idle (not loading, not compacting)', async () => {
    renderInput({ isLoading: false, isCompacting: false });

    expect(await screen.findByLabelText('Send message')).toBeInTheDocument();
    expect(screen.queryByLabelText(stopLabel)).not.toBeInTheDocument();
  });
});
