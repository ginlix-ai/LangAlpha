import { describe, expect, it, vi, beforeEach } from 'vitest';
import type { ReactNode } from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

interface Prompt {
  offered: boolean;
  threadId: string | null;
  pin: (threadId: string) => void;
  dismiss: () => void;
  connect: () => void;
}

const h = vi.hoisted(() => ({
  prompt: null as null | Prompt,
  options: { enabled: true, apps: {} as Record<string, unknown> },
}));
vi.mock('../OnboardingProvider', () => ({ useChatAppsPrompt: () => h.prompt }));
vi.mock('@/hooks/useLinkedChatApps', () => ({
  deliveryOptionsQuery: () => ({ queryKey: ['delivery-options'], queryFn: async () => h.options }),
}));

import { ChatAppsPromptCard } from '../ChatAppsPromptCard';

function renderCard(threadId = 't1', state: { active?: boolean; idle?: boolean; replied?: boolean } = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { active = true, idle = true, replied = true } = state;
  return render(<ChatAppsPromptCard threadId={threadId} active={active} idle={idle} replied={replied} />, { wrapper });
}

function offer(over: Partial<Prompt> = {}): Prompt {
  return { offered: true, threadId: null, pin: vi.fn(), dismiss: vi.fn(), connect: vi.fn(), ...over };
}

describe('ChatAppsPromptCard', () => {
  beforeEach(() => {
    h.options = { enabled: true, apps: {} };
  });

  it('shows nothing where no prompt is offered', () => {
    h.prompt = null;
    const { container } = renderCard();
    expect(container).toBeEmptyDOMElement();
    h.prompt = offer({ offered: false });
    renderCard();
    expect(screen.queryByTestId('chat-apps-prompt')).toBeNull();
  });

  it('offers Not now and Connect a chat app, and stays with the first thread it shows in', async () => {
    h.prompt = offer();
    renderCard('t1');
    expect(await screen.findByText('Ask from your chat apps too')).toBeInTheDocument();
    expect(h.prompt.pin).toHaveBeenCalledWith('t1');
    await userEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(h.prompt.dismiss).toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Connect a chat app' }));
    expect(h.prompt.connect).toHaveBeenCalled();
  });

  it('shows in the pinned thread without pinning again, and in no other', async () => {
    h.prompt = offer({ threadId: 't1' });
    renderCard('t1');
    expect(await screen.findByTestId('chat-apps-prompt')).toBeInTheDocument();
    expect(h.prompt.pin).not.toHaveBeenCalled();
    const other = renderCard('t2');
    await new Promise((r) => setTimeout(r, 20));
    expect(other.container).toBeEmptyDOMElement();
  });

  it('shows and pins only on the thread on screen, once its reply is in and no turn runs', async () => {
    h.prompt = offer();
    const hidden = renderCard('t1', { active: false });
    const running = renderCard('t2', { idle: false });
    const empty = renderCard('t3', { replied: false });
    await new Promise((r) => setTimeout(r, 20));
    expect(hidden.container).toBeEmptyDOMElement();
    expect(running.container).toBeEmptyDOMElement();
    expect(empty.container).toBeEmptyDOMElement();
    expect(h.prompt.pin).not.toHaveBeenCalled();
    renderCard('t4');
    await screen.findByTestId('chat-apps-prompt');
    expect(h.prompt.pin).toHaveBeenCalledWith('t4');
  });

  it('stays away once a chat app is linked, or where there is no messaging service', async () => {
    h.prompt = offer();
    h.options = { enabled: true, apps: { slack: {} } };
    const linked = renderCard();
    await new Promise((r) => setTimeout(r, 20));
    expect(linked.container).toBeEmptyDOMElement();
    h.options = { enabled: false, apps: {} };
    const none = renderCard();
    await new Promise((r) => setTimeout(r, 20));
    expect(none.container).toBeEmptyDOMElement();
    expect(h.prompt.pin).not.toHaveBeenCalled();
  });
});
