/**
 * An automation's target identity: the bot its runs speak as, and the thread
 * they land in. A bot-owned automation speaks in one continuous thread unless
 * the reader presses the strategy control; the form always names a strategy,
 * so it must state that default itself. These cover the form's payload and the
 * more-options fold's bot picker.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { BotInfo } from '@/api/bots';
import AutomationInlineForm from '../AutomationInlineForm';
import MoreOptions from '../MoreOptions';
import { ALL_WORKSPACES, INITIAL_FORM, type FormState } from '../../utils/form';

const label = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts);

const h = vi.hoisted(() => ({
  bots: [] as unknown[],
  lastEnabled: true,
}));

// A realistic mock: the hook is disabled until its workspace is known, and a
// disabled read must not look like an answer with no bots.
vi.mock('@/hooks/useBots', () => ({
  useBots: (_workspaceId: string | null, enabled = true) => {
    h.lastEnabled = enabled;
    const answer = { bots: h.bots, max_bots: 20 };
    // A disabled read still sees what a previous open left in the cache, which
    // is how the folded line can keep naming its target.
    const cached = h.bots.length > 0 ? answer : undefined;
    return { data: enabled ? answer : cached, isLoading: false, isError: false };
  },
}));

vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws-1', name: 'Research' }] } }),
}));

vi.mock('@/hooks/useHomeWorkspace', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useHomeWorkspace')>()),
  useHomeWorkspaceId: () => 'home-1',
}));

vi.mock('@/hooks/useHomeTimezone', () => ({ useHomeTimezone: () => 'America/New_York' }));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('@/pages/Automations/hooks/useAutomationPlace', () => ({
  useAutomationPlace: () => ({
    homeId: 'home-1',
    runsInHome: () => false,
    label: () => 'Research',
  }),
}));

vi.mock('@/pages/Automations/hooks/useDeliveryOptions', () => ({
  useDeliveryOptions: () => ({
    options: null,
    isPending: false,
    error: null,
    isPlaceholderData: false,
    workspaceId: 'ws-1',
  }),
}));

function bot(overrides: Partial<BotInfo> = {}): BotInfo {
  return {
    bot_id: 'bot-1',
    user_id: 'user-1',
    workspace_id: 'ws-1',
    name: 'Macro analyst',
    role: 'analyst',
    persona: '',
    avatar: {},
    provider: null,
    model: null,
    skills: [],
    tools_allow: [],
    tools_deny: [],
    approval: 'ask',
    memory_scopes: ['workspace'],
    status: 'active',
    primary_thread_id: null,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

const botForm = (overrides: Partial<FormState> = {}): FormState => ({
  ...INITIAL_FORM,
  place: 'ws-1',
  bot_id: 'bot-1',
  name: 'Brief',
  instruction: 'Summarize.',
  ...overrides,
});

beforeEach(() => {
  h.bots = [bot()];
});

describe("an automation's target identity", () => {
  it("names the bot a create speaks as, and states continue while the strategy control is untouched", async () => {
    const onSubmit = vi.fn();
    renderWithProviders(
      <AutomationInlineForm
        initialValues={botForm({ thread_strategy: 'new' })}
        original={null}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
        loading={false}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalled());
    const submission = onSubmit.mock.calls[0][0] as { kind: string; payload: Record<string, unknown> };
    expect(submission.kind).toBe('create');
    expect(submission.payload.bot_id).toBe('bot-1');
    expect(submission.payload.thread_strategy).toBe('continue');
  });

  it('sends the strategy the reader pressed once they touch the control', async () => {
    const onSubmit = vi.fn();
    renderWithProviders(
      <AutomationInlineForm
        initialValues={botForm({ thread_strategy: 'new' })}
        original={null}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
        loading={false}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: new RegExp(label('automation.moreOptions')) }));
    fireEvent.click(screen.getByRole('button', { name: label('automation.newThreadEachRun') }));
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalled());
    const submission = onSubmit.mock.calls[0][0] as { payload: Record<string, unknown> };
    expect(submission.payload.thread_strategy).toBe('new');
  });
});

describe('the more-options bot picker', () => {
  it('offers the chosen workspace’s bots and describes the target thread', () => {
    const form = botForm({ thread_strategy: 'new' });
    const { container } = renderWithProviders(
      <MoreOptions form={form} patch={vi.fn()} open onOpenChange={vi.fn()} threadStrategy="continue" />,
    );

    expect(screen.getByText(label('automation.botTargetContinueNamed', { name: 'Macro analyst' }))).toBeInTheDocument();
    // The picker's read is enabled: a disabled read would draw no bots at all.
    expect(h.lastEnabled).toBe(true);
    // The selected bot wears the same mark it wears everywhere else.
    const mark = container.querySelector('[data-bot-mark]') as HTMLElement;
    expect(mark).not.toBeNull();
    expect(mark).toHaveAttribute('data-mark-kind', 'mark');
    fireEvent.click(screen.getByRole('button', { name: /Macro analyst/ }));
    const options = screen.getAllByRole('option').map((option) => option.textContent);
    expect(options).toEqual([label('automation.workspaceAnalyst'), 'Macro analyst']);
  });

  it('describes a fresh thread each run when the strategy is new', () => {
    const form = botForm();
    renderWithProviders(
      <MoreOptions form={form} patch={vi.fn()} open onOpenChange={vi.fn()} threadStrategy="new" />,
    );

    expect(screen.getByText(label('automation.botTargetNewNamed', { name: 'Macro analyst' }))).toBeInTheDocument();
  });

  it('states the target on the folded line as well', () => {
    renderWithProviders(
      <MoreOptions
        form={botForm()}
        patch={vi.fn()}
        open={false}
        onOpenChange={vi.fn()}
        threadStrategy="continue"
      />,
    );

    expect(
      screen.getByRole('button', { name: new RegExp(label('automation.moreOptions')) }),
    ).toHaveTextContent(label('automation.botTargetContinueNamed', { name: 'Macro analyst' }));
  });

  it('keeps a bot the workspace no longer lists selected instead of dropping it', () => {
    h.bots = [];
    const form = botForm();
    renderWithProviders(
      <MoreOptions form={form} patch={vi.fn()} open onOpenChange={vi.fn()} threadStrategy="continue" />,
    );

    fireEvent.click(screen.getByRole('button', { name: new RegExp(label('automation.botUnavailable')) }));
    expect(
      screen.getAllByRole('option').map((option) => option.textContent),
    ).toContain(label('automation.botUnavailable'));
  });

  it('applies a bot pick to the form', () => {
    const patch = vi.fn();
    const form = botForm({ bot_id: '' });
    renderWithProviders(
      <MoreOptions form={form} patch={patch} open onOpenChange={vi.fn()} threadStrategy="continue" />,
    );

    fireEvent.click(screen.getByRole('button', { name: /Workspace's analyst/ }));
    fireEvent.click(screen.getByRole('option', { name: 'Macro analyst' }));

    expect(patch).toHaveBeenCalledWith('bot_id', 'bot-1');
  });

  it('clears the bot when the place moves to another workspace', () => {
    const patch = vi.fn();
    const form = botForm({ place: ALL_WORKSPACES });
    renderWithProviders(
      <MoreOptions form={form} patch={patch} open onOpenChange={vi.fn()} threadStrategy="continue" />,
    );

    fireEvent.click(screen.getByRole('button', { name: new RegExp(label('agents.chiefOfStaff')) }));
    fireEvent.click(screen.getByRole('option', { name: 'Research' }));

    expect(patch).toHaveBeenCalledWith('bot_id', '');
    expect(patch).toHaveBeenCalledWith('place', 'ws-1');
  });
});
