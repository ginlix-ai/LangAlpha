/**
 * What a thread host says on its composer's controls. The model pill takes
 * the most urgent notice alone: a lapsed sign-in, then a Claude plan at or
 * near its limit, a held Default, a fallback that answered, and last the
 * offer to make the pick the default. The plan's sentence follows the plan
 * behind the workspace's token (Home's for the Chief of Staff): used up under
 * Stop, on usage credits the user chose, near from 80% of a window, and
 * nothing on another model or family. A fallback is offered only once the
 * turn is over and while the composer still names the troubled model.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import type { ClaudeCodePlan, ClaudeCodePlanHeld, ClaudeCodePlanWindow } from '@/api/model';
import type { PlanOut } from '@/lib/claudeCodePlan';
import { queryKeys } from '@/lib/queryKeys';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import type { FallbackSuggestion } from '../../../session/types';
import type { useThreadModel } from '../../../hooks/useThreadModel';

const h = vi.hoisted(() => ({
  plan: null as ClaudeCodePlan | null,
  homePlan: null as ClaudeCodePlan | null,
  planOut: 'stop' as PlanOut,
  meter: undefined as { credential: { kind: 'saved' } | null; checkAfter: number | null } | undefined,
  steps: 'none' as 'pending' | 'none',
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    metadata: {
      'claude-opus-5-5-code': { provider: 'claude-code', sdk: 'claude-code', display_name: 'Opus Code' },
      'claude-sonnet-5-5-code': { provider: 'claude-code', sdk: 'claude-code', display_name: 'Sonnet Code' },
      'model-alpha': { provider: 'openai', display_name: 'Alpha' },
      'model-beta': { provider: 'openai', display_name: 'Beta' },
    },
  }),
}));
vi.mock('@/hooks/useWorkspaceClaudeCode', () => ({
  useTurnClaudeCode: (home: boolean) => ({ plan: home ? h.homePlan : h.plan, meter: h.meter }),
}));
vi.mock('@/hooks/useClaudeCodeSteps', () => ({ useClaudeCodeSteps: () => ({ status: h.steps }) }));
vi.mock('@/components/modelAccess/PlanCheck', () => ({
  CheckNowFor: ({ target }: { target: { credential: { kind: string } } }) => <button type="button">{`Check now:${target.credential.kind}`}</button>,
}));
vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'America/New_York', now: Date.parse('2026-11-06T19:05:00Z') }),
  usePlanOut: () => ({ planOut: h.planOut }),
}));

import { NoticeHeader } from '@/components/ui/chat-input.notices';
import { useComposerNotices } from '../useComposerNotices';

type ThreadModel = Parameters<typeof useComposerNotices>[0]['threadModel'];

const NO_SUBAGENTS = { offer: null, closeOffer: () => {}, expireOffer: () => {} };

function threadModel(model: string, over: Partial<ReturnType<typeof useThreadModel>> = {}): ThreadModel {
  return {
    model, lapsed: null, heldDefault: null, offer: null,
    dismissOffer: () => {}, expireOffer: () => {}, pickModel: vi.fn(() => Promise.resolve(true)),
    ...over,
  };
}

function Notices({ model, fallback = null, running = false, chiefOfStaff = false, onClear = () => {} }: {
  model: ThreadModel;
  fallback?: FallbackSuggestion | null;
  running?: boolean;
  chiefOfStaff?: boolean;
  onClear?: () => void;
}) {
  const notices = useComposerNotices({
    threadId: 'thread-1',
    threadModel: model,
    subagents: NO_SUBAGENTS,
    chat: { fallbackSuggestion: fallback, clearFallbackSuggestion: onClear, isLoading: running },
    workspaceId: chiefOfStaff ? 'home-1' : 'ws-1',
    chiefOfStaff,
  });
  const notice = notices.model;
  // Drawn as the picker heads with it, a frame that gives the content its sentence.
  return notice ? <div data-key={notice.key} data-tone={notice.tone}><NoticeHeader notice={notice} onClose={() => {}} /></div> : null;
}

// The fallback names models as the cached catalog does.
function client() {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(queryKeys.models.all, {
    model_metadata: { 'model-alpha': { display_name: 'Alpha' }, 'model-beta': { display_name: 'Beta' } },
  });
  return queryClient;
}

const show = (ui: React.ReactElement) => renderWithProviders(ui, { queryClient: client(), route: '/chat' });

const limited = (
  window: ClaudeCodePlanWindow, credits: ClaudeCodePlan['credits'], family: ClaudeCodePlanHeld['family'] = null,
): ClaudeCodePlan => ({
  five_hour: { used: 1, resets_at: '2026-11-06T20:40:00Z' }, seven_day: null,
  limits: [{ window, family, until: '2026-11-06T20:40:00Z', reset_known: true }], credits, observed_at: '2026-11-06T19:00:00Z',
});

const LAPSED = { model: 'model-alpha', provider: 'codex-oauth' as const, reason: 'expired' as const };
const OFFER = { model: 'model-alpha', defaultModel: 'model-beta' };

beforeEach(() => {
  h.plan = null;
  h.homePlan = null;
  h.planOut = 'stop';
  h.meter = { credential: { kind: 'saved' }, checkAfter: null };
  h.steps = 'none';
});

describe('the model notice', () => {
  it('takes the most urgent one alone, a lapsed sign-in first', () => {
    h.plan = limited('five_hour', 'on');
    show(
      <Notices
        model={threadModel('model-alpha', { lapsed: LAPSED, offer: OFFER })}
        fallback={{ fromModel: 'model-alpha', toModel: 'model-beta' }}
      />,
    );
    expect(screen.getAllByRole('status')).toHaveLength(1);
    expect(screen.getByRole('status')).toHaveTextContent('Your ChatGPT sign-in has expired. Reconnect it to use Alpha.');
    expect(screen.getByRole('button', { name: 'Reconnect' })).toBeTruthy();
  });

  it('says a plan that is not connected at all, with the way to connect it', () => {
    show(<Notices model={threadModel('model-alpha', { lapsed: { ...LAPSED, reason: 'missing' } })} />);
    expect(screen.getByRole('status')).toHaveTextContent(
      "Your ChatGPT plan isn't connected. Connect it to use Alpha, or pick another model.",
    );
    expect(screen.getByRole('button', { name: 'Connect' })).toBeTruthy();
  });

  it('puts a fallback before the default offer, and the offer last', () => {
    const { unmount } = show(
      <Notices model={threadModel('model-alpha', { offer: OFFER })} fallback={{ fromModel: 'model-alpha', toModel: 'model-beta' }} />,
    );
    expect(document.querySelector('[data-tone]')).toHaveAttribute('data-tone', 'warning');
    expect(screen.getByRole('status')).toHaveTextContent('Alpha is having trouble, so this answer came from Beta.');
    unmount();

    show(<Notices model={threadModel('model-alpha', { offer: OFFER })} />);
    expect(document.querySelector('[data-tone]')).toHaveAttribute('data-tone', 'offer');
    expect(screen.getByRole('button', { name: 'Make Alpha my default' })).toBeTruthy();
  });

  it('says a held Default only where the cloud steps would let it run', () => {
    const held = { heldDefault: { model: 'claude-opus-5-5-code', fallback: 'model-alpha' } };
    const { unmount } = show(<Notices model={threadModel('model-alpha', held)} chiefOfStaff />);
    expect(screen.queryByRole('status')).toBeNull();
    unmount();

    h.steps = 'pending';
    show(<Notices model={threadModel('model-alpha', held)} chiefOfStaff />);
    expect(screen.getByRole('button', { name: 'Set up Claude Code' })).toBeTruthy();
  });
});

describe('the fallback', () => {
  const suggestion: FallbackSuggestion = { fromModel: 'model-alpha', toModel: 'model-beta' };

  it('switches the thread to the working model, and clears once the pick is saved', async () => {
    const model = threadModel('model-alpha');
    const onClear = vi.fn();
    show(<Notices model={model} fallback={suggestion} onClear={onClear} />);
    fireEvent.click(screen.getByRole('button', { name: 'Switch to Beta' }));
    expect(model.pickModel).toHaveBeenCalledWith('model-beta');
    await vi.waitFor(() => expect(onClear).toHaveBeenCalledOnce());
  });

  // Two models can share a name, one on a plan and one on credits, so the
  // switch says which payer the thread moves onto.
  it('says who pays for the model it would switch to', () => {
    show(<Notices model={threadModel('model-alpha')} fallback={{ ...suggestion, paidBy: 'byok' }} />);
    expect(screen.getByRole('button', { name: 'Switch to Beta (Your key)' })).toBeTruthy();
  });

  it('says nothing while a turn runs, or once the composer shows the working model', () => {
    show(<Notices model={threadModel('model-alpha')} fallback={suggestion} running />);
    show(<Notices model={threadModel('model-beta')} fallback={suggestion} />);
    expect(screen.queryByRole('status')).toBeNull();
  });

  // The warning is about a send on the troubled model, so a third pick both
  // drops it and lets that pick's own default offer through.
  it('goes once the composer moves off the troubled model, even to a third one', () => {
    show(
      <Notices
        model={threadModel('model-gamma', { offer: { model: 'model-gamma', defaultModel: 'model-beta' } })}
        fallback={suggestion}
      />,
    );
    expect(screen.queryByText(/is having trouble/)).toBeNull();
    expect(document.querySelector('[data-tone]')).toHaveAttribute('data-tone', 'offer');
  });
});

describe("the Claude plan's notice", () => {
  const code = threadModel('claude-opus-5-5-code');

  it("reads the plan the Chief of Staff's turns run on in Home", () => {
    h.homePlan = limited('five_hour', 'on');
    show(<Notices model={code} chiefOfStaff />);
    expect(screen.getByRole('status')).toHaveTextContent(
      "Your Claude plan's 5-hour limit is used up until 3:40 PM. Pick another model to keep going.",
    );
  });

  it('says turns run on usage credits the user chose', () => {
    h.plan = limited('five_hour', 'on');
    h.planOut = 'credits';
    show(<Notices model={code} />);
    expect(screen.getByRole('status')).toHaveTextContent(
      "Your Claude plan's 5-hour limit is used up, so Claude Code runs on your usage credits until it resets at 3:40 PM.",
    );
  });

  it('offers Check now where the account has the usage credits the user chose off', () => {
    h.plan = limited('five_hour', 'off');
    h.planOut = 'credits';
    show(<Notices model={code} />);
    expect(screen.getByRole('button', { name: 'Check now:saved' })).toBeInTheDocument();
  });

  it('offers no Check now under Stop, on credits that pay, or without a credential to check', () => {
    h.plan = limited('five_hour', 'off');
    show(<Notices model={code} />);
    h.plan = limited('five_hour', 'on');
    h.planOut = 'credits';
    show(<Notices model={code} />);
    h.plan = limited('five_hour', 'off');
    h.meter = { credential: null, checkAfter: null };
    show(<Notices model={code} />);
    expect(screen.getAllByRole('status')).toHaveLength(3);
    expect(screen.queryByRole('button', { name: /Check now/ })).toBeNull();
  });

  it('says how much of a window is used from 80% of it, and opens again once the limit holds', () => {
    h.plan = { ...limited('five_hour', null), five_hour: { used: 0.8, resets_at: '2026-11-06T20:40:00Z' }, limits: [] };
    const { unmount } = show(<Notices model={code} />);
    expect(screen.getByRole('status')).toHaveTextContent("You've used 80% of your Claude plan's 5-hour limit. It resets at 3:40 PM.");
    const near = document.querySelector('[data-key]')?.getAttribute('data-key');
    unmount();

    h.plan = limited('five_hour', 'on');
    show(<Notices model={code} />);
    expect(document.querySelector('[data-key]')?.getAttribute('data-key')).not.toBe(near);
  });

  it('says nothing on another model, below 80%, or for another family', () => {
    h.plan = limited('five_hour', null);
    show(<Notices model={threadModel('model-alpha')} />);
    h.plan = { ...limited('five_hour', null), five_hour: { used: 0.79, resets_at: '2026-11-06T20:40:00Z' }, limits: [] };
    show(<Notices model={code} />);
    h.plan = { ...limited('seven_day_opus', null, 'opus'), five_hour: { used: 0.5, resets_at: '2026-11-06T20:40:00Z' } };
    show(<Notices model={threadModel('claude-sonnet-5-5-code')} />);
    expect(screen.queryByRole('status')).toBeNull();
  });
});
