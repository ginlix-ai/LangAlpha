/**
 * Check now: held while the last check holds it, by a clock that stops once
 * the hold lapses, one request per credential, every control for that
 * credential in step, and a toast only for a check that failed.
 */
import { useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { PlanCredential } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';

const h = vi.hoisted(() => ({
  toast: vi.fn(),
  check: vi.fn(),
  status: vi.fn(),
  computers: vi.fn(),
}));

vi.mock('@/api/model', () => ({
  checkClaudeCodePlan: h.check, getClaudeCodeStatus: h.status, getClaudeCodeComputers: h.computers, getClaudeCodeSetup: vi.fn(),
}));
vi.mock('@/hooks/useFeatures', () => ({ useFeatures: () => ({ data: {}, isLoading: false }), isFeatureEnabled: () => true }));
vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast: h.toast }) }));
vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'America/New_York', now: Date.parse('2026-11-07T15:00:00Z') }),
}));

import { CheckNowButton } from '../PlanCheck';
import { usePlanCheck, type CheckTarget } from '../usePlanCheck';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '@/hooks/useClaudeCode';

const NOW = '2026-11-07T15:00:00Z';
const SAVED: PlanCredential = { kind: 'saved' };
const COMPUTER: PlanCredential = { kind: 'computer', computerId: 'cmp-1' };

function Control({ credential, name, icon = false, held = null }: { credential: PlanCredential; name: string; icon?: boolean; held?: number | null }) {
  const targets: CheckTarget[] = [{ credential, checkAfter: held }];
  const check = usePlanCheck(targets);
  return <span data-testid={name}><CheckNowButton check={check} icon={icon} /></span>;
}

function renderControls(controls: React.ReactNode) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
  render(<QueryClientProvider client={queryClient}>{controls}</QueryClientProvider>);
  return { invalidate };
}

const failed = (status: number) => Object.assign(new Error(`Request failed with status code ${status}`), { response: { status } });

describe('Check now', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(NOW));
    h.toast.mockReset();
    h.check.mockReset().mockResolvedValue(undefined);
  });
  afterEach(() => vi.useRealTimers());

  it('is held while the last check holds it, and its tooltip says when the next can go', async () => {
    renderControls(<Control credential={SAVED} name="saved" held={Date.parse('2026-11-07T15:04:00Z')} />);
    const button = screen.getByRole('button', { name: 'Check now' });
    expect(button).toHaveAttribute('aria-disabled', 'true');
    fireEvent.focus(button);
    expect(await screen.findByRole('tooltip')).toHaveTextContent('You can check again at 10:04 AM.');
    fireEvent.click(button);
    expect(h.check).not.toHaveBeenCalled();
  });

  it('frees as the hold lapses, and its clock stops turning then', () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
    vi.setSystemTime(new Date(NOW));
    renderControls(<Control credential={SAVED} name="saved" held={Date.parse(NOW) + 2_500} />);
    expect(screen.getByRole('button', { name: 'Check now' })).toHaveAttribute('aria-disabled', 'true');
    expect(vi.getTimerCount()).toBe(1);
    act(() => vi.advanceTimersByTime(3_000));
    expect(screen.getByRole('button', { name: 'Check now' })).not.toHaveAttribute('aria-disabled');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('is ready once the hold has passed, and says what a check costs', async () => {
    renderControls(<Control credential={SAVED} name="saved" held={Date.parse('2026-11-07T14:55:00Z')} />);
    const button = screen.getByRole('button', { name: 'Check now' });
    expect(button).not.toHaveAttribute('aria-disabled');
    fireEvent.focus(button);
    expect(await screen.findByRole('tooltip')).toHaveTextContent('Sends one small request on your Claude plan to read its usage.');
  });

  it.each([[SAVED], [COMPUTER]])('on a click while ready, checks the row\'s credential (%j), then reads the Claude Code status, computers and setup again', async (credential) => {
    const { invalidate } = renderControls(<Control credential={credential} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(3));
    expect(h.check).toHaveBeenCalledOnce();
    expect(h.check).toHaveBeenCalledWith(credential);
    expect(invalidate.mock.calls.map(([filters]) => filters?.queryKey)).toEqual([
      queryKeys.features.claudeCodeStatus(), queryKeys.features.claudeCodeComputers(), queryKeys.features.claudeCodeSetup(),
    ]);
    expect(h.toast).not.toHaveBeenCalled();
  });

  it.each([
    [SAVED, 'a check that answered', null, 300_000],
    [COMPUTER, 'a check that answered', null, 300_000],
    [SAVED, 'a hold taken elsewhere', { status: 429, code: 'check_held', check_in: 120 }, 120_000],
    [COMPUTER, 'a hold taken elsewhere', { status: 429, code: 'check_held', check_in: 120 }, 120_000],
    [SAVED, 'a limit that holds', { status: 409, code: 'limit_held' }, 60_000],
    [COMPUTER, 'a limit that holds', { status: 409, code: 'limit_held' }, 60_000],
    [SAVED, 'a check already running', { status: 409, code: 'check_in_progress' }, 60_000],
    [SAVED, 'a credential it cannot check', { status: 409, code: 'not_checkable' }, 60_000],
  ] as const)('after %j meets %s, stays down while the reads fail, then reads again and comes back', async (credential, _, refusal, holdMs) => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'], shouldAdvanceTime: true });
    vi.setSystemTime(new Date(NOW));
    if (refusal) {
      const { status: code, ...error } = refusal;
      h.check.mockRejectedValue(Object.assign(failed(code), {
        response: { status: code, data: { error: { message: '', retryable: false, ...error } } },
      }));
    }
    h.status.mockResolvedValueOnce({ enabled: true, state: 'signed_in', email: null, expires_at: null, saved: true, check_in: null })
      .mockRejectedValue(new Error('offline'));
    h.computers.mockResolvedValueOnce({
      computers: [{ computer_id: 'cmp-1', name: 'Lab box', kind: 'ssh', state: 'signed_in', source: 'machine', email: null, expires_at: null, check_in: null }],
    }).mockRejectedValue(new Error('offline'));
    // As a surface holds Check now: by the `check_in` its reads carry.
    function Surface() {
      const status = useClaudeCodeStatus();
      const { computers, checkAfterOf } = useClaudeCodeComputers();
      const checkAfter = credential.kind === 'saved' ? status.checkAfter : computers[0] ? checkAfterOf(computers[0]) : null;
      return <CheckNowButton check={usePlanCheck(computers[0] ? [{ credential, checkAfter }] : [])} />;
    }
    renderControls(<Surface />);
    const button = await screen.findByRole('button', { name: 'Check now' });
    await waitFor(() => expect(button).not.toHaveAttribute('aria-disabled'));
    fireEvent.click(button);
    await waitFor(() => expect([h.status, h.computers].map((read) => read.mock.calls.length >= 2)).toEqual([true, true]));
    await waitFor(() => expect(button).toHaveAttribute('aria-disabled', 'true'));
    // Past the computers' one retry, so the reads counted are the hold's own.
    await act(() => vi.advanceTimersByTimeAsync(5_000));
    const reads = [h.status.mock.calls.length, h.computers.mock.calls.length];
    await act(() => vi.advanceTimersByTimeAsync(holdMs - 12_000));
    expect(button).toHaveAttribute('aria-disabled', 'true');
    expect([h.status.mock.calls.length, h.computers.mock.calls.length]).toEqual(reads);
    await act(() => vi.advanceTimersByTimeAsync(10_000));
    await waitFor(() => expect(h.status.mock.calls.length).toBeGreaterThan(reads[0]));
    expect(h.computers.mock.calls.length).toBeGreaterThan(reads[1]);
    await waitFor(() => expect(button).not.toHaveAttribute('aria-disabled'));
    expect(h.toast).not.toHaveBeenCalled();
  });

  it('reads Checking… on every control for the credential while one check runs, and nowhere else', async () => {
    let finish: () => void = () => {};
    h.check.mockReturnValue(new Promise<void>((resolve) => { finish = resolve; }));
    renderControls(
      <>
        <Control credential={SAVED} name="header" />
        <Control credential={SAVED} name="picker" icon />
        <Control credential={COMPUTER} name="other" />
      </>,
    );
    fireEvent.click(screen.getAllByRole('button', { name: 'Check now' })[0]);
    await waitFor(() => expect(screen.getByTestId('header')).toHaveTextContent('Checking…'));
    expect(screen.getByTestId('picker').querySelector('button')).toHaveAttribute('aria-label', 'Checking…');
    expect(screen.getByTestId('picker').querySelector('button')).toHaveAttribute('aria-disabled', 'true');
    expect(screen.getByTestId('other')).toHaveTextContent('Check now');
    await act(async () => finish());
    await waitFor(() => expect(screen.getByTestId('header')).toHaveTextContent('Check now'));
    expect(screen.getByTestId('picker').querySelector('button')).toHaveAttribute('aria-label', 'Check now');
  });

  it('reads Checking… on a control that comes to the credential while its check runs', async () => {
    let finish: () => void = () => {};
    h.check.mockReturnValue(new Promise<void>((resolve) => { finish = resolve; }));
    function Moving() {
      const [credential, setCredential] = useState<PlanCredential>(COMPUTER);
      return (
        <>
          <button type="button" onClick={() => setCredential(SAVED)}>move</button>
          <Control credential={credential} name="menu" />
        </>
      );
    }
    renderControls(<><Control credential={SAVED} name="header" /><Moving /></>);
    fireEvent.click(screen.getByTestId('header').querySelector('button')!);
    await waitFor(() => expect(screen.getByTestId('header')).toHaveTextContent('Checking…'));
    expect(screen.getByTestId('menu')).toHaveTextContent('Check now');
    fireEvent.click(screen.getByRole('button', { name: 'move' }));
    expect(screen.getByTestId('menu')).toHaveTextContent('Checking…');
    await act(async () => finish());
  });

  it('does not start a second check on a credential already being checked', async () => {
    let finish: () => void = () => {};
    h.check.mockReturnValue(new Promise<void>((resolve) => { finish = resolve; }));
    renderControls(
      <>
        <Control credential={SAVED} name="header" />
        <Control credential={SAVED} name="picker" icon />
      </>,
    );
    fireEvent.click(screen.getAllByRole('button', { name: 'Check now' })[0]);
    await waitFor(() => expect(screen.getByTestId('header')).toHaveTextContent('Checking…'));
    screen.getAllByRole('button', { name: 'Checking…' }).forEach((button) => fireEvent.click(button));
    expect(h.check).toHaveBeenCalledOnce();
    await act(async () => finish());
    await waitFor(() => expect(screen.getByTestId('header')).toHaveTextContent('Check now'));
  });

  it.each([[503], [500], [401]])('says Claude could not be reached after a %i', async (status) => {
    h.check.mockRejectedValue(Object.assign(failed(status), {
      response: { status, data: { error: { code: 'check_unavailable', message: '', retryable: true } } },
    }));
    renderControls(<Control credential={SAVED} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(h.toast).toHaveBeenCalledOnce());
    expect(h.toast).toHaveBeenCalledWith({ variant: 'destructive', description: "Couldn't reach Claude to check usage. Try again later." });
  });

  it('says the usage could not be updated where Claude answered but the plan it read was not kept', async () => {
    h.check.mockRejectedValue(Object.assign(failed(503), {
      response: { status: 503, data: { error: { code: 'plan_unsaved', message: '', retryable: true } } },
    }));
    renderControls(<Control credential={SAVED} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(h.toast).toHaveBeenCalledOnce());
    expect(h.toast).toHaveBeenCalledWith({ variant: 'destructive', description: "Claude answered, but the usage couldn't be updated. Try again later." });
  });

  it.each([
    [400, 'policy_blocked', "Your organization's Claude Code policy is installed on this server."],
    [503, 'redis_unavailable', 'Settings are unavailable right now. Try again shortly.'],
    [404, 'unavailable', 'Claude Code is off for you.'],
  ])('says the server\'s own sentence for a %i %s the check never ran past', async (status, code, message) => {
    h.check.mockRejectedValue(Object.assign(failed(status), { response: { status, data: { error: { code, message, retryable: false } } } }));
    renderControls(<Control credential={SAVED} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(h.toast).toHaveBeenCalledOnce());
    expect(h.toast).toHaveBeenCalledWith({ variant: 'destructive', description: message });
  });

  it('says the computer could not be reached where the server says the computer was the one out of reach', async () => {
    h.check.mockRejectedValue(Object.assign(failed(503), {
      response: { status: 503, data: { error: { code: 'computer_unreachable', message: '', retryable: true } } },
    }));
    renderControls(<Control credential={COMPUTER} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(h.toast).toHaveBeenCalledOnce());
    expect(h.toast).toHaveBeenCalledWith({ variant: 'destructive', description: "Couldn't reach this computer to check usage. Try again later." });
  });

  it('says Claude could not be reached after a failure that has no status', async () => {
    h.check.mockRejectedValue(new Error('Network Error'));
    renderControls(<Control credential={SAVED} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(h.toast).toHaveBeenCalledOnce());
  });

  it.each([[409], [429]])('says nothing after a %i, since the read after it shows where the check stands', async (status) => {
    h.check.mockRejectedValue(failed(status));
    const { invalidate } = renderControls(<Control credential={SAVED} name="row" />);
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }));
    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Check now' })).toBeInTheDocument());
    expect(h.toast).not.toHaveBeenCalled();
  });
});
