/**
 * The ring and its context panel. The panel counts the parts only while it is
 * open: nothing before the first open, one read on it, none on a reopen
 * within the stale time, none without a thread. What it shows: the breakdown
 * once counted, placeholders while counting, a line when the computer is
 * stopped and the window alone when the parts cannot be had; the model's
 * window only when it declares one, and a cache hit rate only when the
 * counts behind it are known. Before the count lands it holds its slot as an
 * inert track, which turns into the trigger in place.
 */
import { useState } from 'react';
import { beforeEach, describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, cleanup, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import type { TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
import type { DueSummary } from '@/pages/ChatAgent/hooks/useNextSummaryHint';
import type { ThreadContextResponse } from '@/pages/ChatAgent/utils/api/threads';

vi.mock('@/pages/ChatAgent/utils/api', () => ({ fetchThreadContext: vi.fn() }));

import { fetchThreadContext } from '@/pages/ChatAgent/utils/api';
import { TokenUsageRing } from '../token-usage-ring';

const mockFetch = fetchThreadContext as Mock;

const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

const USAGE: TokenUsage = {
  totalInput: 10_000, totalOutput: 700, totalCached: 6000, fromServer: true,
  lastInput: 140_000, lastCached: 137_200,
  total: 144_400, threshold: 500_000, contextLimit: 1_000_000,
};

const READY: ThreadContextResponse = { thread_id: THREAD, parts: { system: 13_100, tools: 16_100 } };

/** Retries off as in every test, but the app's cache lifetime, so a reopen
 *  finds the count the first open read. */
const client = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });

const trigger = () => screen.getByRole('button', { name: 'Context window, 29% used' });

async function open(usage: Partial<TokenUsage> = {}, threadId: string | null = THREAD) {
  renderWithProviders(<TokenUsageRing tokenUsage={{ ...USAGE, ...usage }} threadId={threadId} />, { queryClient: client() });
  await userEvent.click(trigger());
  return screen.getByRole('dialog', { name: 'Context' });
}

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(READY);
});

describe('TokenUsageRing', () => {
  it('holds its slot as an inert track until the count lands, then becomes the trigger in place', async () => {
    const { container, rerender } = renderWithProviders(
      <TokenUsageRing tokenUsage={null} threadId={THREAD} />, { queryClient: client() },
    );
    const slot = container.querySelector('button')!;
    expect(slot.disabled).toBe(true);
    expect(screen.queryByRole('button')).toBeNull();
    await userEvent.click(slot);
    expect(screen.queryByRole('dialog')).toBeNull();

    rerender(<TokenUsageRing tokenUsage={USAGE} threadId={THREAD} />);
    expect(trigger()).toBe(slot);
    expect(slot.disabled).toBe(false);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('reads nothing until it opens, then once', async () => {
    renderWithProviders(<TokenUsageRing tokenUsage={USAGE} threadId={THREAD} />, { queryClient: client() });
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();

    await userEvent.click(trigger());
    await waitFor(() => expect(screen.getByText('System prompt')).toBeTruthy());
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(THREAD);
  });

  it('reopening within the stale time shows the count it had without reading again', async () => {
    await open();
    await waitFor(() => expect(screen.getByText('System prompt')).toBeTruthy());
    await userEvent.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await userEvent.click(trigger());
    expect(screen.getByText('System prompt')).toBeTruthy();
    await act(async () => {});
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('reads nothing for a chat with no thread yet, and is the window alone', async () => {
    for (const threadId of [null, '__default__']) {
      await open({}, threadId);
      await act(async () => {});
      expect(screen.queryByRole('list', { hidden: true })).toBeNull();
      expect(screen.getByText('Compact at')).toBeTruthy();
      cleanup();
    }
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('lists the counted parts with the conversation as the rest of the window', async () => {
    const panel = await open();
    await waitFor(() => expect(screen.getByText('System prompt')).toBeTruthy());
    expect(panel.textContent).toContain('144.4K / 500K used (29%)');
    expect(screen.getByText('Tools')).toBeTruthy();
    expect(screen.queryByText('MCP servers')).toBeNull();
    const conversation = screen.getByText('Conversation').closest('li')!;
    expect(within(conversation).getByText('115.2K')).toBeTruthy();
    expect(screen.queryByRole('list', { name: 'Counting context' })).toBeNull();
  });

  it('holds seven placeholder rows while counting, with the bar in one fill', async () => {
    mockFetch.mockReturnValue(new Promise(() => {}));
    await open();
    const placeholder = screen.getByRole('list', { name: 'Counting context', hidden: true });
    expect(placeholder.getAttribute('aria-busy')).toBe('true');
    expect(placeholder.querySelectorAll('li')).toHaveLength(7);
    expect(screen.queryByText('System prompt')).toBeNull();
    expect(screen.getByText('Compact at')).toBeTruthy();
  });

  it('says the breakdown waits for a stopped computer', async () => {
    mockFetch.mockResolvedValue({ thread_id: THREAD, parts: null, reason: 'computer_stopped' });
    await open();
    await waitFor(() => expect(screen.getByText('Breakdown is available while the computer is running.')).toBeTruthy());
    expect(screen.queryByText('System prompt')).toBeNull();

    // Started since: the next open asks again rather than keep the answer.
    mockFetch.mockResolvedValue(READY);
    await userEvent.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await userEvent.click(trigger());
    await waitFor(() => expect(screen.getByText('System prompt')).toBeTruthy());
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('is the window alone for another reason, or a failed read', async () => {
    const answers: (() => Promise<unknown>)[] = [
      () => Promise.resolve({ thread_id: THREAD, parts: null, reason: 'refused' }),
      () => Promise.reject(new Error('Network Error')),
    ];
    for (const answer of answers) {
      mockFetch.mockImplementation(answer);
      const panel = await open();
      // A dropped read is tried once more before it counts as failed.
      await waitFor(() => expect(screen.queryByRole('list', { name: 'Counting context', hidden: true })).toBeNull(), { timeout: 3000 });
      expect(screen.queryByRole('list', { hidden: true })).toBeNull();
      expect(panel.textContent).not.toContain('computer is running');
      expect(screen.getByText('Compact at')).toBeTruthy();
      expect(screen.getByText('This thread')).toBeTruthy();
      cleanup();
    }
  });

  it('draws no window without a limit', async () => {
    const panel = await open({ contextLimit: null });
    expect(panel.textContent).not.toContain('/ 1M');
  });

  it('shows the window when the model declares one, and the hit rates', async () => {
    await open();
    expect(screen.getByText('/ 1M', { exact: false })).toBeTruthy();
    expect(screen.getByText('60%')).toBeTruthy();
    expect(screen.getByText('last call 98%')).toBeTruthy();
  });

  it('hides a hit rate it cannot know rather than showing 0%', async () => {
    await open({ totalCached: null, lastCached: null });
    expect(screen.queryByText('Cache hit rate')).toBeNull();
    expect(screen.queryByText('0%')).toBeNull();
  });
});

/**
 * A summary the next send runs first is said on the ring: a mark on it, its
 * name, and the panel's first line. The summary is the plan, so the override
 * is a quiet action, and the cost shows only once the user has taken it. At
 * the threshold the summary has to run, and so does a rebuild, so there is
 * nothing to take. With no window counted yet the ring still shows, since it
 * is where the summary is said.
 */
describe('TokenUsageRing with a summary due', () => {
  const hint = (reason: DueSummary['reason'], skipped = false, toggleSkip = vi.fn()): DueSummary => ({
    reason, skipped, keepFullContext: skipped, toggleSkip,
  });
  const LINE = 'Earlier messages will be summarized before your next one.';

  async function openDue(summary: DueSummary, usage: TokenUsage | null = USAGE) {
    renderWithProviders(<TokenUsageRing tokenUsage={usage} threadId={THREAD} summary={summary} />, { queryClient: client() });
    const ring = screen.getByRole('button', { name: /Earlier messages will be summarized/ });
    expect(ring.querySelector('[data-summary-due]')).toBeTruthy();
    await userEvent.click(ring);
    return screen.getByRole('dialog', { name: 'Context' });
  }

  it('marks nothing while no summary is due', () => {
    renderWithProviders(<TokenUsageRing tokenUsage={USAGE} threadId={THREAD} summary={null} />, { queryClient: client() });
    expect(trigger().querySelector('[data-summary-due]')).toBeNull();
  });

  it('states the cold summary as the plan, with Skip beside it', async () => {
    const toggleSkip = vi.fn();
    const panel = await openDue(hint('cold', false, toggleSkip));
    expect(panel.textContent).toContain(LINE);
    expect(panel.textContent).not.toContain('costs more');
    await userEvent.click(within(panel).getByRole('button', { name: 'Skip' }));
    expect(toggleSkip).toHaveBeenCalledOnce();
  });

  it('after Skip, drops the mark and the due label, and still opens to name the cost and offer Undo', async () => {
    const toggleSkip = vi.fn();
    renderWithProviders(<TokenUsageRing tokenUsage={USAGE} threadId={THREAD} summary={hint('cold', true, toggleSkip)} />, { queryClient: client() });
    const ring = trigger();
    expect(ring.querySelector('[data-summary-due]')).toBeNull();
    await userEvent.click(ring);
    const panel = screen.getByRole('dialog', { name: 'Context' });
    expect(panel.textContent).toContain('Your next message will include the whole conversation, which costs more.');
    await userEvent.click(within(panel).getByRole('button', { name: 'Undo' }));
    expect(toggleSkip).toHaveBeenCalledOnce();
  });

  it('keeps focus on the action from Skip to Undo and back', async () => {
    function Stateful() {
      const [skipped, setSkipped] = useState(false);
      return <TokenUsageRing tokenUsage={USAGE} threadId={THREAD} summary={hint('cold', skipped, vi.fn(() => setSkipped((v) => !v)))} />;
    }
    renderWithProviders(<Stateful />, { queryClient: client() });
    await userEvent.click(screen.getByRole('button', { name: /^Context window/ }));
    const panel = screen.getByRole('dialog', { name: 'Context' });
    within(panel).getByRole('button', { name: 'Skip' }).focus();
    await userEvent.keyboard('{Enter}');
    expect(document.activeElement).toBe(within(panel).getByRole('button', { name: 'Undo' }));
    await userEvent.keyboard('{Enter}');
    expect(document.activeElement).toBe(within(panel).getByRole('button', { name: 'Skip' }));
  });

  it('after Skip with no window counted, stays a trigger so the skip can be undone', async () => {
    const { container } = renderWithProviders(
      <TokenUsageRing tokenUsage={null} threadId={THREAD} summary={hint('cold', true)} />, { queryClient: client() },
    );
    const ring = screen.getByRole('button', { name: 'Context' });
    expect(ring).toBeEnabled();
    expect(container.querySelector('[data-summary-due]')).toBeNull();
    await userEvent.click(ring);
    const panel = screen.getByRole('dialog', { name: 'Context' });
    expect(within(panel).getByRole('button', { name: 'Undo' })).toBeTruthy();
  });

  it.each(['threshold', 'rebuild'] as const)('for %s says so with nothing to skip', async (reason) => {
    const panel = await openDue(hint(reason));
    expect(panel.textContent).toContain(LINE);
    expect(within(panel).queryByRole('button', { name: /Skip|Undo/ })).toBeNull();
  });

  it('shows with no window counted, and the panel is the summary alone', async () => {
    const panel = await openDue(hint('rebuild'), null);
    expect(panel.textContent).toContain(LINE);
    expect(screen.queryByText('Compact at')).toBeNull();
    await act(async () => {});
    expect(mockFetch).not.toHaveBeenCalled();
  });
});
