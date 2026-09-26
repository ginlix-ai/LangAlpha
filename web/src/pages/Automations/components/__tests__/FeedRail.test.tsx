import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import i18n from '@/i18n';
import { relativeTime } from '@/lib/format';
import { renderWithProviders } from '@/test/utils';
import type { Automation, AutomationStatus, DisableReason, FailureReason } from '@/types/automation';
import * as api from '../../utils/api';
import { FeedRail } from '../FeedRail';

// With nothing scheduled the rail offers starters, whose cadence reads the
// user's home zone from a query this test does not answer.
vi.mock('@/hooks/useHomeTimezone', () => ({ useHomeTimezone: () => 'America/New_York' }));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  pauseAutomation: vi.fn(() => Promise.resolve({})),
  resumeAutomation: vi.fn(() => Promise.resolve({})),
  triggerAutomation: vi.fn(() => Promise.resolve({})),
}));

afterEach(() => vi.clearAllMocks());

const COMPLETED_AT = '2026-09-25T13:02:00Z';

function failing({
  status = 'active',
  disable,
  reason = null,
  message = null,
  failures = 1,
}: {
  status?: AutomationStatus;
  disable?: DisableReason;
  reason?: FailureReason | null;
  message?: string | null;
  failures?: number;
}): Automation {
  return {
    automation_id: 'auto-1',
    user_id: 'user-1',
    name: 'Morning briefing',
    description: null,
    trigger_type: 'cron',
    cron_expression: '0 9 * * 1-5',
    timezone: 'America/New_York',
    trigger_config: null,
    next_run_at: null,
    last_run_at: null,
    agent_mode: 'flash',
    instruction: 'Summarize the market.',
    workspace_id: null,
    llm_model: null,
    thread_strategy: 'new',
    conversation_thread_id: null,
    status,
    max_failures: 3,
    failure_count: failures,
    delivery_config: null,
    disable_reason: disable,
    created_at: '2026-09-01T00:00:00Z',
    updated_at: '2026-09-01T00:00:00Z',
    last_execution: {
      automation_execution_id: 'exec-1',
      automation_id: 'auto-1',
      status: 'failed',
      conversation_thread_id: 'thread-1',
      scheduled_at: '2026-09-25T13:00:00Z',
      started_at: '2026-09-25T13:00:01Z',
      completed_at: COMPLETED_AT,
      error_message: message,
      skip_reason: null,
      failure_reason: reason,
      delivery_result: null,
      created_at: '2026-09-25T13:00:00Z',
      excerpt: null,
    },
  };
}

function renderRow(a: Automation) {
  renderWithProviders(
    <FeedRail automations={[a]} readings={new Map()} onOpenAutomation={vi.fn()} onManage={vi.fn()} onNew={vi.fn()} />,
  );
  const row = screen.getByRole('button', { name: a.name }).closest('.automations-rail-row') as HTMLElement;
  const buttons = within(row)
    .getAllByRole('button')
    .map((b) => b.textContent)
    .filter((text) => text !== a.name);
  const links = within(row).queryAllByRole('link');
  return { row, buttons, links };
}

const label = (key: string, options?: Record<string, unknown>) => i18n.t(key, options);

describe('FeedRail attention row', () => {
  it('quotes a usage limit and offers another try, a pause, and the plan', async () => {
    const message = 'You have used this month’s credits. They reset on Oct 1.';
    const { row, buttons, links } = renderRow(failing({ reason: 'usage_limit', message }));

    expect(within(row).getByText(message)).toBeInTheDocument();
    expect(buttons).toEqual([label('common.retry'), label('automation.pause'), label('automation.openThread')]);
    expect(links.map((l) => l.textContent)).toEqual([label('chat.errorLinkManagePlan'), label('chat.errorLinkViewUsage')]);
    for (const link of links) expect(link).toHaveAttribute('target', '_blank');

    fireEvent.click(within(row).getByRole('button', { name: label('automation.pause') }));
    await waitFor(() => expect(api.pauseAutomation).toHaveBeenCalledWith('auto-1'));
    // A write in flight holds every button, so the next click waits for it.
    await waitFor(() => expect(within(row).getByRole('button', { name: label('common.retry') })).toBeEnabled());
    fireEvent.click(within(row).getByRole('button', { name: label('common.retry') }));
    await waitFor(() => expect(api.triggerAutomation).toHaveBeenCalledWith('auto-1'));
  });

  it('names the limit when the service left no message', () => {
    const { row, buttons, links } = renderRow(failing({ reason: 'usage_limit' }));

    expect(within(row).getByText(label('automation.stateUsageLimit'))).toBeInTheDocument();
    expect(buttons).toEqual([label('common.retry'), label('automation.pause'), label('automation.openThread')]);
    expect(links).toHaveLength(2);
  });

  it('says the provider rejected the key and sends the reader to their models', async () => {
    const { row, buttons, links } = renderRow(
      failing({ status: 'disabled', disable: 'provider_auth', reason: 'provider_auth', message: '401 Unauthorized' }),
    );

    expect(within(row).getByText(label('automation.keyRejectedReason'))).toBeInTheDocument();
    expect(buttons).toEqual([label('automation.resume'), label('automation.openThread')]);
    expect(links.map((l) => [l.textContent, l.getAttribute('href')])).toEqual([
      [label('chat.modelSelector.manageModels'), '/settings?tab=model'],
    ]);

    fireEvent.click(within(row).getByRole('button', { name: label('automation.resume') }));
    await waitFor(() => expect(api.resumeAutomation).toHaveBeenCalledWith('auto-1'));
  });

  it('counts the failures that switched it off', () => {
    const { row, buttons, links } = renderRow(failing({ status: 'disabled', disable: 'max_failures', failures: 3 }));

    expect(within(row).getByText(label('automation.disabledAfter', { count: 3 }))).toBeInTheDocument();
    expect(buttons).toEqual([label('automation.resume'), label('automation.openThread')]);
    expect(links).toHaveLength(0);
  });

  it('leaves the thread out when the run never got one', () => {
    const a = failing({ reason: 'usage_limit', message: 'Out of credits.' });
    const { buttons } = renderRow({ ...a, last_execution: { ...a.last_execution!, conversation_thread_id: null } });

    expect(buttons).toEqual([label('common.retry'), label('automation.pause')]);
  });

  it('offers another try or a pause for any other failure', () => {
    const { row, buttons, links } = renderRow(failing({ message: 'Tool call failed' }));

    expect(
      within(row).getByText(label('automation.lastRunFailedAgo', { when: relativeTime(COMPLETED_AT) })),
    ).toBeInTheDocument();
    expect(buttons).toEqual([label('common.retry'), label('automation.pause'), label('automation.openThread')]);
    expect(links).toHaveLength(0);
  });
});
