import { afterEach, describe, expect, it, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { Automation, AutomationExecution, AutomationRun, DeliveryAttempt, DeliveryOptions } from '@/types/automation';
import * as api from '../utils/api';
import Automations from '../Automations';

vi.mock('@/hooks/useHomeTimezone', () => ({ useHomeTimezone: () => 'America/New_York' }));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws-1', name: 'Research' }] } }),
}));

vi.mock('../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../utils/api')>()),
  listAutomations: vi.fn(),
  listExecutions: vi.fn(),
  listRecentRuns: vi.fn(),
  getDeliveryOptions: vi.fn(),
}));

afterEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

const label = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts);

const OPTIONS: DeliveryOptions = {
  enabled: true,
  apps: {
    slack: {
      chats: [{ address: 'slack:T1/C1', name: '#demo', kind: 'channel' }],
      default: { address: 'slack:T1/C1', name: '#demo', via: 'workspace' },
      error: null,
    },
    discord: {
      chats: [{ address: 'discord:@me', name: 'Your Discord DM', kind: 'dm' }],
      default: { address: 'discord:@me', name: 'Your Discord DM', via: 'dm' },
      error: null,
    },
  },
};

function execution(id: string, day: number, delivery: DeliveryAttempt[] | null): AutomationExecution {
  const at = `2026-09-${day}T13:00:00Z`;
  return {
    automation_execution_id: id,
    automation_id: 'auto-1',
    status: 'completed',
    conversation_thread_id: null,
    scheduled_at: at,
    started_at: at,
    completed_at: `2026-09-${day}T13:02:00Z`,
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: delivery,
    created_at: at,
    excerpt: `Report ${id}`,
    dismissed_at: null,
  };
}

const SENT: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo', via: 'agent' };
const FALLBACK: DeliveryAttempt = { method: 'discord:@me', success: true, address: 'discord:@me', name: 'Your Discord DM', via: 'fallback' };
const NOTICE: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo', via: 'notice' };
const FAILED: DeliveryAttempt = {
  method: 'slack:T1/C9',
  success: false,
  address: 'slack:T1/C9',
  name: '#ops',
  via: null,
  error: 'The bot was removed from the channel.',
};
// A run from before deliveries named where they landed.
const LEGACY: DeliveryAttempt = { method: 'slack', success: true };

const RUNS = [
  execution('run-agent', 25, [SENT, FALLBACK]),
  execution('run-notice', 24, [NOTICE]),
  execution('run-failed', 23, [FAILED]),
  execution('run-legacy', 22, [LEGACY]),
];

const AUTOMATION: Automation = {
  automation_id: 'auto-1',
  user_id: 'user-1',
  name: 'Morning briefing',
  description: null,
  trigger_type: 'cron',
  cron_expression: '0 9 * * 1-5',
  timezone: 'America/New_York',
  trigger_config: null,
  next_run_at: '2026-09-26T13:00:00Z',
  last_run_at: RUNS[0].completed_at,
  agent_mode: 'ptc',
  instruction: 'Summarize the market.',
  workspace_id: 'ws-1',
  llm_model: null,
  thread_strategy: 'new',
  conversation_thread_id: null,
  status: 'active',
  max_failures: 3,
  failure_count: 0,
  delivery_config: { methods: ['slack', 'discord:@me', 'slack:T1/C9'] },
  created_at: '2026-09-01T00:00:00Z',
  updated_at: '2026-09-01T00:00:00Z',
  last_execution: RUNS[0],
};

const asFeed = (e: AutomationExecution): AutomationRun => ({
  ...e,
  automation_name: AUTOMATION.name,
  agent_mode: 'ptc',
  trigger_type: 'cron',
  workspace_id: 'ws-1',
});

function renderAt(route: string) {
  vi.mocked(api.listAutomations).mockResolvedValue({ data: { automations: [AUTOMATION], total: 1 } } as never);
  vi.mocked(api.listExecutions).mockResolvedValue({ data: { executions: RUNS, has_more: false } } as never);
  vi.mocked(api.listRecentRuns).mockResolvedValue({ data: { executions: RUNS.map(asFeed), has_more: false } } as never);
  vi.mocked(api.getDeliveryOptions).mockResolvedValue({ data: OPTIONS } as never);
  renderWithProviders(<Automations />, { route });
}

async function inspector() {
  return (await screen.findByRole('heading', { level: 2, name: AUTOMATION.name })).closest('article')!;
}

/** The history row whose run started on the given day, and the error row under it. */
async function historyRow(day: string) {
  const table = await within(await inspector()).findByRole('table');
  const row = within(table).getByRole('button', { name: new RegExp(day) }).closest('tr')!;
  return { row, next: row.nextElementSibling as HTMLElement | null };
}

describe('delivery names in the inspector', () => {
  it('names each entry: an app by where it lands, a chat by its name, else as a run named it', async () => {
    renderAt('/automations?id=auto-1');
    const pane = await inspector();

    const term = await within(pane).findByText(label('automation.delivery'), { selector: 'dt' });
    // #ops is no longer listed; the failed run named it.
    expect(await within(pane).findByText('Slack (#demo), Your Discord DM, #ops')).toBeInTheDocument();
    expect(term.nextElementSibling).toHaveTextContent('Slack (#demo), Your Discord DM, #ops');
    expect(api.getDeliveryOptions).toHaveBeenCalledWith('ws-1');
  });
});

describe('where each run’s delivery landed', () => {
  it('says the agent sent it, and where a final answer was posted for it', async () => {
    renderAt('/automations?id=auto-1');
    const { row } = await historyRow('25');

    expect(within(row).getByText(label('automation.deliveredVia', { method: '#demo' }))).toBeInTheDocument();
    expect(within(row).getByText(label('automation.deliveredFallbackVia', { method: 'Your Discord DM' }))).toBeInTheDocument();
  });

  it('says a notice went', async () => {
    renderAt('/automations?id=auto-1');
    const { row } = await historyRow('24');

    expect(within(row).getByText(label('automation.deliveredNoticeVia', { method: '#demo' }))).toBeInTheDocument();
  });

  it('says a delivery failed, and why, under the run', async () => {
    renderAt('/automations?id=auto-1');
    const { row, next } = await historyRow('23');

    expect(within(row).getByText(label('automation.deliveryFailedVia', { method: '#ops' }))).toBeInTheDocument();
    expect(within(row).getByLabelText(label('automation.runFailed'))).toBeInTheDocument();
    expect(row).toHaveClass('has-error');
    expect(next).toHaveTextContent('#ops: The bot was removed from the channel.');
  });

  it('names only the app for a run from before', async () => {
    renderAt('/automations?id=auto-1');
    const { row, next } = await historyRow('22');

    const cell = row.querySelector('.automation-history-delivery')!;
    expect(cell.textContent).toBe('Slack');
    expect(next?.classList.contains('automation-history-error') ?? false).toBe(false);
  });
});

describe('delivery in the feed', () => {
  it('reads each delivery by the name the run recorded and how it went', async () => {
    renderAt('/automations?view=feed');

    const agent = (await screen.findByText('Report run-agent')).closest('article')!;
    expect(agent).toHaveTextContent(
      `${label('automation.deliveredVia', { method: '#demo' })} · ${label('automation.deliveredFallbackVia', { method: 'Your Discord DM' })}`,
    );
    const notice = screen.getByText('Report run-notice').closest('article')!;
    expect(notice).toHaveTextContent(label('automation.deliveredNoticeVia', { method: '#demo' }));
    const failed = screen.getByText('Report run-failed').closest('article')!;
    expect(failed).toHaveTextContent(label('automation.deliveryFailedVia', { method: '#ops' }));
    const legacy = screen.getByText('Report run-legacy').closest('article')!;
    expect(legacy).toHaveTextContent(label('automation.deliveredVia', { method: 'Slack' }));
    // The feed reads names from the runs alone, never the apps' chats.
    expect(api.getDeliveryOptions).not.toHaveBeenCalled();
  });
});
