import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { useLocation } from 'react-router-dom';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { Automation, AutomationExecution, AutomationRun } from '@/types/automation';
import * as api from '../utils/api';
import Automations from '../Automations';

vi.mock('@/hooks/useHomeTimezone', () => ({ useHomeTimezone: () => 'America/New_York' }));
vi.mock('@/hooks/useWorkspaces', () => ({ useWorkspaces: () => ({ data: { workspaces: [] } }) }));

vi.mock('../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../utils/api')>()),
  listAutomations: vi.fn(),
  listExecutions: vi.fn(),
  listRecentRuns: vi.fn(),
}));

afterEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

function execution(id: string, day: number, excerpt: string): AutomationExecution {
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
    delivery_result: null,
    created_at: at,
    excerpt,
  };
}

const NEWEST = execution('exec-new', 25, 'Stocks rose on the jobs report.');
const OLDER = execution('exec-old', 24, 'Stocks fell on rate worries.');

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
  last_run_at: NEWEST.completed_at,
  agent_mode: 'flash',
  instruction: 'Summarize the market.',
  workspace_id: null,
  llm_model: null,
  thread_strategy: 'new',
  conversation_thread_id: null,
  status: 'active',
  max_failures: 3,
  failure_count: 0,
  delivery_config: null,
  created_at: '2026-09-01T00:00:00Z',
  updated_at: '2026-09-01T00:00:00Z',
  last_execution: NEWEST,
};

const asFeed = (e: AutomationExecution): AutomationRun => ({
  ...e,
  automation_name: AUTOMATION.name,
  agent_mode: 'flash',
  trigger_type: 'cron',
  workspace_id: null,
});

function serve() {
  vi.mocked(api.listAutomations).mockResolvedValue({ data: { automations: [AUTOMATION], total: 1, has_more: false } } as never);
  vi.mocked(api.listExecutions).mockResolvedValue({ data: { executions: [NEWEST, OLDER], total: 2, has_more: false } } as never);
  vi.mocked(api.listRecentRuns).mockResolvedValue({ data: { executions: [NEWEST, OLDER].map(asFeed), total: 2, has_more: false } } as never);
}

let search = '';
function Location() {
  search = useLocation().search;
  return null;
}

function renderAt(route: string) {
  serve();
  renderWithProviders(
    <>
      <Automations />
      <Location />
    </>,
    { route },
  );
}

const params = () => Object.fromEntries(new URLSearchParams(search));
const label = (key: string) => i18n.t(key);

describe('a link to one run', () => {
  it('reports that run in its automation', async () => {
    renderAt('/automations?id=auto-1&run=exec-old');

    expect(await screen.findByText(OLDER.excerpt!)).toBeInTheDocument();
    expect(screen.getByText(label('automation.selectedRun'))).toBeInTheDocument();
    expect(screen.queryByText(NEWEST.excerpt!)).not.toBeInTheDocument();
  });

  it('finds the automation of a newest run from the run alone', async () => {
    renderAt('/automations?run=exec-new');

    expect(await screen.findByText(NEWEST.excerpt!)).toBeInTheDocument();
    expect(screen.getByText(label('automation.latestRun'))).toBeInTheDocument();
  });

  it('shows the newest, and says so, for a run past the history', async () => {
    renderAt('/automations?id=auto-1&run=exec-gone');

    expect(await screen.findByText(label('automation.runNotFound'))).toBeInTheDocument();
    expect(screen.getByText(NEWEST.excerpt!)).toBeInTheDocument();
  });

  it('says a run it cannot place is not in the list', async () => {
    renderAt('/automations?run=exec-unknown');

    expect(await screen.findByText(label('automation.runLinkNotFound'))).toBeInTheDocument();
  });

  it('puts a run opened from the history into the link', async () => {
    renderAt('/automations?id=auto-1');
    const table = await screen.findByRole('table');
    // The newest is what the report shows, so it is the current one.
    await within(table).findByRole('button', { current: true });
    const [older] = within(table).getAllByRole('button', { current: false });

    fireEvent.click(older);

    await waitFor(() => expect(params()).toMatchObject({ view: 'manage', id: 'auto-1', run: 'exec-old' }));
    expect(await screen.findByText(OLDER.excerpt!)).toBeInTheDocument();
  });

  it('opens a feed entry on its own run', async () => {
    renderAt('/automations?view=feed');
    const entry = (await screen.findByText(OLDER.excerpt!)).closest('article')!;

    fireEvent.click(within(entry).getByRole('button', { name: AUTOMATION.name }));

    await waitFor(() => expect(params()).toMatchObject({ view: 'manage', id: 'auto-1', run: 'exec-old' }));
    expect(await screen.findByText(label('automation.selectedRun'))).toBeInTheDocument();
  });
});
