import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { toast } from '@/components/ui/use-toast';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { Automation, AutomationPayload, AutomationUpdatePayload } from '@/types/automation';
import * as api from '../utils/api';
import Automations from '../Automations';

vi.mock('@/hooks/useHomeTimezone', () => ({ useHomeTimezone: () => 'America/New_York' }));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws-1', name: 'Research' }] } }),
}));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));
// Home's request, which every reader of Home's query shares.
const home = vi.hoisted(() => ({ getHomeWorkspace: vi.fn() }));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: home.getHomeWorkspace,
}));
const HOME_ROW = { workspace_id: 'home-1', is_home: true };

/** Holds Home's request until the returned function answers it. */
function holdHome(): () => Promise<void> {
  let answer!: () => void;
  home.getHomeWorkspace.mockReturnValue(new Promise((resolve) => (answer = () => resolve(HOME_ROW))));
  return () => act(async () => answer());
}

vi.mock('../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../utils/api')>()),
  listAutomations: vi.fn(),
  listExecutions: vi.fn(),
  listRecentRuns: vi.fn(),
  getDeliveryOptions: vi.fn(),
  createAutomation: vi.fn(),
  updateAutomation: vi.fn(),
}));

beforeEach(() => {
  home.getHomeWorkspace.mockResolvedValue(HOME_ROW);
});

afterEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

const label = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts);

/** A row that runs in the Chief of Staff, which names Home's id. */
const IN_HOME: Automation = {
  automation_id: 'auto-1',
  user_id: 'user-1',
  name: 'Morning briefing',
  description: null,
  trigger_type: 'cron',
  cron_expression: '0 9 * * 1-5',
  timezone: 'America/New_York',
  trigger_config: null,
  next_run_at: '2026-09-26T13:00:00Z',
  last_run_at: null,
  instruction: 'Summarize the market.',
  workspace_id: 'home-1',
  llm_model: null,
  thread_strategy: 'new',
  conversation_thread_id: null,
  status: 'active',
  max_failures: 3,
  failure_count: 0,
  delivery_config: null,
  created_at: '2026-09-01T00:00:00Z',
  updated_at: '2026-09-01T00:00:00Z',
  last_execution: null,
};

function renderAt(route: string, automations: Automation[]) {
  vi.mocked(api.listAutomations).mockResolvedValue({ data: { automations, total: automations.length } } as never);
  vi.mocked(api.listExecutions).mockResolvedValue({ data: { executions: [], has_more: false } } as never);
  vi.mocked(api.listRecentRuns).mockResolvedValue({ data: { executions: [], has_more: false } } as never);
  vi.mocked(api.getDeliveryOptions).mockResolvedValue({ data: { enabled: false, apps: {}, workspace_id: null } } as never);
  vi.mocked(api.createAutomation).mockResolvedValue({ data: { ...IN_HOME, automation_id: 'auto-new' } } as never);
  vi.mocked(api.updateAutomation).mockResolvedValue({ data: IN_HOME } as never);
  renderWithProviders(<Automations />, { route });
}

/** Starts a new automation from the zero state's starting points and names it. */
async function startFrom(templateKey: string) {
  renderAt('/automations', []);
  fireEvent.click(await screen.findByText(label(templateKey)));
  fireEvent.change(await screen.findByLabelText(label('common.name')), { target: { value: 'Brief' } });
  fireEvent.change(screen.getByLabelText(label('automation.instruction')), { target: { value: 'Summarize.' } });
}

const created = () => vi.mocked(api.createAutomation).mock.calls[0][0] as AutomationPayload;

describe('where an automation runs', () => {
  it("opens a row naming Home's id as the Chief of Staff and saves that id, with no agent mode", async () => {
    renderAt('/automations?id=auto-1', [IN_HOME]);
    const pane = (await screen.findByRole('heading', { level: 2, name: IN_HOME.name })).closest('article')!;
    expect(within(pane).getByText(new RegExp(label('agents.chiefOfStaff')))).toBeInTheDocument();

    fireEvent.click(within(pane).getByRole('button', { name: label('automation.edit') }));
    expect(await screen.findByText(new RegExp(`^${label('agents.chiefOfStaff')} · `))).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: label('automation.saveChanges') }));

    await waitFor(() => expect(api.updateAutomation).toHaveBeenCalled());
    const [id, payload] = vi.mocked(api.updateAutomation).mock.calls[0] as [string, AutomationUpdatePayload];
    expect(id).toBe('auto-1');
    expect(payload.workspace_id).toBe('home-1');
    expect(payload).not.toHaveProperty('agent_mode');
  });

  it("creates a blank automation in the Chief of Staff with Home's id", async () => {
    await startFrom('automation.blankAutomation');
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(api.createAutomation).toHaveBeenCalled());
    expect(created().workspace_id).toBe('home-1');
    expect(created()).not.toHaveProperty('agent_mode');
  });

  it('asks a workspace template for its workspace, then sends that id', async () => {
    await startFrom('automation.templateWeeklyReview');
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    // Refused before the round trip, with the place unfolded to pick.
    const trigger = await screen.findByRole('button', { name: new RegExp(label('automation.selectWorkspace').replace('...', '')) });
    expect(api.createAutomation).not.toHaveBeenCalled();
    fireEvent.click(trigger);
    const listbox = await screen.findByRole('listbox');
    expect(within(listbox).getAllByRole('option').map((o) => o.textContent)).toEqual([label('agents.chiefOfStaff'), 'Research']);
    fireEvent.click(within(listbox).getByRole('option', { name: 'Research' }));
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(api.createAutomation).toHaveBeenCalled());
    expect(created().workspace_id).toBe('ws-1');
  });
});

describe("while Home's request is out", () => {
  const leave = () => fireEvent.click(screen.getByRole('button', { name: label('automation.viewFeed') }));

  it('opens a new automation at once, and saves it in Home once Home answers', async () => {
    const answer = holdHome();
    await startFrom('automation.blankAutomation');
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));
    expect(await screen.findByRole('button', { name: label('common.saving') })).toBeDisabled();
    expect(api.createAutomation).not.toHaveBeenCalled();

    await answer();
    await waitFor(() => expect(api.createAutomation).toHaveBeenCalledTimes(1));
    expect(created()).toMatchObject({ name: 'Brief', instruction: 'Summarize.', workspace_id: 'home-1' });
  });

  it('saves a workspace the reader picked without waiting on Home', async () => {
    holdHome();
    await startFrom('automation.blankAutomation');
    fireEvent.click(screen.getByRole('button', { name: new RegExp(label('automation.moreOptions')) }));
    fireEvent.click(screen.getByRole('button', { name: new RegExp(label('agents.chiefOfStaff')) }));
    fireEvent.click(within(await screen.findByRole('listbox')).getByRole('option', { name: 'Research' }));
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(api.createAutomation).toHaveBeenCalled());
    expect(created().workspace_id).toBe('ws-1');
  });

  it('opens an edit once Home answers, reading a row in Home as the Chief of Staff, unchanged', async () => {
    const answer = holdHome();
    renderAt('/automations?id=auto-1', [IN_HOME]);
    const pane = (await screen.findByRole('heading', { level: 2, name: IN_HOME.name })).closest('article')!;
    fireEvent.click(within(pane).getByRole('button', { name: label('automation.edit') }));
    expect(screen.queryByRole('button', { name: label('automation.saveChanges') })).not.toBeInTheDocument();

    await answer();
    await screen.findByRole('button', { name: label('automation.saveChanges') });
    expect(screen.getByText(new RegExp(`^${label('agents.chiefOfStaff')} · `))).toBeInTheDocument();
    leave();
    expect(screen.queryByText(label('automation.discardDraftTitle'))).not.toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('button', { name: label('automation.saveChanges') })).not.toBeInTheDocument());
  });
});

describe('when Home cannot be read', () => {
  it('says why at save, as a refused write does, and keeps the form', async () => {
    home.getHomeWorkspace.mockRejectedValue(new Error('Network Error'));
    await startFrom('automation.blankAutomation');
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(toast).toHaveBeenCalledWith({ variant: 'destructive', description: 'Network Error' }));
    expect(api.createAutomation).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: label('common.create') })).toBeEnabled();
    expect(screen.getByLabelText(label('common.name'))).toHaveValue('Brief');
  });
});
