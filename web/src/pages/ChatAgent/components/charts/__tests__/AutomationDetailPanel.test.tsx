import { describe, expect, it } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import AutomationDetailPanel from '../AutomationDetailPanel';

// The cards are kept history from a tool that named the agent a run used,
// never a workspace.
const card = (place: Record<string, unknown>) => ({
  automation_id: 'auto-1',
  name: 'Morning briefing',
  status: 'active',
  trigger_type: 'cron',
  schedule: '0 9 * * 1-5',
  ...place,
});

function renderList(...automations: Record<string, unknown>[]) {
  renderWithProviders(<AutomationDetailPanel data={{ type: 'automations', mode: 'list', automations, total: automations.length }} />);
}

describe('where an automation card says it ran', () => {
  it('reads a card that ran outside a workspace as Chief of Staff and a ptc card as a workspace', () => {
    renderList(card({ agent_mode: 'home' }), card({ agent_mode: 'ptc' }));
    expect(screen.getByText('Chief of Staff')).toBeInTheDocument();
    expect(screen.getByText('Workspace')).toBeInTheDocument();
  });

  it('says nothing for a card that names no agent', () => {
    renderList(card({}));
    expect(screen.queryByText('Chief of Staff')).not.toBeInTheDocument();
    expect(screen.queryByText('Workspace')).not.toBeInTheDocument();
  });

  it('names the place in the detail view under the Agent row', () => {
    renderWithProviders(
      <AutomationDetailPanel
        data={{ type: 'automations', mode: 'detail', automation: card({ agent_mode: 'home' }), executions: [] }}
      />,
    );
    expect(screen.getByText('Agent').parentElement).toHaveTextContent('Chief of Staff');
  });
});
