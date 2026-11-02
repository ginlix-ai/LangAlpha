/**
 * The drawer covers the thread it just opened, so every pick closes it, including
 * a pick that stays in this view (the thread already open, or a subagent's row).
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MobileNavDrawer } from '../MobileNavDrawer';
import type { NavTreeAgents } from '../../../hooks/useNavTreeProps';

const navigate = vi.hoisted(() => vi.fn());
vi.mock('react-router', async (actual) => ({
  ...(await actual<typeof import('react-router')>()),
  useNavigate: () => navigate,
}));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock('@/hooks/useWorkspaceLabel', () => ({
  useWorkspaceLabel: () => (ws?: { name?: string }) => ws?.name ?? '',
}));
vi.mock('../../../hooks/useNavigationData', () => ({
  useNavigationData: () => ({ workspaces: [], workspaceThreads: {} }),
}));
vi.mock('../../NavDisplayOptions', () => ({ default: () => null }));
vi.mock('../../NavigationPanel', () => ({
  default: ({ onNavigateThread, onNewThread, onSelectAgent }: {
    onNavigateThread: (ws: string, thread: string) => void;
    onNewThread: (ws: string, data: undefined) => void;
    onSelectAgent: (id: string) => void;
  }) => (
    <>
      <button onClick={() => onNavigateThread('ws-1', 'other')}>other thread</button>
      <button onClick={() => onNavigateThread('ws-1', 'current')}>current thread</button>
      <button onClick={() => onNewThread('ws-1', undefined)}>new thread</button>
      <button onClick={() => onSelectAgent('sub-1')}>subagent</button>
    </>
  ),
}));

function renderDrawer() {
  const onMinimize = vi.fn();
  const agents: NavTreeAgents = { agents: [], activeAgentId: null, onSelectAgent: vi.fn() };
  render(
    <MobileNavDrawer
      visible
      onMinimize={onMinimize}
      isActive
      workspaceId="ws-1"
      threadId="current"
      agents={agents}
      workspaceName="Workspace"
    />,
  );
  return { onMinimize, agents };
}

beforeEach(() => navigate.mockClear());

describe('MobileNavDrawer', () => {
  it('opens a picked thread and closes', () => {
    const { onMinimize } = renderDrawer();
    fireEvent.click(screen.getByText('other thread'));
    expect(navigate).toHaveBeenCalledWith('/chat/t/other', expect.anything());
    expect(onMinimize).toHaveBeenCalledTimes(1);
  });

  it('closes on the thread already open, without a second history entry', () => {
    const { onMinimize } = renderDrawer();
    fireEvent.click(screen.getByText('current thread'));
    expect(navigate).not.toHaveBeenCalled();
    expect(onMinimize).toHaveBeenCalledTimes(1);
  });

  it('closes on a new thread', () => {
    const { onMinimize } = renderDrawer();
    fireEvent.click(screen.getByText('new thread'));
    expect(navigate).toHaveBeenCalledWith('/chat/t/__default__', expect.anything());
    expect(onMinimize).toHaveBeenCalledTimes(1);
  });

  it("selects a subagent's row and closes", () => {
    const { onMinimize, agents } = renderDrawer();
    fireEvent.click(screen.getByText('subagent'));
    expect(agents.onSelectAgent).toHaveBeenCalledWith('sub-1');
    expect(onMinimize).toHaveBeenCalledTimes(1);
  });
});
