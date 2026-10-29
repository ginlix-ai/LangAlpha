/**
 * CreateWorkspaceCard: pending approval flow. "Always allow" answers through
 * `useApproveAlways`, whose save-then-answer contract is tested in
 * hooks/__tests__/useApproveAlways.test.tsx; here only the wiring.
 */
import { beforeEach, describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const approveAlways = vi.hoisted(() => ({ saving: false, fn: vi.fn((then: () => void) => then()) }));
vi.mock('@/hooks/useAutoApprove', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useAutoApprove')>();
  return {
    AUTO_APPROVE: actual.AUTO_APPROVE,
    useApproveAlways: () => ({ approveAlways: approveAlways.fn, saving: approveAlways.saving }),
  };
});

import CreateWorkspaceCard from '../CreateWorkspaceCard';

// A case that leaves the save out would disable every button in the next one.
beforeEach(() => {
  approveAlways.saving = false;
  approveAlways.fn.mockClear();
});

const PENDING = { status: 'pending' as const, workspace_name: 'New Workspace', workspace_description: 'A fresh workspace' };

describe('CreateWorkspaceCard: pending', () => {
  it('shows the workspace preview and actions', () => {
    renderWithProviders(
      <CreateWorkspaceCard proposalData={PENDING} onApprove={vi.fn()} onReject={vi.fn()} />,
    );
    expect(screen.getAllByText('New Workspace')[0]).toBeInTheDocument();
    expect(screen.getByText('A fresh workspace')).toBeInTheDocument();
    expect(screen.getByText('chat.createWorkspaceCard.create')).toBeInTheDocument();
  });

  it('approves on Create with no overrides', () => {
    const onApprove = vi.fn();
    renderWithProviders(
      <CreateWorkspaceCard proposalData={PENDING} onApprove={onApprove} onReject={vi.fn()} />,
    );
    fireEvent.click(screen.getByText('chat.createWorkspaceCard.create'));
    expect(onApprove).toHaveBeenCalledWith();
  });
});

describe('CreateWorkspaceCard: always allow', () => {
  it('answers through useApproveAlways, and waits while it saves', () => {
    const onApprove = vi.fn();
    const card = () => <CreateWorkspaceCard proposalData={PENDING} onApprove={onApprove} onReject={vi.fn()} />;
    const { rerender } = renderWithProviders(card());

    fireEvent.click(screen.getByText('chat.createWorkspaceCard.alwaysApprove'));

    expect(approveAlways.fn).toHaveBeenCalledOnce();
    expect(onApprove).toHaveBeenCalledWith();

    approveAlways.saving = true;
    rerender(card());

    expect(screen.getByText('chat.createWorkspaceCard.create').closest('button')).toBeDisabled();
    expect(screen.getByText('chat.createWorkspaceCard.alwaysApprove').closest('button')).toBeDisabled();
    expect(screen.getByText('chat.createWorkspaceCard.decline').closest('button')).toBeDisabled();
  });
});
