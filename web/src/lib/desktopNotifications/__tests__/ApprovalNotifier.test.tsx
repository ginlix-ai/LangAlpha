/**
 * The desktop notification for a request waiting on the user: posted once
 * per request, taken down once it is no longer waiting or the page goes.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import type { CliApproval } from '@/api/cliApprovals';

const desktop = vi.hoisted(() => ({
  desktopNotify: vi.fn(async () => ({ shown: true })),
  clearDesktopNotifications: vi.fn(),
}));
vi.mock('@/lib/desktop', () => desktop);

const pending = vi.hoisted(() => ({ list: [] as CliApproval[] }));
vi.mock('@/hooks/useCliApprovals', () => ({ usePendingCliApprovals: () => pending.list }));

import { ApprovalNotifier } from '../ApprovalNotifier';

function approval(id: string, over: Pick<Partial<CliApproval>, 'requester'> = {}): CliApproval {
  return {
    approval_id: id,
    status: 'pending',
    action: 'thread_delete',
    target: { id: 't-9', name: 'Old notes' },
    details: {},
    requester: {
      kind: 'analyst',
      thread_id: 't-1',
      thread_title: 'Q3 prep',
      workspace_id: 'w-1',
      workspace_name: 'NVDA',
      computer_name: null,
    },
    created_at: '2026-11-14T00:00:00Z',
    expires_at: '2026-11-14T00:05:00Z',
    ...over,
  };
}

beforeEach(() => {
  desktop.desktopNotify.mockClear();
  desktop.clearDesktopNotifications.mockClear();
  pending.list = [];
});

describe('ApprovalNotifier', () => {
  it('posts each request once, as a question, and takes it down when answered', () => {
    pending.list = [approval('ap-1')];
    const view = render(<ApprovalNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(1);
    expect(desktop.desktopNotify).toHaveBeenCalledWith({
      id: 'cli-approval:ap-1',
      kind: 'needs_input',
      title: 'Delete thread “Old notes”?',
      body: 'The analyst in “NVDA” asked in “Q3 prep” to delete this thread.',
      path: '/chat/t/t-1',
    });

    pending.list = [approval('ap-1'), approval('ap-2', { requester: { ...approval('x').requester, thread_id: null } })];
    view.rerender(<ApprovalNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(2);
    expect(desktop.desktopNotify).toHaveBeenLastCalledWith(expect.objectContaining({ id: 'cli-approval:ap-2', path: '/' }));

    pending.list = [approval('ap-2')];
    view.rerender(<ApprovalNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(2);
    expect(desktop.clearDesktopNotifications).toHaveBeenLastCalledWith(['cli-approval:ap-1']);

    view.unmount();
    expect(desktop.clearDesktopNotifications).toHaveBeenLastCalledWith(['cli-approval:ap-2']);
  });

  it('posts an action this version does not know as a plain question', () => {
    pending.list = [{ ...approval('ap-new'), action: 'drive_purge_all', target: { id: 'x', name: 'Everything' } } as unknown as CliApproval];
    render(<ApprovalNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledWith(expect.objectContaining({
      id: 'cli-approval:ap-new',
      title: 'Allow “Everything”?',
      body: 'The analyst in “NVDA” asked in “Q3 prep” to do something this version of the app cannot describe. Reload the page to see what it is.',
    }));
  });
});
