// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { applyFeedEvent, resetThreadLifecycle } from '@/lib/threadLifecycle/store';
import type { WorkflowStatusResponse } from '../api/messages';

const boot = vi.hoisted(() => ({ read: vi.fn() }));
vi.mock('../api/workflowStatus', () => ({ getWorkflowStatus: boot.read }));

import { primeWorkflowStatus, readWorkflowStatus } from '../primedStatus';

const THREAD = '7f0c1e5a-9d2b-4c8e-a1f3-2b6d9e0c4a71';
const idle = { can_reconnect: false, status: 'completed' } as WorkflowStatusResponse;
const live = { can_reconnect: true, status: 'running' } as WorkflowStatusResponse;

describe('readWorkflowStatus', () => {
  beforeEach(() => {
    boot.read.mockReset();
    resetThreadLifecycle();
    runAuthResets();
  });
  afterEach(() => resetThreadLifecycle());

  it('takes the boot read, with the time it was sent', async () => {
    boot.read.mockResolvedValue(idle);
    primeWorkflowStatus(THREAD);
    const ask = vi.fn();
    const { status } = await readWorkflowStatus(THREAD, ask);
    expect(status).toBe(idle);
    expect(ask).not.toHaveBeenCalled();
  });

  it('asks again when the feed reports a run started since the boot read', async () => {
    boot.read.mockResolvedValue(idle);
    primeWorkflowStatus(THREAD);
    applyFeedEvent({ type: 'run_started', thread_id: THREAD, run_id: 'run-1', run_seq: 1, status: 'running' });
    const ask = vi.fn().mockResolvedValue(live);
    expect((await readWorkflowStatus(THREAD, ask)).status).toBe(live);
    expect(ask).toHaveBeenCalledWith(THREAD);
  });

  it('asks again when a run started and settled since the boot read', async () => {
    boot.read.mockResolvedValue(idle);
    primeWorkflowStatus(THREAD);
    applyFeedEvent({ type: 'run_started', thread_id: THREAD, run_id: 'run-1', run_seq: 1, status: 'running' });
    applyFeedEvent({ type: 'run_settled', thread_id: THREAD, run_id: 'run-1', run_seq: 1, status: 'completed' });
    const settled = { ...idle, latest_turn_index: 1 };
    const ask = vi.fn().mockResolvedValue(settled);
    expect((await readWorkflowStatus(THREAD, ask)).status).toBe(settled);
  });

  it('keeps the boot read when the feed has said nothing new since', async () => {
    applyFeedEvent({ type: 'run_started', thread_id: THREAD, run_id: 'run-1', run_seq: 1, status: 'running' });
    const named = { ...live, run_id: 'run-1' };
    boot.read.mockResolvedValue(named);
    primeWorkflowStatus(THREAD);
    const ask = vi.fn();
    expect((await readWorkflowStatus(THREAD, ask)).status).toBe(named);
    expect(ask).not.toHaveBeenCalled();
  });

  it('asks again when the boot read failed', async () => {
    boot.read.mockRejectedValue(new Error('offline'));
    primeWorkflowStatus(THREAD);
    const ask = vi.fn().mockResolvedValue(idle);
    expect((await readWorkflowStatus(THREAD, ask)).status).toBe(idle);
  });

  it('drops the boot read when the account changes', async () => {
    boot.read.mockResolvedValue(live);
    primeWorkflowStatus(THREAD);
    runAuthResets();
    const ask = vi.fn().mockResolvedValue(idle);
    expect((await readWorkflowStatus(THREAD, ask)).status).toBe(idle);
  });
});
