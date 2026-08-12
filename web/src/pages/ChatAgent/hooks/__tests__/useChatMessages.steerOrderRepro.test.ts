/**
 * SCRATCH REPRO (uncommitted): drive the real history-replay path with the
 * exact event stream of the steering-order incident and inspect the
 * resulting message order.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import INCIDENT_EVENTS from './__fixtures__/steerReplayIncident.json';
import PROJECTED_EVENTS from './__fixtures__/steerWireProjected.json';
import REFRESH_EVENTS from './__fixtures__/steerWireRefresh.json';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue('thread-A'),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => {
  const harness = await import('./chatHookHarness');
  return harness.apiMockModule({
    getWorkflowStatus: vi.fn().mockResolvedValue(harness.threadStatus()),
  });
});

import { replayThreadHistory, getWorkflowStatus } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockReplay = replayThreadHistory as Mock;
const mockStatus = getWorkflowStatus as Mock;

interface AnyMsg {
  id: string;
  role: string;
  content?: string;
  steeringDelivered?: boolean;
  contentSegments?: Array<{ type: string; order: number; subagentId?: string; content?: string }>;
}

function describeMessages(msgs: AnyMsg[]): string[] {
  return msgs.map((m) => {
    const segs = (m.contentSegments || [])
      .map((s) => `${s.type}${s.type === 'subagent_task' ? `:${s.subagentId}` : ''}@${s.order}`)
      .join(',');
    const flags = m.steeringDelivered ? ' [steeringDelivered]' : '';
    const text = typeof m.content === 'string' ? m.content.slice(0, 40).replace(/\n/g, ' ') : '';
    return `${m.role}(${m.id})${flags} :: ${text} :: segs=[${segs}]`;
  });
}

describe('steering-order incident repro', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue({
      can_reconnect: false,
      status: 'completed',
      pending_report_back: false,
      active_tasks: ['prkuCg', 'zcYd1g'],
    });
    mockReplay.mockImplementation(async (_tid: string, cb: (e: Record<string, unknown>) => void) => {
      for (const e of INCIDENT_EVENTS as Record<string, unknown>[]) cb(e);
    });
  });

  const CASES: Array<[string, Record<string, unknown>[]]> = [
    ['stored-merge stream (today dump)', INCIDENT_EVENTS as Record<string, unknown>[]],
    ['projected-only stream (stored=[])', PROJECTED_EVENTS as Record<string, unknown>[]],
    ['refresh-time stream (main-only stored merge)', REFRESH_EVENTS as Record<string, unknown>[]],
  ];

  it.each(CASES)('renders the steering bubble between the two task dispatches — %s', async (_label, events) => {
    mockReplay.mockImplementation(async (_tid: string, cb: (e: Record<string, unknown>) => void) => {
      for (const e of events) cb(e);
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws-A'));

    await waitFor(() => {
      expect(mockReplay).toHaveBeenCalledTimes(1);
      expect(result.current.messages.length).toBeGreaterThan(0);
    });
    // allow post-replay effects to settle
    await new Promise((r) => setTimeout(r, 20));

    const msgs = result.current.messages as unknown as AnyMsg[];
    // eslint-disable-next-line no-console
    console.log('=== MESSAGE ORDER ===');
    for (const line of describeMessages(msgs)) console.log(line);

    const steerIdx = msgs.findIndex((m) => m.role === 'user' && m.steeringDelivered);
    expect(steerIdx).toBeGreaterThan(-1);

    // every subagent_task segment BEFORE the steering bubble must be dispatch 1
    // (never dispatch 2); dispatch 2 must appear only AFTER the steering bubble
    const segsBefore = msgs.slice(0, steerIdx).flatMap((m) => m.contentSegments || []);
    const segsAfter = msgs.slice(steerIdx + 1).flatMap((m) => m.contentSegments || []);
    const tasksBefore = segsBefore.filter((s) => s.type === 'subagent_task').length;
    const tasksAfter = segsAfter.filter((s) => s.type === 'subagent_task').length;
    // eslint-disable-next-line no-console
    console.log('tasksBefore=', tasksBefore, 'tasksAfter=', tasksAfter);

    expect(tasksBefore).toBe(1);
    expect(tasksAfter).toBe(1);
  });
});
