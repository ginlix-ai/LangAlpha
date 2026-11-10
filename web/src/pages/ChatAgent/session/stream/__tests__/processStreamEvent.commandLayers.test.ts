/**
 * A background command publishes its chart layers on the server with no stream
 * to carry them, so its report is the only word a chart already open gets:
 * the report that opens a turn and the one a live turn takes in each mark the
 * charts' layer lists stale, and a report with none touches nothing.
 */
import { QueryClient } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ReportSource } from '@/types/api';
import { chartLayersKey } from '@/pages/MarketView/utils/chartLayerCache';
import { chartKeyFor } from '@/pages/MarketView/utils/chartInstanceKey';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const WS = 'ws-cmd';
const published = (symbol: string) =>
  ({ workspace_id: WS, chart_id: `${symbol}:1day`, symbol, timeframe: '1day', layer_id: 'l1', title: 't', summary: null, as_of: null });
const report = (chart_layers?: ReturnType<typeof published>[]): ReportSource => ({
  kind: 'command_report', command_id: 'bg-1', tool: 'Bash', description: 'Backtest',
  status: 'completed', tool_call_id: 'call_1', ...(chart_layers ? { chart_layers } : {}),
});

const NVDA = chartLayersKey(chartKeyFor(WS, 'NVDA', '1day')!);
const AAPL = chartLayersKey(chartKeyFor(WS, 'AAPL', '1day')!);
let client: QueryClient;
const stale = (key: readonly unknown[]) => client.getQueryState(key)?.isInvalidated;

function setup() {
  const rt = {
    queryClient: client,
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    currentRunIdRef: { current: null },
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    currentMessageRef: { current: 'a1' },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = {
    clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn(), refreshWakeups: vi.fn(),
    refreshWorkspaceFolder: null, refreshCommands: vi.fn(),
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null },
    isReconnect: false,
  };
  return createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    () => null,
  );
}

beforeEach(() => {
  client = new QueryClient();
  client.setQueryData(NVDA, []);
  client.setQueryData(AAPL, []);
});
afterEach(() => client.clear());

describe('a command report carrying chart layers', () => {
  it('marks the reported chart stale when it opens a turn', () => {
    setup()({ event: 'metadata', run_id: 'run-2', turn_index: 3, report_source: report([published('NVDA')]) } as unknown as SSEEvent);
    expect([stale(NVDA), stale(AAPL)]).toEqual([true, false]);
  });

  it('marks the reported charts stale when a live turn takes it in', () => {
    setup()({
      event: 'steering_delivered',
      messages: [{ content: '', report_source: report([published('NVDA'), published('AAPL')]) }],
    } as unknown as SSEEvent);
    expect([stale(NVDA), stale(AAPL)]).toEqual([true, true]);
  });

  it('touches no chart when the report lists none', () => {
    const process = setup();
    process({ event: 'metadata', run_id: 'run-2', turn_index: 3, report_source: report() } as unknown as SSEEvent);
    process({ event: 'steering_delivered', messages: [{ content: '', report_source: report([]) }] } as unknown as SSEEvent);
    expect([stale(NVDA), stale(AAPL)]).toEqual([false, false]);
  });
});
