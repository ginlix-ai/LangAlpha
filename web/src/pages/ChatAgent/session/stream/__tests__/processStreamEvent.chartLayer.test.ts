/**
 * A published chart layer arrives as a `chart_layer` artifact and goes straight
 * to the query cache, from the main lane and from a subagent's: the server has
 * stored it either way, so a chart on screen would otherwise miss it until a
 * reload. The transcript's card is not made here; it comes from the tool
 * result's `chart_layers`.
 */
import { QueryClient } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { chartLayersKey } from '@/pages/MarketView/utils/chartLayerCache';
import { chartKeyFor } from '@/pages/MarketView/utils/chartInstanceKey';
import { resetLayerViewForTesting } from '@/pages/MarketView/stores/layerViewStore';
import type { StoredChartLayer } from '@/pages/MarketView/types/chartLayer';
import type { ChartLayerArtifactPayload } from '@/pages/MarketView/types/chartLayer';
import { addPayload } from '@/pages/MarketView/stores/__tests__/layerFixture';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const WS = 'ws-stream';
const CHART = 'NVDA:1day';

function setup() {
  let messages = [
    { id: 'u0', role: 'user', content: 'chart it', turnIndex: 0 },
    {
      id: 'a1', role: 'assistant', content: '', isStreaming: true, turnIndex: 0,
      contentSegments: [], reasoningProcesses: {}, toolCallProcesses: {},
    },
  ] as unknown as ChatMessage[];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const rt = {
    queryClient: client,
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    currentMessageRef: { current: 'a1' as string | null },
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = { clearModelStatus: () => {}, refreshMarketWatch: vi.fn(), refreshWakeups: vi.fn() };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null as string | null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null as number | null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  return { process, refs, read: () => messages };
}

const artifact = (agent: string, payload: ChartLayerArtifactPayload) =>
  ({ event: 'artifact', agent, artifact_type: 'chart_layer', artifact_id: 'nvda-channel', payload }) as unknown as SSEEvent;

const KEY = chartLayersKey(chartKeyFor(WS, 'NVDA', '1day')!);
let client: QueryClient;
const layersOf = () => client.getQueryData<StoredChartLayer[]>(KEY)?.map((row) => row.layer_id);
/** Event application waits out a cancel before it writes. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

beforeEach(() => {
  client = new QueryClient();
  // A chart on screen has read the server's list.
  client.setQueryData(KEY, []);
  resetLayerViewForTesting();
});
afterEach(() => client.clear());

describe('chart_layer artifact', () => {
  it('adds, then removes, a layer from the main lane without touching the transcript', async () => {
    const { process, read } = setup();
    const before = read();
    process(artifact('main', addPayload(WS)));
    await settle();
    expect(layersOf()).toEqual(['nvda-channel']);
    process(artifact('main', { op: 'remove', workspace_id: WS, chart_id: CHART, symbol: 'NVDA', timeframe: '1day', ids: ['nvda-channel'] }));
    await settle();
    expect(layersOf()).toEqual([]);
    expect(read()).toEqual(before);
  });

  it('takes a subagent’s publish into the cache as well', async () => {
    const { process } = setup();
    process(artifact('task:abc123', addPayload(WS)));
    await settle();
    expect(layersOf()).toEqual(['nvda-channel']);
  });

  it('leaves a reconnect’s replayed add to the server’s list', async () => {
    const { process, refs } = setup();
    refs.isReconnect = true;
    process(artifact('main', addPayload(WS)));
    await settle();
    expect(layersOf()).toEqual([]);
    expect(client.getQueryState(KEY)?.isInvalidated).toBe(true);
  });

  it('survives a payload with nothing in it', async () => {
    const { process } = setup();
    process({ event: 'artifact', agent: 'main', artifact_type: 'chart_layer', artifact_id: 'x' } as unknown as SSEEvent);
    await settle();
    expect(layersOf()).toEqual([]);
  });
});

// The retired annotation tool's live artifact. The server no longer sends it,
// but an older cached stream could: it is dropped without touching anything.
describe('chart_annotation artifact', () => {
  const legacy = (agent: string) =>
    ({
      event: 'artifact', agent, artifact_type: 'chart_annotation', artifact_id: 'ann_1',
      payload: {
        type: 'chart_annotation', op: 'add', workspace_id: WS, chart_id: CHART, symbol: 'NVDA', timeframe: '1day',
        annotation_id: 'ann_1', annotation: { annotation_id: 'ann_1', type: 'price_line', symbol: 'NVDA', price: 180 },
      },
    }) as unknown as SSEEvent;

  it('is ignored on the main lane and a subagent lane', () => {
    const { process, read } = setup();
    const before = read();
    expect(() => {
      process(legacy('main'));
      process(legacy('task:abc123'));
    }).not.toThrow();
    expect(read()).toEqual(before);
    expect(layersOf()).toEqual([]);
  });
});
