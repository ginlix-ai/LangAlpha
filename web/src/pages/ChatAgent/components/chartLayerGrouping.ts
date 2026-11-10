/**
 * Which chart-layer cards a transcript draws, and where.
 *
 * A layer is published from inside an ExecuteCode or Bash call, so the call has
 * a result of its own to show and its row stays in the timeline. The result's
 * artifact lists what the call published (`chart_layers`), and a command handed
 * off to the background lists it on its report-back instead. A turn draws one
 * card per chart instance: pinned beside the first publish on it, and listing
 * every layer published there since. A layer republished under the same id
 * replaces its entry (the server replaces the stored row the same way), keeping
 * its place.
 *
 * A report row opens the turn it reports into, so a chart its report published
 * on has its card under the row, and the turn's message folds its own publishes
 * on that chart into that card rather than drawing a second one
 * (`planReportChartCards`).
 */

import type { PublishedLayer } from '@/pages/MarketView/types/chartLayer';
import { chartKeyFor, chartKeyParts, type ChartKey } from '@/pages/MarketView/utils/chartInstanceKey';
import { displaySpelling } from '@/lib/bars/exchanges';
import type { MessageProvenance } from '@/types/chat';
import { isReportRow } from './messageList/messagePredicates';
import type { ProjectedMessage } from './messageList/turnProjection';
import type { ContentSegmentRecord, MessageRecord, ToolCallProcessRecord } from './messageList/types';

/** One chart instance's card. */
export interface ChartLayersCard {
  /** The chart's key, or for an entry naming no workspace (which the server
   *  never sends) its raw chart, so such a card still groups. */
  id: string;
  /** Null for that entry: the card names no chart to show the layers on. */
  key: ChartKey | null;
  workspaceId: string | null;
  /** Display spelling. */
  symbol: string;
  timeframe: string;
  /** In first-publish order, each layer once at its latest version. */
  layers: PublishedLayer[];
}

function openCard(entry: PublishedLayer): ChartLayersCard {
  const key = chartKeyFor(entry.workspace_id, entry.symbol, entry.timeframe);
  if (key) {
    const { workspaceId, symbol, timeframe } = chartKeyParts(key);
    return { id: key, key, workspaceId, symbol, timeframe, layers: [] };
  }
  return {
    id: `${entry.workspace_id ?? ''}|${entry.chart_id}`,
    key: null,
    workspaceId: entry.workspace_id || null,
    symbol: displaySpelling(entry.symbol) || entry.symbol,
    timeframe: entry.timeframe,
    layers: [],
  };
}

/** The entries a call's result lists (trusted server data), if any. */
export function publishedBy(proc: ToolCallProcessRecord | undefined): readonly PublishedLayer[] | undefined {
  const entries = proc?.toolCallResult?.artifact?.chart_layers;
  return Array.isArray(entries) && entries.length > 0 ? (entries as PublishedLayer[]) : undefined;
}

/**
 * Fold entries into `cards`, keyed by card id, opening a card for a chart not
 * yet in it. A layer already listed is replaced in its place. Entries on a
 * chart in `omit` are left out. Returns the cards the entries landed in, in
 * the order first touched.
 */
function foldChartLayers(
  cards: Map<string, ChartLayersCard>,
  entries: readonly PublishedLayer[],
  omit?: ReadonlySet<ChartKey>,
): ChartLayersCard[] {
  const touched: ChartLayersCard[] = [];
  for (const entry of entries) {
    const opened = openCard(entry);
    if (opened.key && omit?.has(opened.key)) continue;
    let card = cards.get(opened.id);
    if (!card) {
      card = opened;
      cards.set(card.id, card);
    }
    if (!touched.includes(card)) touched.push(card);
    const slot = card.layers.findIndex((l) => l.layer_id === entry.layer_id);
    if (slot >= 0) card.layers[slot] = entry;
    else card.layers.push(entry);
  }
  return touched;
}

/**
 * Walk a message's segments in order and fold every call's `chart_layers`
 * into one card per chart instance. Returns the cards keyed by the call they
 * sit beside (the first that published on the chart), in the order that call
 * listed them. Charts in `omit` are carded elsewhere in the turn.
 */
export function planChartLayerCards(
  segments: readonly ContentSegmentRecord[],
  toolCallProcesses: Readonly<Record<string, ToolCallProcessRecord | undefined>>,
  omit?: ReadonlySet<ChartKey>,
): Map<string, ChartLayersCard[]> {
  const cards = new Map<string, ChartLayersCard>();
  const anchored = new Map<string, ChartLayersCard[]>();
  const placed = new Set<ChartLayersCard>();
  for (const seg of segments) {
    if (seg.type !== 'tool_call' || !seg.toolCallId) continue;
    const entries = publishedBy(toolCallProcesses[seg.toolCallId]);
    if (!entries) continue;
    for (const card of foldChartLayers(cards, entries, omit)) {
      if (placed.has(card)) continue;
      placed.add(card);
      const at = anchored.get(seg.toolCallId);
      if (at) at.push(card);
      else anchored.set(seg.toolCallId, [card]);
    }
  }
  return anchored;
}

/** Where the turns opened by report rows draw their chart cards. */
export interface ReportChartCards {
  /** The cards under each report row, by message id: the charts its report
   *  is the turn's first publish on, with the turn's later publishes there
   *  folded in. */
  byReport: ReadonlyMap<string, ChartLayersCard[]>;
  /** The charts carded under a report row, for each assistant message of
   *  that turn, which draws no card of its own for them. */
  omitByMessage: ReadonlyMap<MessageRecord, ReadonlySet<ChartKey>>;
}

const NO_REPORT_CARDS: ReportChartCards = { byReport: new Map(), omitByMessage: new Map() };

interface TurnCards {
  cards: Map<string, ChartLayersCard>;
  carded: Set<ChartKey>;
}

function reportedLayers(message: MessageRecord): readonly PublishedLayer[] | undefined {
  const provenance = message.provenance as MessageProvenance | undefined;
  return provenance?.kind === 'command_report' && provenance.chartLayers?.length ? provenance.chartLayers : undefined;
}

/**
 * The report rows' chart cards and what each turn's messages leave to them.
 * Several report rows can open one turn; a chart's card is under the first
 * that published on it.
 */
export function planReportChartCards(projected: readonly ProjectedMessage[]): ReportChartCards {
  let turns: Map<number, TurnCards> | null = null;
  let byReport: Map<string, ChartLayersCard[]> | null = null;
  const omitByMessage = new Map<MessageRecord, ReadonlySet<ChartKey>>();
  for (const { message, turnIndex } of projected) {
    if (isReportRow(message)) {
      const entries = reportedLayers(message);
      if (!entries) continue;
      // A row with no turn has no message to share its charts with.
      let turn: TurnCards = { cards: new Map(), carded: new Set() };
      if (turnIndex !== null) {
        turns ??= new Map();
        turn = turns.get(turnIndex) ?? turn;
        turns.set(turnIndex, turn);
      }
      const known = new Set(turn.cards.values());
      const own = foldChartLayers(turn.cards, entries).filter((card) => !known.has(card));
      for (const card of own) if (card.key) turn.carded.add(card.key);
      if (own.length > 0) (byReport ??= new Map()).set(message.id as string, own);
      continue;
    }
    if (message.role !== 'assistant' || turnIndex === null) continue;
    const turn = turns?.get(turnIndex);
    if (!turn || turn.carded.size === 0) continue;
    // Its own copy: a report row later in the turn cards more charts, and this
    // message's publishes on those were not folded into that row's card.
    omitByMessage.set(message, new Set(turn.carded));
    const processes = (message.toolCallProcesses ?? {}) as Record<string, ToolCallProcessRecord | undefined>;
    const segments = [...((message.contentSegments ?? []) as ContentSegmentRecord[])].sort((a, b) => a.order - b.order);
    for (const seg of segments) {
      if (seg.type !== 'tool_call' || !seg.toolCallId) continue;
      const entries = publishedBy(processes[seg.toolCallId]);
      if (!entries) continue;
      const onCarded = entries.filter((entry) => {
        const key = chartKeyFor(entry.workspace_id, entry.symbol, entry.timeframe);
        return key !== null && turn.carded.has(key);
      });
      foldChartLayers(turn.cards, onCarded);
    }
  }
  return byReport ? { byReport, omitByMessage } : NO_REPORT_CARDS;
}
