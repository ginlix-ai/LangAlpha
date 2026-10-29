/**
 * The link from a hand-off down to the analyst's report-back row further on
 * in the same conversation. The list matches each hand-off to the report that
 * answers it; the slip only needs its own match.
 */
import { createContext, useContext } from 'react';
import type { ReportOutcome } from '@/types/api';
import type { MessageProvenance, PTCAgentProposalState } from '@/types/chat';
import type { DispatchPin } from '../../hooks/usePTCDispatchStatus';
import { preapprovedCardOf, type ToolCallLike } from '../../utils/preapprovedCards';
import { isReportRow } from '../messageList/messagePredicates';
import type { MessageRecord } from '../messageList/types';
import { messageTimeMs } from '../messageList/turnProjection';
import { followedRun } from './handoffModel';

/** Where a hand-off sits in the conversation. */
export interface HandoffPlace {
  /** When its reply began (epoch ms), which is no later than the hand-off. */
  madeAt?: number;
  /** The id of the report-back row that answers it, once one is on screen. */
  reportId?: string;
  /** The run that report is on, which is where the hand-off ended. */
  reportRunId?: string;
  /** How that run ended, as the report has it; absent on older reports. */
  outcome?: ReportOutcome;
  /** The report follows the hand-off straight on: nothing between them but
   *  replies and reports. */
  reportFollows?: boolean;
}

/** A hand-off's identity for its whole life: the tool call that made it, or
 *  the approval request that carried it. */
export function handoffKey(proposal: PTCAgentProposalState): string | undefined {
  return proposal.tool_call_id || proposal.interruptId || undefined;
}

interface Handoff {
  key: string;
  threadId: string;
  pin: DispatchPin;
  at: number;
}

interface Report {
  id: string;
  threadId: string;
  /** The interrupted runs the reported run continued, then that run. */
  runIds: string[];
  /** The steers those runs took in: empty when they took none, absent when
   *  unknown (a report from before the list, or one the server could not
   *  read it for). */
  steerIds?: string[];
  outcome?: ReportOutcome;
  at: number;
  claimed: boolean;
}

type HandoffMessage = MessageRecord & {
  contentSegments?: { type: string; proposalId?: string; toolCallId?: string }[];
  ptcAgentProposals?: Record<string, PTCAgentProposalState>;
  toolCallProcesses?: Record<string, ToolCallLike>;
};

/** The hand-offs a reply drew, in the order it drew them. */
function handoffsOf(message: HandoffMessage): PTCAgentProposalState[] {
  const proposals = message.ptcAgentProposals ?? {};
  const processes = message.toolCallProcesses ?? {};
  const out: PTCAgentProposalState[] = [];
  for (const seg of message.contentSegments ?? []) {
    if (seg.type === 'ptc_agent') {
      const proposal = seg.proposalId ? proposals[seg.proposalId] : undefined;
      if (proposal) out.push(proposal);
    } else if (seg.type === 'tool_call' && seg.toolCallId) {
      const card = preapprovedCardOf(processes[seg.toolCallId]);
      if (card?.type === 'ptc_agent') out.push(card.proposal);
    }
  }
  return out;
}

/**
 * Matches each hand-off to the report that answers it. A report naming the
 * hand-off's run is its answer, including a report on a later run that
 * continued it past an approval; several hand-offs steered into one run share
 * it. A hand-off that names its steer is answered by the report naming that
 * steer, never by its run, which may have ended without reading it. A report
 * whose steers are unknown cannot say, so the order below may still match it
 * to a steered hand-off; one whose list does not name the steer, even an
 * empty list, is not its answer. Where either side has no run id (a report or
 * a hand-off from before they carried one, or a dispatch whose outcome is
 * unknown), the first report for the same thread after the hand-off that no
 * other hand-off has taken. Last, a report naming no run any hand-off here was
 * pinned to, the one a steer went into included: a newer run on the analyst's
 * thread took that report over and names only itself, so it answers the
 * oldest hand-off on the thread still without one.
 */
export function placeHandoffs(messages: readonly { message: MessageRecord }[]): [string, HandoffPlace][] {
  const handoffs: Handoff[] = [];
  const reports: Report[] = [];
  const places = new Map<string, HandoffPlace>();
  messages.forEach(({ message }, at) => {
    if (message.role === 'user') {
      const provenance = message.provenance as MessageProvenance | undefined;
      if (provenance?.kind === 'analyst_report' && provenance.threadId) {
        const { threadId, runIds = [], steerIds, outcome } = provenance;
        reports.push({ id: String(message.id), threadId, runIds, steerIds, outcome, at, claimed: false });
      }
      return;
    }
    if (message.role !== 'assistant') return;
    const madeAt = messageTimeMs(message.timestamp);
    for (const proposal of handoffsOf(message as HandoffMessage)) {
      const key = handoffKey(proposal);
      const run = followedRun(proposal);
      if (!key || !run) continue;
      places.set(key, { madeAt });
      handoffs.push({ key, threadId: run.threadId, pin: run.pin, at });
    }
  });

  const answered = (h: Handoff, r: Report) => {
    r.claimed = true;
    places.set(h.key, {
      ...places.get(h.key),
      reportId: r.id,
      reportRunId: r.runIds.at(-1),
      outcome: r.outcome,
      reportFollows: followsOn(messages, h.at, r.at),
    });
  };
  const names = (r: Report, { pin }: Handoff) =>
    pin.kind === 'steer' ? !!r.steerIds?.includes(pin.steerId) : pin.kind === 'run' && r.runIds.includes(pin.runId);
  // Only a report whose steers are unknown can answer a steered hand-off
  // without naming it.
  const after = (r: Report, h: Handoff) =>
    !r.claimed && r.threadId === h.threadId && r.at > h.at && !(h.pin.kind === 'steer' && r.steerIds);
  const open: Handoff[] = [];
  for (const h of handoffs) {
    const own = reports.find((r) => names(r, h) && r.at > h.at);
    if (own) answered(h, own);
    else open.push(h);
  }
  const unanswered: Handoff[] = [];
  for (const h of open) {
    const report = reports.find((r) => after(r, h) && (h.pin.kind !== 'run' || r.runIds.length === 0));
    if (report) answered(h, report);
    else unanswered.push(h);
  }
  const pinned = new Set(handoffs.flatMap(({ pin }) =>
    pin.kind === 'run' ? [pin.runId] : pin.kind === 'steer' ? [pin.steeredInto] : []));
  for (const h of unanswered) {
    const report = reports.find((r) => after(r, h) && !r.runIds.some((id) => pinned.has(id)));
    if (report) answered(h, report);
  }
  return [...places];
}

/**
 * Whether the message at `to` follows the one at `from` straight on: only
 * replies and reports lie between, the stretch a reader reads on through
 * while the user says nothing. A message of the user's or a divider between
 * means the conversation moved on in the meantime.
 */
function followsOn(messages: readonly { message: MessageRecord }[], from: number, to: number): boolean {
  for (let i = to - 1; i > from; i--) {
    const { message } = messages[i];
    if (message.role !== 'assistant' && !isReportRow(message)) return false;
  }
  return true;
}

/** Whether two `placeHandoffs` results say the same, so the list can keep
 *  the map it built from the first. */
export function samePlaces(a: [string, HandoffPlace][], b: [string, HandoffPlace][]): boolean {
  return a.length === b.length && a.every(([key, x], i) => {
    const [otherKey, y] = b[i];
    return key === otherKey && x.madeAt === y.madeAt && x.reportId === y.reportId
      && x.reportRunId === y.reportRunId && x.outcome === y.outcome
      && x.reportFollows === y.reportFollows;
  });
}

const NONE: ReadonlyMap<string, HandoffPlace> = new Map();

/** Every hand-off in the transcript on screen, by `handoffKey`. */
export const HandoffPlacesContext = createContext<ReadonlyMap<string, HandoffPlace>>(NONE);

export function useHandoffPlaces(): ReadonlyMap<string, HandoffPlace> {
  return useContext(HandoffPlacesContext);
}

const NOWHERE: HandoffPlace = {};

export function placeOf(places: ReadonlyMap<string, HandoffPlace>, proposal: PTCAgentProposalState): HandoffPlace {
  const key = handoffKey(proposal);
  return (key && places.get(key)) || NOWHERE;
}
