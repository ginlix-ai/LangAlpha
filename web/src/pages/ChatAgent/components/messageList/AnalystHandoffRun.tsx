import React, { useLayoutEffect, useRef, useState } from 'react';
import { AnimatePresence } from '@/lib/framer';
import type { PTCAgentProposalState } from '@/types/chat';
import { useDispatchStatuses } from '../../hooks/usePTCDispatchStatus';
import { useOpenDispatchedThread, type DispatcherContext } from '../../hooks/useOpenDispatchedThread';
import { ResolveSwap, TOGETHER } from '../decisionCardMotion';
import { HandoffApproval } from '../analystHandoff/HandoffApproval';
import { HandoffDeclined } from '../analystHandoff/HandoffDeclined';
import { HandoffGroup } from '../analystHandoff/HandoffGroup';
import { LeaveFold } from '../analystHandoff/handoffMotion';
import { describeHandoff, followedRun, type HandoffView } from '../analystHandoff/handoffModel';
import { HandoffSlip } from '../analystHandoff/HandoffSlip';
import { placeOf, useHandoffPlaces } from '../analystHandoff/reportLinks';
import { useMessageActions, type MessageActions } from './MessageActionsContext';

export interface AnalystHandoffMember {
  /** The render block's key, stable for the hand-off's life. */
  key: string;
  proposal: PTCAgentProposalState;
  proposalId: string;
}

type Piece =
  | { kind: 'handed'; key: string; members: AnalystHandoffMember[] }
  | { kind: 'single'; key: string; member: AnalystHandoffMember };

/** Each handed-off member's key, to the key of the stack it was drawn in. */
type StackKeys = ReadonlyMap<string, string>;

/**
 * Handed-off members side by side form one stack; one awaiting an answer, or
 * declined, stands on its own where it falls, so the order reads as written.
 *
 * A piece's key is its box's identity across an answer. One on its own is
 * keyed by its member, so a card answered into a slip of its own is the same
 * box, and resolves in place. A stack keeps the key it was last drawn under,
 * by any member it held, so a stack that takes in an approved neighbour grows
 * rather than remounts, and the neighbour's card is the piece that leaves. A
 * stack drawn for the first time takes a member's key.
 */
function piecesOf(members: readonly AnalystHandoffMember[], stackKeys: StackKeys): Piece[] {
  const runs: Array<AnalystHandoffMember[] | AnalystHandoffMember> = [];
  for (const m of members) {
    const last = runs[runs.length - 1];
    if (m.proposal.status !== 'approved') runs.push(m);
    else if (Array.isArray(last)) last.push(m);
    else runs.push([m]);
  }
  // Two stacks a decline once split can meet again under one key; the first
  // keeps it and the other takes a member's.
  const claimed = new Set(runs.flatMap((run) => (Array.isArray(run) ? [] : [run.key])));
  return runs.map((run): Piece => {
    if (!Array.isArray(run)) return { kind: 'single', key: run.key, member: run };
    const candidates = [...run.map((m) => stackKeys.get(m.key)), ...run.map((m) => m.key)];
    const key = candidates.find((k): k is string => k !== undefined && !claimed.has(k))!;
    claimed.add(key);
    return { kind: 'handed', key, members: run };
  });
}

function stackKeysOf(pieces: readonly Piece[]): StackKeys {
  const keys = new Map<string, string>();
  for (const p of pieces) if (p.kind === 'handed') for (const m of p.members) keys.set(m.key, p.key);
  return keys;
}

function sameKeys(a: StackKeys, b: StackKeys): boolean {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) if (b.get(k) !== v) return false;
  return true;
}

/** The run's pieces, keyed as they were last drawn (see `piecesOf`). */
function usePieces(members: readonly AnalystHandoffMember[]): Piece[] {
  const [stackKeys, setStackKeys] = useState<StackKeys>(() => stackKeysOf(piecesOf(members, new Map())));
  const pieces = piecesOf(members, stackKeys);
  const next = stackKeysOf(pieces);
  if (!sameKeys(next, stackKeys)) setStackKeys(next);
  return pieces;
}

const answered = (m: AnalystHandoffMember) => m.proposal.status === 'approved' || m.proposal.status === 'rejected';

const CONTROL = 'button:not([disabled]), a[href]';

/** The hand-off whose card or row holds `el`. */
function handoffOf(el: Element | null): string | null {
  const at = el?.closest('[data-handoff-card], [data-handoff-member]');
  return at ? at.getAttribute('data-handoff-card') ?? at.getAttribute('data-handoff-member') : null;
}

/**
 * Hands focus from an answered card on to what it became: the slip's way
 * into its thread, the row it took in a group, the declined line's toggle, or
 * the row itself while it has no control yet. A slip that gives way to a
 * group, or folds out of the run into a stack, hands it on the same way. A
 * group folded shut has no rows drawn, so its header takes it. Focus the
 * reader has since taken somewhere else stays there.
 */
function focusAnswered(root: HTMLElement, key: string): void {
  const active = root.ownerDocument.activeElement;
  if (active && active !== root.ownerDocument.body && handoffOf(active) !== key) return;
  const live = (el: Element) => !el.closest('[inert]');
  const landing = [...root.querySelectorAll('[data-handoff-member]')]
    .find((el) => el.getAttribute('data-handoff-member') === key && live(el))
    ?? [...root.querySelectorAll('[data-handoff-members]')]
      .find((el) => el.getAttribute('data-handoff-members')!.split(' ').includes(key) && live(el));
  if (!(landing instanceof HTMLElement)) return;
  const target = landing.querySelector<HTMLElement>(CONTROL) ?? (landing.tabIndex === -1 ? landing : null);
  target?.focus({ preventScroll: true });
}

/**
 * Hand-offs handed off side by side: a slip alone, a group from two on. When
 * a second joins a slip, the slip gives way to the group in place, its box
 * tweening on `TOGETHER` in step with the card that joined it leaving.
 */
function HandoffStack({ views }: { views: HandoffView[] }): React.ReactElement {
  const grouped = views.length > 1;
  return (
    <ResolveSwap resolved={grouped} settled={grouped && <HandoffGroup views={views} />} enter={false} leg={TOGETHER}>
      {!grouped && <HandoffSlip view={views[0]} />}
    </ResolveSwap>
  );
}

/**
 * Hand-offs to workspace analysts made back to back in a reply, with nothing
 * between them. Keyed by the first, so a later hand-off grows the run without
 * remounting the ones already drawn. Two or more already handed off read as
 * one group under one header. Every handed-off run is read here,
 * in one batch, so a group's header and its rows say the same from the first
 * paint.
 *
 * An answer moves the card in place. Alone, the card resolves into its slip or
 * its declined line through `ResolveSwap`. Beside a stack, it joins it: the
 * card folds out of the run and the stack takes it in as a row, or a slip
 * becomes a group, every box on one clock so what is below glides once. A run
 * drawn answered (a reload, a replay) draws as it stands.
 */
export function AnalystHandoffRun({
  members,
  onApprove,
  onReject,
  dispatcherContext,
}: {
  members: AnalystHandoffMember[];
  onApprove?: MessageActions['onApprovePTCAgent'];
  onReject?: MessageActions['onRejectPTCAgent'];
  dispatcherContext?: DispatcherContext | null;
}): React.ReactElement | null {
  const places = useHandoffPlaces();
  const { onRevealMessage } = useMessageActions();
  const openDispatchedThread = useOpenDispatchedThread(dispatcherContext);
  const handed = members.filter((m) => m.proposal.status === 'approved');
  const reads = useDispatchStatuses(handed.map((m) => followedRun(m.proposal, placeOf(places, m.proposal))));
  const states = new Map(handed.map((m, i) => [m.key, describeHandoff(m.proposal, placeOf(places, m.proposal), reads[i])]));
  const pieces = usePieces(members);

  // The card or slip focus is in, by its hand-off, and the element holding
  // it. A focus lost to nowhere (the card going inert as it is answered, its
  // buttons disabled while "Always approve" saves, a slip giving way to a
  // group) keeps them, so focus can still be handed on.
  const rootRef = useRef<HTMLDivElement>(null);
  const focused = useRef<{ key: string; el: Element } | null>(null);
  const noteFocus = (e: React.FocusEvent) => {
    const el = e.target as Element;
    const key = handoffOf(el);
    focused.current = key ? { key, el } : null;
  };
  const noteBlur = (e: React.FocusEvent) => {
    if (e.relatedTarget instanceof Node && !e.currentTarget.contains(e.relatedTarget)) focused.current = null;
  };
  // In the commit that draws the change: what the card or slip became is in
  // the document, and what held focus, inert or gone, has not yet given it up
  // to the page. Focus in what stays live is the reader's to move.
  useLayoutEffect(() => {
    const at = focused.current;
    const root = rootRef.current;
    if (!at || !root) return;
    const member = members.find((m) => m.key === at.key);
    if (member && !answered(member)) return;
    if (member && at.el.isConnected && !at.el.closest('[inert]')) return;
    focused.current = null;
    if (member) focusAnswered(root, at.key);
  }, [members]);

  // A report that follows straight on is read next, so a link down to it
  // would only repeat it. Hand-offs drawn together link theirs all at once or
  // not at all: once each that will report has its report, if one of those
  // does not follow. One that will not (asked not to, never started) would
  // otherwise hold back the others' links for good.
  const viewsOf = (drawn: AnalystHandoffMember[]): HandoffView[] => {
    const reporting = drawn.filter((m) => m.proposal.report_back !== false
      && states.get(m.key)!.phase !== 'not_started'
      && followedRun(m.proposal, placeOf(places, m.proposal)) !== null);
    const linked = reporting.length > 0
      && reporting.every((m) => states.get(m.key)!.reportId)
      && reporting.some((m) => !placeOf(places, m.proposal).reportFollows);
    return drawn.map((m) => {
      const { proposal } = m;
      const state = states.get(m.key)!;
      const threadId = proposal.thread_id;
      const onOpen = threadId ? () => openDispatchedThread(threadId, proposal.workspace_id) : undefined;
      const reportId = linked ? state.reportId : undefined;
      // The report may not be drawn (a list still paging it in), or the host
      // may have no way to reach it; the thread holds it either way.
      const onSeeReport = reportId ? () => { if (!onRevealMessage?.(reportId)) onOpen?.(); } : undefined;
      return { ...state, id: m.key, onOpen, onSeeReport };
    });
  };

  // Every piece draws through ResolveSwap, so a card and the slip it becomes
  // are one box: the card asks while its hand-off is pending, and what stands
  // for it once answered is settled. A piece drawn answered has no card.
  const drawPiece = (piece: Piece): React.ReactElement => {
    if (piece.kind === 'handed') {
      return <ResolveSwap resolved settled={<HandoffStack views={viewsOf(piece.members)} />}>{null}</ResolveSwap>;
    }
    const { key, proposal, proposalId } = piece.member;
    const interruptId = proposal.interruptId ?? '';
    const pending = !answered(piece.member);
    return (
      <ResolveSwap
        resolved={!pending}
        settled={!pending && (
          <div data-handoff-member={key}>
            <HandoffDeclined proposal={proposal} />
          </div>
        )}
      >
        {pending && (
          <div data-handoff-card={key}>
            <HandoffApproval
              proposal={proposal}
              onApprove={onApprove ? (overrides) => onApprove(proposal, overrides, proposalId, interruptId) : undefined}
              onReject={onReject ? () => onReject(proposal, proposalId, interruptId) : undefined}
            />
          </div>
        )}
      </ResolveSwap>
    );
  };

  if (members.length === 0) return null;
  return (
    <div ref={rootRef} data-handoff-run className="sibling-space-y-3" onFocus={noteFocus} onBlur={noteBlur}>
      {/* Only a piece an answer takes away leaves, by folding out of the
          run. Whether a piece enters is ResolveSwap's to say: a card asking,
          and nothing drawn answered. */}
      <AnimatePresence>
        {pieces.map((piece) => <LeaveFold key={piece.key}>{drawPiece(piece)}</LeaveFold>)}
      </AnimatePresence>
    </div>
  );
}

export default AnalystHandoffRun;
