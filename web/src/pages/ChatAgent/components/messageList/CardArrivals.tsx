import React from 'react';
import { animate } from '@/lib/framer';
import { followHeight } from '@/lib/followHeight';
import { cssEase, DURATION, EASE_OUT, STAGGER, TWEEN } from '@/lib/motion';
import { isPageUnseen } from '@/lib/pageVisibility';
import { RISE_IN_PX, RISE_OUT_PX } from './LiveRow';
import { CardCallsContext } from './cardCalls';
import { pickSource, splitGrowth } from './cardArrivalPlan';
import { isFreshArrival, playEntrance } from './transcriptMotion';

/** A card the message draws for one tool call, whose box carries
 *  `data-card-arrival` with the key. */
export interface ArrivingCard {
  key: string;
  /** The tool call it draws. */
  call: string;
  /** When the call's result reached this page; absent from history and from
   *  a reconnect's replay. */
  settledAt?: number;
}

interface CardArrivalsProps {
  /** Every card the message draws, in transcript order. */
  cards: readonly ArrivingCard[];
  /** The turn is running, so a card that appears is landing in front of the
   *  reader rather than being drawn again. */
  live: boolean;
  motionSafe: boolean;
  className?: string;
  children: React.ReactNode;
}

/** A box as it was drawn, in px from the top of the list. */
interface Place {
  top: number;
  height: number;
}

/** A box the reader saw before the commit, and what it looked like. */
interface Drawn extends Place {
  node: HTMLElement;
  /** What the reader saw there, drawn in a card's box while it fades. */
  look: HTMLElement;
}

interface Landing {
  key: string;
  /** Each row naming the call, and the activity block holding it. */
  rows: Array<{ row: Drawn; block: Drawn | null }>;
}

interface Snapshot {
  height: number;
  landings: Landing[];
}

/** A landing card's box as the commit drew it. */
interface Plan extends Place {
  box: HTMLElement;
  /** The card, or null for one whose artifact had nothing to draw. */
  content: HTMLElement | null;
  /** The place the card takes over. */
  source: Drawn | null;
  /** Two calls landing out of one block share its space, but only the first
   *  draws it fading. */
  drawsLook: boolean;
  /** The list's own child holding the box, whose top margin is its gap. */
  listChild: HTMLElement | null;
  gap: number;
}

/** The card comes in once the row has gone, a beat after it: what arrives
 *  follows what leaves, as at a turn's end (TURN_END_DELAY), on the card's own
 *  clock. */
const ENTER_DELAY = DURATION.exit + STAGGER;
/** Closer than this, the box has caught up with its card, and a clip would
 *  only shave the card's last row of pixels. */
const CAUGHT_UP_PX = 0.5;

const placeIn = (origin: DOMRect, el: HTMLElement): Place => {
  const rect = el.getBoundingClientRect();
  return { top: rect.top - origin.top, height: rect.height };
};

/** The row with its block's boxes around it, emptied of everything else, so
 *  the block's own rules (`.live-zone [data-status-line] .titem`) still draw
 *  it once it stands on its own. */
function copyRow(row: HTMLElement, block: HTMLElement | null): HTMLElement {
  let look = row.cloneNode(true) as HTMLElement;
  for (let el = row.parentElement; block && el && el !== block.parentElement; el = el.parentElement) {
    const shell = el.cloneNode(false) as HTMLElement;
    shell.append(look);
    look = shell;
  }
  return look;
}

/** A look the reader sees but nothing else does: no ids, test ids or hand-over
 *  marks doubled in the document, no focus, no pointer, no screen reader. */
function asGhost(look: HTMLElement, top: number): HTMLElement {
  for (const el of [look, ...look.querySelectorAll<HTMLElement>('*')]) {
    for (const name of ['id', 'data-testid', 'data-call-row', 'data-handed-over', 'data-activity-block']) el.removeAttribute(name);
  }
  look.setAttribute('aria-hidden', 'true');
  look.inert = true;
  Object.assign(look.style, { position: 'absolute', top: `${top}px`, left: '0', right: '0', margin: '0', pointerEvents: 'none' });
  return look;
}

/** The list's own child that holds `el`: the block a card's gap belongs to. */
function listChildOf(el: HTMLElement, list: HTMLElement): HTMLElement | null {
  let child: HTMLElement | null = el;
  while (child && child.parentElement !== list) child = child.parentElement;
  return child;
}

/**
 * The message's blocks, and a data card growing out of the row that ran its
 * call. In a live turn a call that lands with a card leaves the activity
 * block it ran in, and its card takes the row's place: the card's box starts
 * at the space the row gave up and grows to the card's height while the row
 * fades and lifts where it was, and the card fades and rises in once the row
 * has gone. One space, so nothing under it moves but the growth.
 *
 * The row is read before the commit that lands the card touches the DOM, the
 * one moment it is still drawn as the reader saw it whichever way it goes: a
 * row whose block stays gives its height up at once and says so
 * (`data-handed-over`, LiveRow's hand-over exit), and one that was the block's
 * only item goes with the block in the same commit, with no exit at all. That
 * is the reason for a class here: `getSnapshotBeforeUpdate` is the only read
 * React offers ahead of a commit's DOM writes, and a block that unmounts
 * leaves nothing to measure after them.
 *
 * The box starts at whatever height leaves the list as tall as it was drawn,
 * so the gaps a hand-over opens or closes (a card beside a block that stays,
 * or joining the card run above it) are taken up by the same growth.
 *
 * Only a card landing now moves. One drawn with the message (a load, a
 * replay, a reconnect, a thread switched back to, a turn opened again) is not
 * new to the list, or its call did not settle a moment ago.
 */
export class CardArrivals extends React.Component<CardArrivalsProps> {
  private list = React.createRef<HTMLDivElement>();
  private running = new Set<() => void>();
  /** The calls whose cards arrive in their rows' place, kept while they are
   *  the same calls: every live row reads them, and a new set would draw each
   *  row again. */
  private calls: ReadonlySet<string> = new Set();

  override getSnapshotBeforeUpdate(prev: CardArrivalsProps): Snapshot | null {
    const list = this.list.current;
    if (!list) return null;
    const drawn = new Set(prev.cards.map((card) => card.key));
    // The calls this render told their rows to hand over: a card that lands
    // with nothing to hand it its row would leave the row with nowhere to go.
    const landing = this.props.cards.filter((card) => !drawn.has(card.key) && this.calls.has(card.call));
    if (landing.length === 0) return null;
    const origin = list.getBoundingClientRect();
    const rows = Array.from(list.querySelectorAll<HTMLElement>('[data-call-row]'));
    return {
      height: origin.height,
      landings: landing.map(({ key, call }) => ({
        key,
        rows: rows.filter((el) => el.dataset.callRow?.split(' ').includes(call)).map((row) => {
          const block = row.closest<HTMLElement>('[data-activity-block]');
          return {
            row: { ...placeIn(origin, row), node: row, look: copyRow(row, block) },
            // The block itself: it is only ever shown once React has let it go.
            block: block && { ...placeIn(origin, block), node: block, look: block },
          };
        }),
      })),
    };
  }

  override componentDidUpdate(_prev: CardArrivalsProps, _state: unknown, snapshot: Snapshot | null): void {
    const list = this.list.current;
    if (!snapshot || !list) return;
    // Every read before the first write, so the commit is laid out once.
    const origin = list.getBoundingClientRect();
    const boxes = Array.from(list.querySelectorAll<HTMLElement>('[data-card-arrival]'));
    const drawnLooks = new Set<HTMLElement>();
    const plans: Plan[] = [];
    for (const { key, rows } of snapshot.landings) {
      const box = boxes.find((el) => el.dataset.cardArrival === key);
      if (!box) continue;
      const content = box.firstElementChild instanceof HTMLElement ? box.firstElementChild : null;
      const source = pickSource(rows.map(({ row, block }) => ({
        row,
        goneBlock: block && !block.node.isConnected ? block : null,
        handedOver: row.node.isConnected && row.node.hasAttribute('data-handed-over'),
      })));
      // A card with nothing to draw has nothing to grow to, but its row gave
      // its place up all the same, and the box closes it.
      if (!content && !source) continue;
      const drawsLook = !!source && !drawnLooks.has(source.look);
      if (source) drawnLooks.add(source.look);
      const listChild = listChildOf(box, list);
      plans.push({
        box,
        content,
        source,
        drawsLook,
        ...placeIn(origin, box),
        listChild,
        gap: listChild ? parseFloat(getComputedStyle(listChild).marginTop) || 0 : 0,
      });
    }
    if (plans.length === 0) return;
    // What the commit added to the list is what the cards start short of, so
    // the list keeps the height it was drawn at.
    const starts = splitGrowth(plans, origin.height - snapshot.height);
    let shift = 0;
    plans.forEach((plan, i) => {
      const ghost = plan.source && plan.drawsLook
        ? { look: plan.source.look, top: plan.source.top - plan.top - shift }
        : null;
      this.arrive(plan, ghost, starts[i]);
      // A card higher up starting shorter moves the ones under it.
      shift += starts[i] - plan.height;
    });
  }

  override componentWillUnmount(): void {
    for (const stop of this.running) stop();
    this.running.clear();
  }

  /** `ghost` is what the card takes the place of, `top` px from the box's top. */
  private arrive({ box, content, listChild, gap }: Plan, ghost: { look: HTMLElement; top: number } | null, start: number): void {
    const from = Math.max(0, start);
    // Below zero, the gap the card's block opens is more than the commit took
    // away: the block starts with that much less of it and gets it back as
    // the card opens. The block's own margin, since a fold around the card
    // would contain one of the box's.
    const margin = listChild && start < 0 ? { el: listChild, gap, by: start } : null;
    box.dataset.arriving = '';
    box.style.position = 'relative';
    box.style.height = `${from}px`;
    if (margin) margin.el.style.marginTop = `${margin.gap + margin.by}px`;

    const fading = ghost && asGhost(ghost.look, ghost.top);
    if (fading) {
      box.append(fading);
      const leave = typeof fading.animate === 'function'
        ? fading.animate(
          [{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translateY(${-RISE_OUT_PX}px)` }],
          { duration: DURATION.exit * 1000, easing: cssEase(EASE_OUT), fill: 'forwards' },
        )
        : null;
      const drop = () => fading.remove();
      if (leave) leave.finished.then(drop, drop);
      else drop();
    }
    // The card rises in over the same quick beat it fades in on.
    const stopEntrance = content ? playEntrance(content, RISE_IN_PX, ENTER_DELAY, DURATION.quick) : () => {};

    let stopFollow = () => {};
    const land = () => {
      this.running.delete(stop);
      delete box.dataset.arriving;
      box.style.position = '';
      box.style.height = '';
      if (margin) margin.el.style.marginTop = '';
      if (content) content.style.clipPath = '';
    };
    const stop = () => {
      stopFollow();
      stopEntrance();
      fading?.remove();
      land();
    };
    this.running.add(stop);
    if (!content) {
      // Nothing came: the space closes as the row's own exit would have.
      const close = animate(from, 0, {
        ...TWEEN.fold,
        onUpdate: (height) => { box.style.height = `${height}px`; },
        onComplete: land,
      });
      stopFollow = () => close.stop();
      return;
    }
    stopFollow = followHeight(box, content, {
      from,
      leg: () => TWEEN.fold,
      onFrame: (height, target) => {
        if (margin) {
          const opened = target > from ? (height - from) / (target - from) : 1;
          margin.el.style.marginTop = `${margin.gap + margin.by * Math.max(0, 1 - opened)}px`;
        }
        // A card that grows while its box is still catching up (a chart
        // drawing in) is clipped at the box, not drawn over what is below.
        // Only below: a card joining the one above draws the joint over it.
        content.style.clipPath = target - height > CAUGHT_UP_PX ? `inset(-50% 0 ${target - height}px)` : '';
      },
      onLand: land,
    });
  }

  override render(): React.ReactNode {
    // A row hands its place over only to a card that will arrive in it: any
    // other leaves as rows leave. Decided here, where the rows read it, and
    // the snapshot of this commit lands exactly these.
    const { cards, live, motionSafe } = this.props;
    const calls = new Set(live && motionSafe && !isPageUnseen()
      ? cards.filter((card) => isFreshArrival([card.settledAt])).map((card) => card.call)
      : []);
    if (calls.size !== this.calls.size || [...calls].some((call) => !this.calls.has(call))) this.calls = calls;
    return (
      <CardCallsContext value={this.calls}>
        <div ref={this.list} className={this.props.className}>{this.props.children}</div>
      </CardCallsContext>
    );
  }
}
