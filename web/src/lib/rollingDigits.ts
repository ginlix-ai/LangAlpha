/**
 * A ticking label whose changed digits roll: the old digit lifts out of the
 * line as the new one rises into it, and every character that did not change
 * holds still. Imperative, so a clock that writes its own text node once a
 * second (`WorkingLabel`) can roll without a React commit per tick.
 *
 * At rest the element holds plain text. The per-digit spans exist only while a
 * roll plays and are folded back into one text node when it ends, so a screen
 * reader, a copy, a test and a `background-clip: text` shimmer all meet the
 * same text they would without the roll.
 */
import { DURATION, EASE_OUT, cssEase } from './motion';
import { isPageUnseen } from './pageVisibility';
import { prefersReducedMotion } from './reducedMotion';

interface Shown {
  text: string;
  rolls: Animation[];
}

const shown = new WeakMap<HTMLElement, Shown>();

type Piece = { text: string } | { from?: string; to: string };

const ROLL = { duration: DURATION.quick * 1000, easing: cssEase(EASE_OUT) };
const ENTER: Keyframe[] = [{ transform: 'translateY(100%)', opacity: 0 }, { transform: 'none', opacity: 1 }];
const LEAVE: Keyframe[] = [{ transform: 'none', opacity: 1 }, { transform: 'translateY(-100%)', opacity: 0 }];

function runs(text: string): string[] {
  return text.split(/(\d+)/).filter(Boolean);
}

const isDigits = (run: string) => /^\d/.test(run);

/**
 * Which characters roll from `prev` to `next`, or null when the words around
 * the numbers changed (`59s` becoming `1m 0s`), which is a new label rather
 * than a tick and is written as one. A number that gains a digit lines up from
 * the right, so `9` to `10` rolls the units and brings the `1` in from below.
 */
function plan(prev: string, next: string): Piece[] | null {
  const a = runs(prev);
  const b = runs(next);
  if (a.length !== b.length) return null;
  const pieces: Piece[] = [];
  let text = '';
  for (let i = 0; i < b.length; i++) {
    if (!isDigits(b[i])) {
      if (a[i] !== b[i]) return null;
      text += b[i];
      continue;
    }
    if (!isDigits(a[i])) return null;
    const offset = a[i].length - b[i].length;
    for (let j = 0; j < b[i].length; j++) {
      const to = b[i][j];
      const from = a[i][j + offset];
      if (from === to) {
        text += to;
        continue;
      }
      if (text) pieces.push({ text });
      text = '';
      pieces.push({ from, to });
    }
  }
  if (text) pieces.push({ text });
  return pieces;
}

function canRoll(el: HTMLElement): boolean {
  return typeof el.animate === 'function' && el.isConnected && !prefersReducedMotion() && !isPageUnseen();
}

/**
 * One rolling digit (`.digit-roll` in styles/animations.css): the incoming
 * digit is the slot's text; the outgoing one is drawn by the `::after` of an
 * empty, `aria-hidden` span from its `data-from`. Generated content is not
 * text, so a copy or a `textContent` read mid roll gets the new label, and
 * `aria-hidden` keeps it out of the accessible name in every browser.
 */
function slot(doc: Document, { from, to }: { from?: string; to: string }, rolls: Animation[]): HTMLSpanElement {
  const box = doc.createElement('span');
  box.className = 'digit-roll';
  const incoming = doc.createElement('span');
  incoming.textContent = to;
  box.append(incoming);
  rolls.push(incoming.animate(ENTER, ROLL));
  if (from !== undefined) {
    const outgoing = doc.createElement('span');
    outgoing.setAttribute('aria-hidden', 'true');
    outgoing.dataset.from = from;
    box.append(outgoing);
    // Held at its end until the label folds back to text, so the old digit
    // never reappears over the new one in between.
    rolls.push(outgoing.animate(LEAVE, { ...ROLL, fill: 'forwards' }));
  }
  return box;
}

/** Shows `text` in `el`, rolling the digits that changed since the last write. */
export function writeRolling(el: HTMLElement, text: string): void {
  const prev = shown.get(el);
  if (prev?.text === text) return;
  for (const roll of prev?.rolls ?? []) roll.cancel();
  const state: Shown = { text, rolls: [] };
  shown.set(el, state);

  const pieces = prev && canRoll(el) ? plan(prev.text, text) : null;
  if (!pieces) {
    if (el.textContent !== text) el.textContent = text;
    return;
  }
  const doc = el.ownerDocument;
  el.replaceChildren(
    ...pieces.map((piece) => ('text' in piece ? doc.createTextNode(piece.text) : slot(doc, piece, state.rolls))),
  );
  Promise.all(state.rolls.map((roll) => roll.finished)).then(
    () => {
      if (shown.get(el) === state) el.textContent = text;
    },
    // A roll cancelled by the next write: that write owns the element now.
    () => {},
  );
}
