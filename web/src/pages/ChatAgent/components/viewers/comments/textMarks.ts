/**
 * Words being commented on, marked in the app's accent through the CSS
 * Highlight API, as a page's bridge marks them in its frame: no element is
 * added around them, so the viewer's own DOM, and what observes it, stays as
 * it was. Each viewer page marks under a name of its own.
 */
import { animate } from '@/lib/framer';
import { DURATION, EASE_OUT } from '@/lib/motion';
import { prefersReducedMotion } from '@/lib/reducedMotion';
import { rangesOverlap } from './pageText';

let made = 0;

/** How strongly the accent fills marked words, in percent. */
const STRENGTH = 32;

const fill = (percent: number) => `color-mix(in srgb, var(--color-accent-primary) ${percent}%, transparent)`;

export function canMark(): boolean {
  return typeof CSS !== 'undefined' && !!CSS.highlights && typeof Highlight === 'function';
}

export class TextMarks {
  private readonly name = `viewer-comment-mark-${++made}`;
  /** Words whose mark is fading: a highlight of its own, so the words still marked keep their fill. */
  private readonly leaving = `${this.name}-leaving`;
  private style: HTMLStyleElement | null = null;
  /** The rule that fills fading words, whose fill the fade turns down. */
  private leavingRule: CSSStyleRule | null = null;
  private shown: Range[] = [];
  private fade: { stop: () => void } | null = null;

  /**
   * Marks exactly these ranges, or none. Words that lose their mark fade on
   * the exit beat, as the box written about them does, rather than going a
   * frame into its fade; a mark that only moves to the same words stays.
   */
  paint(ranges: Range[]): void {
    if (!canMark()) return;
    const gone = this.shown.filter((was) => !ranges.some((now) => rangesOverlap(was, now)));
    this.shown = ranges;
    if (gone.length) this.fadeOut(gone);
    if (!ranges.length) {
      CSS.highlights.delete(this.name);
      return;
    }
    this.rules();
    CSS.highlights.set(this.name, new Highlight(...ranges));
  }

  dispose(): void {
    this.fade?.stop();
    this.fade = null;
    if (canMark()) {
      CSS.highlights.delete(this.name);
      CSS.highlights.delete(this.leaving);
    }
    this.style?.remove();
    this.style = null;
    this.leavingRule = null;
    this.shown = [];
  }

  private rules(): CSSStyleRule | null {
    if (!this.style) {
      this.style = document.createElement('style');
      document.head.appendChild(this.style);
      const sheet = this.style.sheet;
      sheet?.insertRule(`::highlight(${this.name}){background-color:${fill(STRENGTH)}}`);
      const at = sheet?.insertRule(`::highlight(${this.leaving}){background-color:${fill(STRENGTH)}}`, sheet.cssRules.length);
      const rule = at === undefined ? null : sheet?.cssRules[at];
      this.leavingRule = rule instanceof CSSStyleRule ? rule : null;
    }
    return this.leavingRule;
  }

  private fadeOut(ranges: Range[]): void {
    this.fade?.stop();
    this.fade = null;
    const rule = this.rules();
    if (!rule || prefersReducedMotion()) {
      CSS.highlights.delete(this.leaving);
      return;
    }
    rule.style.backgroundColor = fill(STRENGTH);
    CSS.highlights.set(this.leaving, new Highlight(...ranges));
    const fade = animate(STRENGTH, 0, {
      duration: DURATION.exit,
      ease: EASE_OUT,
      onUpdate: (percent) => {
        rule.style.backgroundColor = fill(Math.max(0, percent));
      },
      onComplete: () => {
        if (this.fade !== fade) return;
        this.fade = null;
        CSS.highlights.delete(this.leaving);
      },
    });
    this.fade = fade;
  }
}
