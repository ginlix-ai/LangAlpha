/**
 * Words being commented on, marked in the app's accent through the CSS
 * Highlight API, as a page's bridge marks them in its frame: no element is
 * added around them, so the viewer's own DOM, and what observes it, stays as
 * it was. Each viewer page marks under a name of its own.
 */
let made = 0;

export function canMark(): boolean {
  return typeof CSS !== 'undefined' && !!CSS.highlights && typeof Highlight === 'function';
}

export class TextMarks {
  private readonly name = `viewer-comment-mark-${++made}`;
  private style: HTMLStyleElement | null = null;

  /** Marks exactly these ranges, or none. */
  paint(ranges: Range[]): void {
    if (!canMark()) return;
    if (!ranges.length) {
      CSS.highlights.delete(this.name);
      return;
    }
    if (!this.style) {
      this.style = document.createElement('style');
      this.style.textContent = `::highlight(${this.name}){background-color:color-mix(in srgb, var(--color-accent-primary) 32%, transparent)}`;
      document.head.appendChild(this.style);
    }
    CSS.highlights.set(this.name, new Highlight(...ranges));
  }

  dispose(): void {
    if (canMark()) CSS.highlights.delete(this.name);
    this.style?.remove();
    this.style = null;
  }
}
