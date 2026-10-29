import type React from 'react';

/** Which drawing of the files toggle a press hands focus to. */
export type PanelToggleSlot = 'header' | 'panel';

/**
 * On desktop the chat header's files toggle and the open panel's corner
 * toggle are one control drawn in two places, and each unmounts as it is
 * pressed, so a keyboard press would drop focus to the page. This moves focus
 * to the other once it renders: the panel is lazy, so on a first open that
 * can take a moment. A pointer press (detail > 0) leaves focus alone. Only a
 * rendered toggle counts: ChatAgent keeps other threads' views mounted under
 * display:none, and focusing one of their toggles does nothing.
 */
export function handOffToggleFocus(event: React.MouseEvent, to: PanelToggleSlot): void {
  if (event.detail === 0) focusToggleWhenRendered(to);
}

/**
 * The hand-off itself, for a press whose effect lands later than its click.
 * While the panel's code loads, focus waits on the loading strip's stand-in,
 * however long that takes, and goes on to the real toggle when it arrives,
 * unless the user has moved it somewhere else meanwhile.
 */
export function focusToggleWhenRendered(to: PanelToggleSlot): void {
  let deadline = performance.now() + 2000;
  let standIn: HTMLElement | null = null;
  const attempt = () => {
    const rendered = Array.from(document.querySelectorAll<HTMLElement>(`[data-panel-toggle="${to}"]`)).filter(
      (candidate) => candidate.getClientRects().length > 0,
    );
    const el = rendered.find((candidate) => !candidate.hasAttribute('data-panel-toggle-stand-in'));
    if (el) {
      const active = document.activeElement;
      if (!standIn || active === standIn || active === document.body || active === null) el.focus();
      return;
    }
    if (rendered.length) {
      if (!standIn) {
        standIn = rendered[0];
        standIn.focus();
      }
      deadline = performance.now() + 2000;
    }
    if (performance.now() < deadline) requestAnimationFrame(attempt);
  };
  requestAnimationFrame(attempt);
}
