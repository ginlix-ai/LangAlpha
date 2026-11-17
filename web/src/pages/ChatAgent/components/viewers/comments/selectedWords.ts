import type { TextAnchor } from '@/types/artifacts';
import type { FrameRect } from '../html/pageProtocol';
import { toFrame } from './pageAnchors';
import { wholeWords } from './spotKit';
import type { SpotStrategy } from './spotStrategy';

function fraction(n: number): number {
  return Math.round(Math.min(1, Math.max(0, n)) * 1000) / 1000;
}

/**
 * Words selected in a viewer as a comment takes them: grown to whole words,
 * their anchor, their box in the scroll viewport, and the range marked while
 * the comment is written. `release`, where the pointer let go, keeps where on
 * the words the pin stands, as a click's point does on an element.
 */
export function selectedWords(
  live: Range | null, release: { clientX: number; clientY: number } | null, strategy: SpotStrategy, scroller: Element,
): { anchor: TextAnchor; rect: FrameRect; range: Range } | null {
  if (!live || live.collapsed) return null;
  const whole = wholeWords(live);
  const range = strategy.clip?.(whole) ?? whole;
  let anchor: TextAnchor | null = null;
  try {
    anchor = strategy.words?.(range) ?? null;
  } catch {
    // Words the strategy cannot read take no comment.
  }
  const r = range.getBoundingClientRect();
  if (!anchor || (!r.width && !r.height)) return null;
  if (release && r.width > 0 && r.height > 0) {
    anchor.point = { x: fraction((release.clientX - r.left) / r.width), y: fraction((release.clientY - r.top) / r.height) };
  }
  return { anchor, rect: toFrame(scroller, r), range };
}
