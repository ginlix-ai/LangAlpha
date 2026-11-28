import type { TextAnchor } from '@/types/artifacts';
import type { SpotRect } from '../html/pageProtocol';
import { toFrame, wordsPinIn } from './pageAnchors';
import { wholeWords } from './spotKit';
import type { SpotStrategy } from './spotStrategy';

/**
 * Words selected in a viewer as a comment takes them: grown to whole words,
 * their anchor, their box in the scroll viewport with where their pin goes,
 * and the range marked while the comment is written. The pin is placed by
 * the rule the page reports it by, so it stays put once the page has.
 */
export function selectedWords(
  live: Range | null, strategy: SpotStrategy, scroller: Element,
): { anchor: TextAnchor; rect: SpotRect; range: Range } | null {
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
  const pin = wordsPinIn(scroller, range);
  return { anchor, rect: { ...toFrame(scroller, r), ...(pin ? { pin } : {}) }, range };
}
