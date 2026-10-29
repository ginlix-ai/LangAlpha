import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** Where the transcript lands when a turn completes: stay at the end (the
 *  default), or bring the start of the final reply under the viewport top. */
export type TurnEndScroll = 'bottom' | 'reply_start';

const TURN_END_SCROLL_KEY = 'turn_end_scroll';

/** `other_preference.turn_end_scroll`; anything but 'reply_start' reads as 'bottom'. */
export function readTurnEndScroll(prefs: UserPreferences | null | undefined): TurnEndScroll {
  return readOtherPreference(prefs, TURN_END_SCROLL_KEY) === 'reply_start' ? 'reply_start' : 'bottom';
}

export function turnEndScrollPatch(next: TurnEndScroll): Partial<UserPreferences> {
  return otherPreferencePatch(TURN_END_SCROLL_KEY, next);
}
