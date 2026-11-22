import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** Where the thread's running work shows: chips above the composer, which fold
 *  into it while the reader is up in the history (`above`), or a strip inside
 *  the composer that stays put (`inside`). */
type ComposerTrayPlacement = 'above' | 'inside';

const COMPOSER_TRAY_KEY = 'composer_tray';

/** `other_preference.composer_tray`; anything but 'inside' reads as 'above'. */
export function readComposerTray(prefs: UserPreferences | null | undefined): ComposerTrayPlacement {
  return readOtherPreference(prefs, COMPOSER_TRAY_KEY) === 'inside' ? 'inside' : 'above';
}

export function composerTrayPatch(next: ComposerTrayPlacement): Partial<UserPreferences> {
  return otherPreferencePatch(COMPOSER_TRAY_KEY, next);
}
