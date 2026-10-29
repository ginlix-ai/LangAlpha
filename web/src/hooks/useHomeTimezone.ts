import { currentTimezoneName, deviceTimezone, isKnownTimezone } from '@/lib/deviceTimezone';
import { useUser } from './useUser';

function homeTimezone(tz: string | null | undefined): string {
  return tz && isKnownTimezone(tz) ? currentTimezoneName(tz) : deviceTimezone();
}

/** The zone the user set in Settings, or this device's until they set one
 *  (or when this browser cannot format in the one they set). */
export function useHomeTimezone(): string {
  return homeTimezone(useUser().user?.timezone);
}

/** The same zone, from the profile only when the app already holds it: a
 *  shared thread, which a signed-out reader opens, reads this device's. */
export function useLoadedHomeTimezone(): string {
  return homeTimezone(useUser({ enabled: false }).user?.timezone);
}
