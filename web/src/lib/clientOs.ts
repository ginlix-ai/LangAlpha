/**
 * The reader's operating system, as far as the browser says: enough to word
 * an offer for the machine they are on, never to gate what the server or the
 * desktop app enforces. An iPad asking for the desktop site says Macintosh,
 * so a touch screen is not read as a Mac.
 */
export type ClientOs = 'mac' | 'windows' | 'other';

export function clientOs(): ClientOs {
  if (typeof navigator === 'undefined') return 'other';
  const nav = navigator as Navigator & { userAgentData?: { platform?: string } };
  const platform = nav.userAgentData?.platform || nav.platform || nav.userAgent || '';
  if (/mac/i.test(platform)) return nav.maxTouchPoints > 1 ? 'other' : 'mac';
  if (/win/i.test(platform)) return 'windows';
  return 'other';
}
