export type BrowserPhase =
  | 'checking' | 'starting' | 'connecting' | 'live' | 'reconnecting'
  | 'idle' | 'low_memory' | 'stopped' | 'outdated' | 'no_chrome' | 'installing' | 'unavailable' | 'failed';

/** On the way to an answer: nothing to act on yet, and the view says it is working. */
export function isTransient(phase: BrowserPhase): boolean {
  return phase === 'checking' || phase === 'starting' || phase === 'connecting' || phase === 'reconnecting';
}

/** No browser on screen, and opening one may change that. Outdated and unavailable
 *  are not: no open brings a browser up there. */
export function isClosed(phase: BrowserPhase): boolean {
  return phase === 'idle' || phase === 'low_memory' || phase === 'stopped' || phase === 'failed';
}

/** A page with nothing on it, which a browser opened from the panel sits on until
 *  someone takes it somewhere: its frame is plain white, and reads as broken. */
export function isBlankPage(url: string | null): boolean {
  return url === null || url === '' || url.startsWith('about:blank');
}

/** Where the browser is when there is no page to show: open on a blank tab,
 *  not open, or on a computer that is stopped. */
export type BrowserStartState = 'open' | 'idle' | 'stopped';

/** The start page's state, or null where the tab has a page or a card to show. */
export function startStateOf(phase: BrowserPhase, pageUrl: string | null): BrowserStartState | null {
  if (phase === 'idle' || phase === 'stopped') return phase;
  return phase === 'live' && isBlankPage(pageUrl) ? 'open' : null;
}

/** Going somewhere from here is the person taking the browser, which also opens
 *  one that is closed on a computer it starts if it is stopped. */
export function canVisit(phase: BrowserPhase): boolean {
  return phase === 'live' || phase === 'idle' || phase === 'stopped';
}
