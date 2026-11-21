export type BrowserPhase =
  | 'checking' | 'starting' | 'connecting' | 'live' | 'reconnecting'
  | 'idle' | 'low_memory' | 'stopped' | 'outdated' | 'unavailable' | 'failed';

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
