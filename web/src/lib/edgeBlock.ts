/**
 * A request refused by something in front of the server: a CDN firewall or a
 * proxy, which answers with an HTML page. The backend answers every refusal in
 * JSON, so a 403 page is never its own, and nothing about it reaches the
 * server's logs. Reported here, it is seen the day it starts rather than
 * weeks later, when someone finally asks why their file will not attach.
 */
import { reportEdgeBlock } from './sentry';

/**
 * `content`: a rule matched the request, most likely its body. `challenge`:
 * the edge wants the browser to prove itself (`cf-mitigated: challenge`),
 * which a reload answers and editing the request does not.
 */
export type EdgeBlockKind = 'content' | 'challenge';

const MESSAGES: Record<EdgeBlockKind, string> = {
  content: 'A firewall in front of the server blocked this request, most likely because of what it contains.',
  challenge: 'A firewall in front of the server stopped this request until the page is reloaded. Reload it and try again.',
};

/** What a surface with no words of its own shows for one. */
export function edgeBlockMessage(kind: EdgeBlockKind): string {
  return MESSAGES[kind];
}

/** Which edge block an answer is, or null for anything the server itself said. */
export function edgeBlockKind(status: number, header: (name: string) => string | null | undefined): EdgeBlockKind | null {
  if (status !== 403 || !/\btext\/html\b/i.test(header('content-type') ?? '')) return null;
  return /\bchallenge\b/i.test(header('cf-mitigated') ?? '') ? 'challenge' : 'content';
}

/**
 * The route a URL names, cut to its collection: `/api/v1/attachments/:id/:id`.
 * Every segment past it is folded, since a path carries ids, capabilities and
 * the user's own names for things, and no pattern tells a name from a word
 * the route itself spells. The edge's ray id finds the exact path where the
 * edge logged it.
 */
export function edgeBlockRoute(url: string): string {
  let path: string;
  try {
    path = new URL(url, 'http://route.invalid').pathname;
  } catch {
    path = url.split(/[?#]/, 1)[0];
  }
  const segments = path.split('/').filter(Boolean);
  // The prefix every route shares (`api`, a version), then the collection.
  let kept = 0;
  while (kept < segments.length && (segments[kept] === 'api' || /^v\d+$/.test(segments[kept]))) kept += 1;
  kept = Math.min(kept + 1, segments.length);
  return '/' + segments.map((segment, i) => (i < kept ? segment : ':id')).join('/');
}

/** Reports a blocked request, with the edge's id for it when the edge gave one. */
export function noteEdgeBlock(kind: EdgeBlockKind, method: string, url: string, rayId: string | null | undefined): void {
  reportEdgeBlock(kind, method.toUpperCase(), edgeBlockRoute(url), rayId || null);
}
