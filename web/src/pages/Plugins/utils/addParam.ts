/**
 * The page-header Add menu's intent, carried in the URL (`?tab=mcp&add=server`)
 * rather than in page state.
 *
 * The tab bodies are conditionally rendered, so switching tabs unmounts the
 * list that was asked to open a modal; a signal held in page state replayed on
 * every remount, re-opening a modal the user had already closed. As a URL
 * param the intent is stripped by the list that acts on it, which makes it
 * naturally single-shot, and it survives a reload the way a deep link should.
 */

export type AddIntent = 'plugin' | 'server' | 'import' | 'skill';

export const ADD_PARAM = 'add';

const INTENTS: ReadonlySet<string> = new Set<AddIntent>([
  'plugin',
  'server',
  'import',
  'skill',
]);

/** The tab whose list owns each intent's modal. */
export const ADD_INTENT_TAB: Record<AddIntent, string> = {
  plugin: 'plugins',
  server: 'mcp',
  import: 'mcp',
  skill: 'skills',
};

export function parseAddIntent(params: URLSearchParams): AddIntent | null {
  const raw = params.get(ADD_PARAM);
  return raw && INTENTS.has(raw) ? (raw as AddIntent) : null;
}
