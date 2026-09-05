// Kept for src/pages/ChatAgent/components/__tests__/Markdown.blocks.test.tsx,
// which imports buildReply: tsc resolves this file for that import, and
// dropping it fails `pnpm typecheck` even though e2e/ is excluded.
export const END_MARKER: string;
export function buildReply(): string;
export function buildEvents(chunkChars?: number): Array<Record<string, unknown>>;
export function buildReasoning(): string;
export function chunk(text: string, size?: number): string[];
