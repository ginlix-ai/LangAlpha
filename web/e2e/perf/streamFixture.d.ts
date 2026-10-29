// Kept for src/pages/ChatAgent/components/__tests__/Markdown.blocks.test.tsx,
// which imports buildReply: tsc resolves this file for that import, and
// dropping it fails `pnpm typecheck` even though e2e/ is excluded.
export const END_MARKER: string;
export function buildReply(sections?: number): string;
export function buildEvents(chunkChars?: number, opts?: { toolTabPause?: number; sections?: number }): Array<Record<string, unknown>>;
export const TAB_CALL: string;
export const TAB_CALL_OUTPUT: string;
export function buildCodeFile(lines?: number): string;
export function buildReasoning(): string;
export function chunk(text: string, size?: number): string[];
