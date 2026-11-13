import { Bot, HardDriveDownload, Shrink, Terminal } from 'lucide-react';
import type { ChipContext, FileMention, QuoteContext, SlashCommand } from './chat-input.types';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { TWEEN } from '@/lib/motion';

/** Return the appropriate icon for a slash command. */
export function getSlashCommandIcon(cmd: SlashCommand, className: string) {
  if (cmd.type === 'subagent') return <Bot className={className} />;
  if (cmd.name === 'offload') return <HardDriveDownload className={className} />;
  if (cmd.type === 'action') return <Shrink className={className} />;
  return <Terminal className={className} />;
}

/* --- MOTION ---
 * What the composer adds and takes away, as framer props on the motion
 * tokens. Opacity and transform only: nothing here tweens a height or a
 * width, so a row reflows once, when a chip arrives or after it has gone. */

/** A chip or pill joining the draft, and leaving it. */
export const CHIP_PRESENCE = {
  initial: { opacity: 0, scale: 0.95 },
  animate: { opacity: 1, scale: 1, transition: TWEEN.quick },
  exit: { opacity: 0, scale: 0.95, transition: TWEEN.exit },
};

/** The slash and @mention menus. They open above the composer, so this is
 *  `.pop-in[data-side='top']` (styles/animations.css) in framer, which keeps
 *  the list a menu last showed on screen through its exit. */
export const MENU_PRESENCE = {
  initial: { opacity: 0, y: 8, scale: 0.95 },
  animate: { opacity: 1, y: 0, scale: 1, transition: TWEEN.quick },
  exit: { opacity: 0, scale: 0.95, transition: TWEEN.exit },
};

/** One passage from one place: the same words on other lines are another quote. */
const quotedFrom = ({ from }: QuoteContext) => (from.kind === 'file' ? `@${from.path}:${from.lineStart ?? ''}-${from.lineEnd ?? ''}` : from.speaker);

/** Two mentions alike here are one context, whatever was said about it. */
/** The same words in two replies are two passages, as far as each is known. */
const sameMessage = ({ from: a }: QuoteContext, { from: b }: QuoteContext) => (
  a.kind !== 'message' || b.kind !== 'message' || !a.message || !b.message || a.message === b.message
);

export function sameMention(a: FileMention | QuoteContext | ChipContext, b: FileMention | QuoteContext | ChipContext): boolean {
  if (a.kind === 'file') return b.kind === 'file' && a.path === b.path;
  if (a.kind === 'quote') return b.kind === 'quote' && a.snippet === b.snippet && quotedFrom(a) === quotedFrom(b) && sameMessage(a, b);
  return b.kind === 'chip' && a.source === b.source && a.path === b.path && a.locator === b.locator && a.snippet === b.snippet;
}

/**
 * A stable key per pill. Removing a pill must not rename the ones after it,
 * or each would leave and arrive again; two pills alike in every field the
 * draft can tell apart are numbered in order.
 */
export function mentionPillKeys(chips: ChipContext[]): string[] {
  const seen = new Map<string, number>();
  return chips.map((f) => {
    const base = [f.path, f.label, f.locator, f.source, f.snippet.length].join('::');
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return n ? `${base}::${n}` : base;
  });
}

/* --- CONSTANTS --- */
export const MAX_FILES = 5;
export const PASTE_PILL_MIN_CHARS = 1500;
export const PASTE_PILL_MIN_LINES = 15;

/**
 * A paste this large would flood the draft — the composer stages it as a
 * removable pill instead and flattens it back into the message on send.
 */
export function isLargePaste(text: string): boolean {
  if (text.length >= PASTE_PILL_MIN_CHARS) return true;
  let lines = 1;
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '\n' && ++lines >= PASTE_PILL_MIN_LINES) return true;
  }
  return false;
}
export const BUILTIN_SLASH_COMMANDS = [
  { type: 'subagent', name: 'subagent' },
  { type: 'action', name: 'compact', aliases: ['compaction', 'summarize'] },
  { type: 'action', name: 'offload', aliases: ['truncate'] },
];

/**
 * Index of the `@`/`/` token the cursor is currently inside, or -1.
 *
 * Scans left from the cursor and stops at the first whitespace: the trigger
 * only counts at a word start (beginning of the draft or after whitespace), so
 * `foo@bar` and a URL's slashes never open a menu.
 */
export function findTriggerIndex(value: string, cursorPos: number, trigger: '@' | '/'): number {
  for (let i = cursorPos - 1; i >= 0; i--) {
    const ch = value[i];
    if (ch === trigger) {
      return i === 0 || /\s/.test(value[i - 1]) ? i : -1;
    }
    if (/\s/.test(ch)) return -1;
  }
  return -1;
}

/**
 * Slash-menu sort key (lower sorts first). Prefix matches (name/alias starts
 * with the query, char-by-char) rank above substring-only matches; within a
 * tier, system/service commands rank above skills. Combined: prefix+system 0,
 * prefix+skill 1, substring+system 2, substring+skill 3.
 */
export function slashRank(item: SlashCommand, query: string): number {
  const isPrefix =
    item.name.toLowerCase().startsWith(query) ||
    !!item.aliases?.some((a) => a.toLowerCase().startsWith(query));
  const isSkill = item.type === 'skill';
  return (isPrefix ? 0 : 2) + (isSkill ? 1 : 0);
}

/** Short display name for a model key: the manifest's authored name when the
 *  metadata carries one, else one derived from the key. */
export function getModelDisplayName(key: string | null, metadata?: Record<string, ModelMetadataEntry>): string {
  if (!key) return '';
  const authored = metadata?.[key]?.display_name;
  if (authored) return authored;
  // A subscription's twin of a model is the same model, so it takes its name.
  // A name the user gave a model of their own is theirs, suffix and all.
  let name = metadata?.[key]?.is_custom_model ? key : key.replace(/-oauth$/, '');
  // Strip common provider prefixes
  for (const prefix of ['claude-', 'gpt-', 'chatgpt-', 'o1-', 'o3-', 'o4-']) {
    if (name.startsWith(prefix)) { name = name.slice(prefix.length); break; }
  }
  // Convert version-like patterns: "opus-4-6" → "Opus 4.6", "sonnet-4-6" → "Sonnet 4.6"
  name = name
    .replace(/-(\d+)-(\d+)/, ' $1.$2')
    .replace(/-(\d+\.\d+)/, ' $1')
    .replace(/-/g, ' ')
    .replace(/\b\w/g, (c: string) => c.toUpperCase());
  return name;
}
