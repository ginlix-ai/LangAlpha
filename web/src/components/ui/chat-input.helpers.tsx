import { Bot, HardDriveDownload, Shrink, Terminal } from 'lucide-react';
import type { SlashCommand } from './chat-input.types';

/** Return the appropriate icon for a slash command. */
export function getSlashCommandIcon(cmd: SlashCommand, className: string) {
  if (cmd.type === 'subagent') return <Bot className={className} />;
  if (cmd.name === 'offload') return <HardDriveDownload className={className} />;
  if (cmd.type === 'action') return <Shrink className={className} />;
  return <Terminal className={className} />;
}

/* --- CONSTANTS --- */
export const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
export const MAX_FILES = 5;
export const BUILTIN_SLASH_COMMANDS = [
  { type: 'subagent', name: 'subagent' },
  { type: 'action', name: 'compact', aliases: ['compaction', 'summarize'] },
  { type: 'action', name: 'offload', aliases: ['truncate'] },
];

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

/** Derive a short display name from a model key string. */
export function getModelDisplayName(key: string | null): string {
  if (!key) return '';
  let name = key;
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
