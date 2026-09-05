import { useCallback, useEffect, useMemo, useRef, useState, type Dispatch, type KeyboardEvent, type RefObject, type SetStateAction } from 'react';
import { useTranslation } from 'react-i18next';
import { useSkills } from '@/hooks/useSkills';
import { BUILTIN_SLASH_COMMANDS, findTriggerIndex, slashRank } from './chat-input.helpers';
import type { SlashCommand } from './chat-input.types';

/**
 * The `/command` machine: skill fetching, trigger detection, ranking,
 * selection, pill bookkeeping and menu keyboard navigation. Rendering lives in
 * `SlashCommandList`.
 */
export function useSlashCommands({
  textareaRef,
  message,
  setMessage,
  mode,
  workspaceId,
}: {
  textareaRef: RefObject<HTMLTextAreaElement | null>;
  message: string;
  setMessage: Dispatch<SetStateAction<string>>;
  mode?: 'fast' | 'ptc';
  workspaceId?: string | null;
}) {
  const { t } = useTranslation();
  const [slashCommands, setSlashCommands] = useState<SlashCommand[]>([]);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const [slashStart, setSlashStart] = useState(-1);
  const listRef = useRef<HTMLDivElement>(null);

  // Enabled-only skills for the agent mode, via React Query so a skill
  // uploaded or toggled on the Plugins page reaches this menu without a
  // reload. Inside a workspace this is the workspace-effective view, so
  // workspace-scoped skills surface here too.
  const skillsMode = mode === 'fast' ? 'flash' : 'ptc';
  const { data: skills = [] } = useSkills(skillsMode, { workspaceId });

  const filtered = useMemo(() => {
    if (!open) return [];
    const q = query.toLowerCase();
    const items: SlashCommand[] = [
      ...skills.filter((s) => s.command).map((s) => ({ type: 'skill', name: s.command!, skillName: s.name, description: t(`chat.slashCommand.${s.command}Desc`, { defaultValue: s.description }) })),
      ...BUILTIN_SLASH_COMMANDS.map((c) => ({ ...c, description: t(`chat.slashCommand.${c.name}Desc`) })),
    ];
    return items
      .filter((item) => !slashCommands.some((c) => c.name === item.name))
      // Fuzzy-match on command name + aliases only — never the description.
      // Matching the description surfaces irrelevant commands (e.g. /subagent
      // for "comp" because its blurb says "...complete this task"); skills and
      // builtins follow the same name-based rule.
      .filter((item) => {
        if (!q) return true;
        if (item.name.toLowerCase().includes(q)) return true;
        if (item.aliases?.some((a: string) => a.toLowerCase().includes(q))) return true;
        return false;
      })
      // Rank: prefix (char-by-char from the start) matches before substring-only
      // matches; within a tier, system/service commands (/compact, /offload,
      // /subagent) outrank skills. So `/d` puts /dcf-model (prefix) above
      // /offload (a system command matching "d" only mid-word), while `/comp`
      // puts /compact above /comps-analysis (both prefix → system wins). Combined
      // score (lower = earlier): prefix+system 0, prefix+skill 1, substr+system
      // 2, substr+skill 3. Array.sort is stable, preserving within-tier order;
      // sort before slice so a high-rank match never falls off the top-10.
      .sort((a, b) => slashRank(a, q) - slashRank(b, q))
      .slice(0, 10);
  }, [skills, open, query, slashCommands, t]);

  useEffect(() => {
    setActiveIndex(0);
  }, [filtered.length]);

  // Scroll the active item into view
  useEffect(() => {
    if (!open || !listRef.current) return;
    const items = listRef.current.querySelectorAll('.mention-autocomplete-item');
    items[activeIndex]?.scrollIntoView({ block: 'nearest' });
  }, [activeIndex, open]);

  const close = useCallback(() => {
    setOpen(false);
    setSlashStart(-1);
    setQuery('');
  }, []);

  /** Drop pills whose `/command` text no longer appears in the draft. */
  const pruneRemoved = useCallback((val: string) => {
    setSlashCommands((prev) => prev.filter((cmd) => val.includes(`/${cmd.name}`)));
  }, []);

  /** Open/refresh the menu when the cursor sits inside a `/…` token. */
  const detectTrigger = useCallback((val: string, cursorPos: number) => {
    const slashIdx = findTriggerIndex(val, cursorPos, '/');
    if (slashIdx < 0) { close(); return false; }
    setSlashStart(slashIdx);
    setQuery(val.slice(slashIdx + 1, cursorPos));
    setOpen(true);
    return true;
  }, [close]);

  const selectCommand = useCallback((cmd: SlashCommand) => {
    if (slashStart < 0) return;
    const cursorPos = textareaRef.current?.selectionStart ?? message.length;
    const before = message.slice(0, slashStart);
    const after = message.slice(cursorPos);

    // All commands — including system actions like /compact — insert a pill and
    // defer execution to send. Nothing fires on mere selection; handleSend
    // dispatches action pills via onAction when the user actually sends.
    const inserted = `/${cmd.name} `;
    setMessage(before + inserted + after);
    close();
    setSlashCommands((prev) => {
      if (prev.some((c) => c.name === cmd.name)) return prev;
      return [...prev, cmd];
    });
    const newCursor = before.length + inserted.length;
    setTimeout(() => {
      if (textareaRef.current) {
        textareaRef.current.focus();
        textareaRef.current.setSelectionRange(newCursor, newCursor);
      }
    }, 0);
  }, [slashStart, message, setMessage, textareaRef, close]);

  const removeCommand = useCallback((name: string) => {
    setSlashCommands((prev) => prev.filter((c) => c.name !== name));
    // Also remove /{command} text from the textarea
    setMessage((prev) => prev.replace(new RegExp(`(^|\\s)/${name}(\\s|$)`), '$1$2').trim());
  }, [setMessage]);

  /** Returns true when the menu consumed the key. */
  const handleKeyDown = useCallback((e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (!open || filtered.length === 0) return false;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveIndex((prev) => (prev + 1) % filtered.length);
      return true;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveIndex((prev) => (prev - 1 + filtered.length) % filtered.length);
      return true;
    }
    if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      selectCommand(filtered[activeIndex]);
      return true;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      setOpen(false);
      return true;
    }
    return false;
  }, [open, filtered, activeIndex, selectCommand]);

  const reset = useCallback(() => {
    setSlashCommands([]);
    setOpen(false);
  }, []);

  return {
    slashCommands,
    open,
    activeIndex,
    setActiveIndex,
    listRef,
    filtered,
    selectCommand,
    removeCommand,
    pruneRemoved,
    detectTrigger,
    handleKeyDown,
    close,
    reset,
  };
}

export type SlashCommandsMachine = ReturnType<typeof useSlashCommands>;
