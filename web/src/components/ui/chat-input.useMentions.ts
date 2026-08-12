import { useCallback, useEffect, useMemo, useRef, useState, type Dispatch, type KeyboardEvent, type RefObject, type SetStateAction } from 'react';
import { findTriggerIndex } from './chat-input.helpers';
import type { MentionedFile } from './chat-input.types';

/**
 * The `@file` mention machine: trigger detection, filtering, selection, pill
 * bookkeeping and menu keyboard navigation. Rendering lives in
 * `MentionAutocompleteList`.
 */
export function useMentions({
  textareaRef,
  message,
  setMessage,
  workspaceFiles,
}: {
  textareaRef: RefObject<HTMLTextAreaElement | null>;
  message: string;
  setMessage: Dispatch<SetStateAction<string>>;
  workspaceFiles: string[];
}) {
  const [mentionedFiles, setMentionedFiles] = useState<MentionedFile[]>([]);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const [mentionStart, setMentionStart] = useState(-1);
  const listRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    if (!open) return [];
    const q = query.toLowerCase();
    const dirPriority: Record<string, number> = { '': 0, results: 1, data: 2 };
    return workspaceFiles
      .filter((f) => f.toLowerCase().includes(q))
      .sort((a, b) => {
        const da = a.includes('/') ? a.slice(0, a.indexOf('/')) : '';
        const db = b.includes('/') ? b.slice(0, b.indexOf('/')) : '';
        const pa = dirPriority[da] ?? 3;
        const pb = dirPriority[db] ?? 3;
        if (pa !== pb) return pa - pb;
        return a.localeCompare(b);
      })
      .slice(0, 10);
  }, [workspaceFiles, query, open]);

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
    setMentionStart(-1);
    setQuery('');
  }, []);

  /** Drop pills whose `@path` text no longer appears in the draft. */
  const pruneRemoved = useCallback((val: string) => {
    setMentionedFiles((prev) => prev.filter((f) => f.snippet || val.includes(`@${f.path}`)));
  }, []);

  /** Open/refresh the menu when the cursor sits inside an `@…` token. */
  const detectTrigger = useCallback((val: string, cursorPos: number) => {
    const atIdx = findTriggerIndex(val, cursorPos, '@');
    if (atIdx < 0) { close(); return false; }
    setMentionStart(atIdx);
    setQuery(val.slice(atIdx + 1, cursorPos));
    setOpen(true);
    return true;
  }, [close]);

  const selectFile = useCallback((filePath: string) => {
    if (mentionStart < 0) return;
    const cursorPos = textareaRef.current?.selectionStart ?? message.length;
    const before = message.slice(0, mentionStart);
    const after = message.slice(cursorPos);
    setMessage(before + '@' + filePath + ' ' + after);

    setMentionedFiles((prev) => {
      if (prev.some((f) => f.path === filePath)) return prev;
      return [...prev, { path: filePath }];
    });
    close();

    setTimeout(() => {
      if (textareaRef.current) {
        const newCursorPos = before.length + 1 + filePath.length + 1;
        textareaRef.current.focus();
        textareaRef.current.setSelectionRange(newCursorPos, newCursorPos);
      }
    }, 0);
  }, [mentionStart, message, setMessage, textareaRef, close]);

  const removeMention = useCallback((path: string, label?: string, snippet?: string) => {
    setMentionedFiles((prev) => prev.filter((f) => {
      if (snippet) return !(f.snippet === snippet && f.path === path);
      if (label) return !(f.path === path && f.label === label);
      return !(f.path === path && !f.label);
    }));
    // Also remove @path text from the textarea. Pathless pills (chat-selection
    // and paste snippets) have no token in the draft — an empty path would
    // match any standalone "@" and trim the message.
    if (path) {
      setMessage((prev) => prev.replace(new RegExp(`(^|\\s)@${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(\\s|$)`), '$1$2').trim());
    }
  }, [setMessage]);

  /** Returns true when the menu consumed the key. */
  const handleKeyDown = useCallback((e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (open && filtered.length > 0) {
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
        selectFile(filtered[activeIndex]);
        return true;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setOpen(false);
        return true;
      }
    }
    if (e.key === 'Escape' && open) {
      setOpen(false);
      return true;
    }
    return false;
  }, [open, filtered, activeIndex, selectFile]);

  const reset = useCallback(() => {
    setMentionedFiles([]);
    setOpen(false);
  }, []);

  return {
    mentionedFiles,
    setMentionedFiles,
    open,
    activeIndex,
    setActiveIndex,
    listRef,
    filtered,
    selectFile,
    removeMention,
    pruneRemoved,
    detectTrigger,
    handleKeyDown,
    close,
    reset,
  };
}

export type MentionsMachine = ReturnType<typeof useMentions>;
