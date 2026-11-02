import { useCallback, useEffect, useMemo, useRef, useState, type Dispatch, type KeyboardEvent, type RefObject, type SetStateAction } from 'react';
import { flushSync } from 'react-dom';
import { findTriggerIndex, sameMention } from './chat-input.helpers';
import type { ChipContext, FileMention, MentionedFile } from './chat-input.types';

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

  /** Drop pills whose `@path` text no longer appears in the draft. A quote stays,
   *  unread, for an undo that brings its token back. */
  const pruneRemoved = useCallback((val: string) => {
    setMentionedFiles((prev) => prev.filter((f) => f.kind !== 'file' || val.includes(`@${f.path}`)));
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
    // Committed before the caret moves: a timer waits behind keystrokes already
    // queued, and would drag the caret back into what they typed.
    flushSync(() => setMessage(before + '@' + filePath + ' ' + after));

    setMentionedFiles((prev) => {
      if (prev.some((f) => f.kind === 'file' && f.path === filePath)) return prev;
      return [...prev, { kind: 'file', path: filePath }];
    });
    close();

    const newCursorPos = before.length + 1 + filePath.length + 1;
    textareaRef.current?.focus();
    textareaRef.current?.setSelectionRange(newCursorPos, newCursorPos);
  }, [mentionStart, message, setMessage, textareaRef, close]);

  const addMention = useCallback((mention: FileMention | ChipContext) => {
    setMentionedFiles((prev) => (prev.some((f) => sameMention(f, mention)) ? prev : [...prev, mention]));
  }, []);

  const removeMention = useCallback((chip: ChipContext) => {
    setMentionedFiles((prev) => prev.filter((f) => f !== chip));
  }, []);

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
    addMention,
    removeMention,
    pruneRemoved,
    detectTrigger,
    handleKeyDown,
    close,
    reset,
  };
}

export type MentionsMachine = ReturnType<typeof useMentions>;
