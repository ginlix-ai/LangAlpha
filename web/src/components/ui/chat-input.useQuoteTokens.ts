import { useCallback, useLayoutEffect, useMemo, useRef, type Dispatch, type KeyboardEvent, type RefObject, type SetStateAction, type SyntheticEvent } from 'react';
import { flushSync } from 'react-dom';
import { focusWithoutRing } from '@/lib/inputModality';
import { sameMention } from './chat-input.helpers';
import { escapeToken, heldInDraft, keepTokensWhole, quoteToken, tokenAround, tokensOf } from './chat-input.quoteTokens';
import type { QuoteContext } from './chat-input.types';
import type { MentionsMachine } from './chat-input.useMentions';

/**
 * What keeps a token whole in a field: the caret and the ends of a selection step
 * across it, and a delete takes all of it. `wordTokens` are a `/skill` or `@file`,
 * which go whole too but only count as words of their own.
 */
export function useWholeTokens({ textareaRef, text, setText, quoteTokens, wordTokens = [] }: {
  textareaRef: RefObject<HTMLTextAreaElement | null>;
  text: string;
  setText: (next: string) => void;
  quoteTokens: string[];
  wordTokens?: string[];
}) {
  const takeOut = (start: number, end: number) => {
    const next = text.slice(0, start) + text.slice(end);
    flushSync(() => setText(next));
    textareaRef.current?.setSelectionRange(start, start);
  };

  // Neither end of a selection, nor a caret, rests inside a quote's token, where a
  // keystroke would break it and lose the quote: it steps across, on the way it
  // was already going, so a selection takes a token whole or not at all.
  const lastSelectionRef = useRef({ start: 0, end: 0 });
  const handleSelect = (e: SyntheticEvent<HTMLTextAreaElement>) => {
    const textarea = e.currentTarget;
    const { selectionStart, selectionEnd, selectionDirection, value } = textarea;
    const stepOut = (at: number, was: number) => {
      const inside = tokenAround(value, at, quoteTokens);
      if (!inside) return at;
      return was >= inside.end ? inside.start : inside.end;
    };
    const last = lastSelectionRef.current;
    const end = stepOut(selectionEnd, last.end);
    const start = selectionStart === selectionEnd ? end : stepOut(selectionStart, last.start);
    if (start !== selectionStart || end !== selectionEnd) textarea.setSelectionRange(start, end, selectionDirection ?? undefined);
    lastSelectionRef.current = { start, end };
  };

  /** For an edit that reached the field whatever it did: `next`, or what keeps a token it cut into whole. */
  const pendingCaretRef = useRef<number | null>(null);
  const keepWhole = (next: string, caret: number) => {
    const kept = keepTokensWhole(text, next, quoteTokens, caret);
    if (!kept) return next;
    pendingCaretRef.current = kept.caret;
    return kept.text;
  };
  useLayoutEffect(() => {
    if (pendingCaretRef.current === null) return;
    textareaRef.current?.setSelectionRange(pendingCaretRef.current, pendingCaretRef.current);
    pendingCaretRef.current = null;
  }, [text, textareaRef]);

  /** `end` is where the pressed token ends: the same one can be in the draft twice. */
  const handleQuotePress = (end: number) => {
    focusWithoutRing(textareaRef.current);
    textareaRef.current?.setSelectionRange(end, end);
  };

  /** Returns true when a whole token took the key. */
  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    const caret = e.currentTarget.selectionStart;
    if (caret !== e.currentTarget.selectionEnd) return false;

    if (e.key === 'Backspace') {
      // A token goes whole: half a skill's name is no skill, only stray text.
      const before = text.slice(0, caret);
      const token = quoteTokens.find((tok) => before.endsWith(tok) || before.endsWith(`${tok} `))
        ?? wordTokens.find((tok) => new RegExp(`(^|\\s)${escapeToken(tok)} ?$`).test(before));
      if (token) {
        e.preventDefault();
        takeOut(before.lastIndexOf(token), caret);
        return true;
      }
    }

    if (e.key === 'Delete') {
      const token = quoteTokens.find((tok) => text.startsWith(tok, caret));
      if (token) {
        e.preventDefault();
        takeOut(caret, caret + token.length);
        return true;
      }
    }
    return false;
  };

  return { handleSelect, handleQuotePress, handleKeyDown, keepWhole };
}

/**
 * The whole words of the draft. A quote lives exactly as long as its token is
 * there, which is why nothing here removes one from `mentionedFiles`: an undo that
 * brings the token back brings the quote back, and only a new quote clears those
 * left behind. Rendering lives in `DraftTokenLayer`.
 */
export function useQuoteTokens({
  textareaRef,
  message,
  setMessage,
  mentions: { mentionedFiles, setMentionedFiles, removeMention },
  draftTokens,
  pruneDraft,
}: {
  textareaRef: RefObject<HTMLTextAreaElement | null>;
  message: string;
  setMessage: Dispatch<SetStateAction<string>>;
  mentions: Pick<MentionsMachine, 'mentionedFiles' | 'setMentionedFiles' | 'removeMention'>;
  /** The words of the draft that stand for a skill or a file. */
  draftTokens: string[];
  pruneDraft: (draft: string) => void;
}) {
  const heldMentions = useMemo(() => heldInDraft(mentionedFiles, message), [mentionedFiles, message]);
  const quotes = useMemo(() => heldMentions.filter((f) => f.kind === 'quote'), [heldMentions]);
  const chips = useMemo(() => heldMentions.filter((f) => f.kind === 'chip'), [heldMentions]);

  const addQuote = useCallback((quote: QuoteContext) => {
    const textarea = textareaRef.current;
    const draft = textarea?.value ?? message;
    const held = heldInDraft(mentionedFiles, draft);
    if (held.some((f) => sameMention(f, quote))) {
      // The same text quoted again with something new to say keeps what was said.
      if (quote.reply) setMentionedFiles(held.map((f) => (f.kind === 'quote' && sameMention(f, quote) ? { ...f, reply: quote.reply } : f)));
      return;
    }
    const token = quoteToken(quote.snippet, tokensOf(held));
    const at = textarea?.selectionEnd ?? draft.length;
    const before = draft.slice(0, at);
    const placed = `${before}${before && !/\s$/.test(before) ? ' ' : ''}${token} `;
    const after = draft.slice(at).replace(/^ /, '');
    // Committed before the caret moves, as a pick from the menus is.
    flushSync(() => {
      setMessage(placed + after);
      setMentionedFiles([...held, { ...quote, token }]);
    });
    focusWithoutRing(textarea);
    textarea?.setSelectionRange(placed.length, placed.length);
  }, [textareaRef, message, mentionedFiles, setMentionedFiles, setMessage]);

  const quoteTokens = useMemo(() => tokensOf(quotes), [quotes]);
  const whole = useWholeTokens({
    textareaRef,
    text: message,
    setText: (next) => {
      setMessage(next);
      pruneDraft(next);
    },
    quoteTokens,
    wordTokens: draftTokens,
  });

  /** Returns true when a whole token or a chip took the key. */
  const handleKeyDown = useCallback((e: KeyboardEvent<HTMLTextAreaElement>) => {
    const { selectionStart, selectionEnd } = e.currentTarget;
    // A chip sits where the draft starts, so backing into it takes it out.
    const lastChip = chips.at(-1);
    if (e.key === 'Backspace' && selectionStart === 0 && selectionEnd === 0 && lastChip) {
      e.preventDefault();
      removeMention(lastChip);
      return true;
    }
    return whole.handleKeyDown(e);
  }, [chips, removeMention, whole]);

  return { heldMentions, quotes, chips, addQuote, handleSelect: whole.handleSelect, handleQuotePress: whole.handleQuotePress, handleKeyDown, keepWhole: whole.keepWhole };
}
