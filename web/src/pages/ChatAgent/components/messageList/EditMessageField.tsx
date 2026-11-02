import { useLayoutEffect, useState, type KeyboardEvent, type RefObject } from 'react';
import { DraftTokenLayer } from '@/components/ui/chat-input.composerParts';
import { heldInDraft, readGivenBack, spellOutQuotes } from '@/components/ui/chat-input.quoteTokens';
import type { QuoteMention } from '@/components/ui/chat-input.types';
import { useWholeTokens } from '@/components/ui/chat-input.useQuoteTokens';
import { isImeComposing } from '@/lib/ime';
import '@/components/ui/chat-input.css';

interface Reading {
  /** The message as it goes out, which is what the box's host holds. */
  value: string;
  /** The same with each quote standing as its token, as the composer shows one. */
  draft: string;
  /** Kept when a token goes, for an undo that brings it back. */
  quotes: QuoteMention[];
}

function read(value: string): Reading {
  const { text, mentions } = readGivenBack(value, [], []);
  return { value, draft: text, quotes: mentions.filter((m) => m.kind === 'quote') };
}

/** The box a sent message is edited in. Its host reads and writes the message as sent. */
export function EditMessageField({ textareaRef, value, onChange, onKeyDown }: {
  textareaRef: RefObject<HTMLTextAreaElement | null>;
  value: string;
  onChange: (value: string) => void;
  onKeyDown: (e: KeyboardEvent<HTMLTextAreaElement>) => void;
}) {
  const [reading, setReading] = useState(() => read(value));
  // An edit the session could not take comes back from the host.
  if (reading.value !== value) setReading(read(value));
  const { draft, quotes } = reading;
  const held = heldInDraft(quotes, draft);

  const change = (next: string) => {
    const sent = spellOutQuotes(next, heldInDraft(quotes, next));
    setReading({ value: sent, draft: next, quotes });
    onChange(sent);
  };

  const whole = useWholeTokens({ textareaRef, text: draft, setText: change, quoteTokens: held.map((f) => f.token) });

  useLayoutEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = 'auto';
    textarea.style.height = `${textarea.scrollHeight}px`;
  }, [textareaRef, draft]);

  return (
    <div className="relative">
      <DraftTokenLayer
        text={draft}
        tokens={[]}
        quotes={held}
        layerRef={null}
        onQuotePress={whole.handleQuotePress}
        className="inset-0 text-sm leading-relaxed"
      />
      <textarea
        ref={textareaRef}
        value={draft}
        onChange={(e) => change(whole.keepWhole(e.target.value, e.target.selectionStart))}
        onSelect={whole.handleSelect}
        onKeyDown={(e) => {
          if (!isImeComposing(e) && whole.handleKeyDown(e)) return;
          onKeyDown(e);
        }}
        className="relative block w-full bg-transparent text-sm resize-none leading-relaxed overflow-hidden"
        style={{ color: 'var(--color-text-primary)' }}
        rows={1}
      />
    </div>
  );
}
