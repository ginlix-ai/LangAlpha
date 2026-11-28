import React, { useLayoutEffect } from 'react';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import { onScreenKeyboardUp } from '@/lib/visualViewport';
import { enterKey } from './composerFooter';

/** How tall the field grows with what is written before it scrolls, in px. */
const FIELD_MAX = 160;

const INLINE_STYLE: React.CSSProperties = {
  backgroundColor: 'var(--color-bg-input)',
  borderColor: 'var(--color-border-input)',
};

/**
 * What a field and its controls sit in: inline, a bordered well under a card
 * or in the list; else the box over the page itself, which is their edge.
 */
export function FieldFrame({ inline, className, ...div }: React.ComponentProps<'div'> & { inline: boolean }): React.ReactElement {
  return (
    <div
      {...div}
      className={cn(
        'flex min-w-0 flex-col',
        inline ? 'gap-0.5 rounded-lg border pt-[7px] pr-2 pb-[5px] pl-2.5' : 'gap-1.5 px-3 pt-2.5 pb-2',
        className,
      )}
      style={inline ? INLINE_STYLE : undefined}
    />
  );
}

type FieldProps = Omit<
  React.ComponentProps<'textarea'>,
  'value' | 'onChange' | 'onKeyDown' | 'readOnly' | 'rows' | 'maxLength' | 'className' | 'ref'
>;

/**
 * The words of a comment, a reply or an edit: a field that grows with them
 * up to a few lines, so the box never hides what is being written behind a
 * scrollbar it does not need yet. Enter (`onEnter`, with whether ⌘/Ctrl was
 * held) is the caller's; Shift+Enter, an input method's Enter and Enter with
 * an on-screen keyboard up are new lines (`enterKey`).
 */
export function CommentField({ ref, value, onChange, onEnter, held, inline, ...field }: FieldProps & {
  ref: React.RefObject<HTMLTextAreaElement | null>;
  value: string;
  onChange: (value: string) => void;
  onEnter: (enter: 'plain' | 'mod') => void;
  /** A send or save is out: words written meanwhile would go with the box it closes. */
  held: boolean;
  /** One line tall, in a well under a card or in the list; else two, as the box over the page. */
  inline: boolean;
}): React.ReactElement {
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${Math.min(el.scrollHeight, FIELD_MAX)}px`;
  }, [ref, value]);

  return (
    <textarea
      {...field}
      ref={ref}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      readOnly={held}
      onKeyDown={(e) => {
        if (isImeComposing(e)) return;
        const enter = enterKey(e, onScreenKeyboardUp());
        if (!enter) return;
        e.preventDefault();
        onEnter(enter);
      }}
      rows={inline ? 1 : 2}
      maxLength={8000}
      className={cn(
        // The box is the field's edge: the field draws no border, halo or
        // ring of its own. A ring would also light on a box opened by a drag
        // in the page, which the app never hears as a pointer.
        'owns-its-edge block w-full resize-none border-0 bg-transparent p-0 py-0.5 leading-[1.45] outline-hidden',
        'text-(--color-text-primary) placeholder:text-(--color-text-quaternary)',
        inline ? 'min-h-[22px] text-[13px]' : 'min-h-[42px] text-sm',
      )}
    />
  );
}
