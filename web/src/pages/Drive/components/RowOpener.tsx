import type React from 'react';
import { useId, useRef } from 'react';
import { cn } from '@/lib/utils';

/**
 * A list row's opener: a button the size of the row, laid under its content,
 * so a control in the content (a comment marker) can be a button of its own.
 * The content names the button and takes the pointer's presses as its own; a
 * press there focuses the button, so the arrow keys go on from this row.
 */
export function RowOpener({ onOpen, active, className, button, press, children }: {
  onOpen: () => void;
  active: boolean;
  /** The content's own class, which lays it out. */
  className: string;
  /** What the button takes besides its press: its keys, a mark its list finds it by. */
  button?: React.ButtonHTMLAttributes<HTMLButtonElement> & { 'data-drive-row'?: boolean };
  /** What a pointer does on the whole row, button and content alike: a press read ahead, a drag. */
  press?: React.HTMLAttributes<HTMLElement>;
  children: React.ReactNode;
}) {
  const id = useId();
  const hit = useRef<HTMLButtonElement>(null);
  return (
    <>
      <button
        type="button"
        ref={hit}
        className="drive-row-hit"
        aria-labelledby={id}
        aria-current={active ? 'true' : undefined}
        {...button}
        {...press}
        onClick={onOpen}
      />
      <span
        id={id}
        className={cn('drive-row-body', className)}
        {...press}
        onClick={() => {
          hit.current?.focus({ preventScroll: true });
          onOpen();
        }}
      >
        {children}
      </span>
    </>
  );
}
