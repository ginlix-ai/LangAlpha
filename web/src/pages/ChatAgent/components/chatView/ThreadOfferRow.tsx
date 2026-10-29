import type React from 'react';
import { useEffect, useEffectEvent, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Info, X } from 'lucide-react';

/** How long an expiring offer stays up while the pointer and focus are elsewhere. */
export const OFFER_EXPIRES_MS = 8000;

interface ThreadOfferRowProps {
  text: string;
  action: string;
  onAction: () => void;
  busy?: boolean;
  /** For a row that comes back in place of what its action opened. */
  focusAction?: boolean;
  onDismiss: () => void;
  /** Given, the row goes on its own. The clock stops while the pointer or
   *  focus is on the row, so the action stays in reach for as long as it is
   *  being read or reached for, and starts over when both leave. */
  onExpire?: () => void;
}

/* An offer that follows a change made to the thread, in the rows above its
   composer. Neutral on purpose: nothing is wrong, so it does not borrow the
   warning treatment the fallback pill uses. */
export function ThreadOfferRow({
  text,
  action,
  onAction,
  busy = false,
  focusAction = false,
  onDismiss,
  onExpire,
}: ThreadOfferRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [pointerOn, setPointerOn] = useState(false);
  const [focusOn, setFocusOn] = useState(false);
  const expires = !!onExpire;
  const paused = pointerOn || focusOn;
  // A host re-renders with every streamed token, so the clock must not
  // restart when the callback's identity does.
  const expire = useEffectEvent(() => onExpire?.());
  useEffect(() => {
    if (!expires || paused) return;
    const timer = setTimeout(expire, OFFER_EXPIRES_MS);
    return () => clearTimeout(timer);
  }, [expires, paused]);

  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5 px-3 py-2 rounded-md text-sm"
      role="status" aria-live="polite"
      onMouseEnter={() => setPointerOn(true)}
      onMouseLeave={() => setPointerOn(false)}
      onFocus={() => setFocusOn(true)}
      onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setFocusOn(false); }}
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-default)',
      }}>
      <Info aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      {/* The floor is what lets the row wrap: with none, a narrow composer (the
          market panel) squeezes the sentence to a word per line beside the
          button instead of moving the button below it. */}
      <span className="flex-1 min-w-48">{text}</span>
      <button
        type="button"
        onClick={onAction}
        disabled={busy}
        autoFocus={focusAction}
        className="text-xs font-medium whitespace-nowrap rounded-md px-2.5 py-1 shrink-0 hover:bg-(--color-bg-hover) disabled:opacity-50"
        style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
      >
        {action}
      </button>
      <button
        type="button"
        onClick={onDismiss}
        aria-label={t('common.close')}
        className="p-1 rounded shrink-0 hover:opacity-70"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
