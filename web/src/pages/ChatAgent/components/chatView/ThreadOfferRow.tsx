import type React from 'react';
import { useEffect, useEffectEvent, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { ComposerRow } from './ComposerRow';

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
   composer. */
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
    <ComposerRow
      text={text}
      onMouseEnter={() => setPointerOn(true)}
      onMouseLeave={() => setPointerOn(false)}
      onFocus={() => setFocusOn(true)}
      onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setFocusOn(false); }}
    >
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
    </ComposerRow>
  );
}
