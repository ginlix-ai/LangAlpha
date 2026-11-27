/**
 * Send now, under the reader's own waiting message. A steer waits for the
 * agent's next model call, which a long command holds back; asking for it now
 * moves that command to the background (nothing is stopped), so the agent
 * reads the message within seconds. The control lives only while the message
 * waits: the delivery that clears the bubble's shimmer unmounts it.
 */
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Check } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';

type SendNowState = 'idle' | 'sending' | 'sent' | 'settled';

const INHERIT_INK = { color: 'inherit' };

export function SendNowAction({ id, onSendNow }: {
  /** The id the message went out under: a steer's request key, or an
   *  instruction's input id. */
  id: string;
  onSendNow: (id: string) => Promise<boolean>;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const [state, setState] = useState<SendNowState>('idle');
  // A message the server already read or settled leaves nothing to do, and
  // its delivery is about to clear the bubble.
  if (state === 'settled') return null;

  const busy = state !== 'idle';
  const handleClick = async () => {
    if (busy) return;
    setState('sending');
    try {
      setState((await onSendNow(id)) ? 'sent' : 'settled');
    } catch {
      setState('idle');
    }
  };

  const label = state === 'sending'
    ? t('chat.actions.sendingNow')
    : state === 'sent'
      ? t('chat.actions.sentNow')
      : t('chat.actions.sendNow');
  return (
    <button
      type="button"
      onClick={handleClick}
      // Kept focusable once pressed, so the focus stays where the reader left
      // it while the label says what happened.
      aria-disabled={busy || undefined}
      title={t('chat.actions.sendNowHint')}
      data-testid="send-now"
      className={cn(
        'press tap-target inline-flex items-center gap-1.5 min-h-8 -mt-1 px-2 rounded-md text-xs transition-colors text-(--color-text-tertiary)',
        busy ? 'cursor-default' : 'hover:bg-(--color-bg-hover) hover:text-(--color-text-secondary)',
      )}
    >
      {state === 'sending'
        ? <span className="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center" aria-hidden="true"><Loader size={12} style={INHERIT_INK} /></span>
        : state === 'sent' && <Check className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />}
      {/* The label changes with no focus move, so it is announced as it does. */}
      <span aria-live="polite">{label}</span>
    </button>
  );
}
