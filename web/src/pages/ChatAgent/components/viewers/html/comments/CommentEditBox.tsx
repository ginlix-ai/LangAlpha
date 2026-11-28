import React, { useEffect, useEffectEvent, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { apiErrorDetailMessage } from '@/api/errors';
import { Button } from '@/components/ui/button';
import { isImeComposing } from '@/lib/ime';
import { CommentField, FieldFrame } from './CommentField';
import { useSubmitting } from './useSubmitting';

/**
 * A card's words, open to change: the comment box's field, with Save and
 * Cancel. Enter saves, as the comment box's field sends; Esc cancels; empty
 * words cannot be saved. Esc is taken here before the box or list around the
 * card hears it, so it ends the edit and nothing else.
 */
export function CommentEditBox({ initial, onSave, onCancel }: {
  initial: string;
  /** Rejects when the server refused the words; they stay in the field. */
  onSave: (body: string) => Promise<void>;
  onCancel: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const [value, setValue] = useState(initial);
  const { busy, failed, run } = useSubmitting();
  const rootRef = useRef<HTMLDivElement>(null);
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const empty = !value.trim();

  // On the window and in its capture phase, so it runs before the box's own
  // Esc (a document listener) and the mode's. Held while a save is out, as
  // Cancel is: ending the edit then would drop the words and any refusal.
  const cancel = useEffectEvent(() => {
    if (!busy) onCancel();
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || isImeComposing(e)) return;
      if (!(e.target instanceof Node) || !rootRef.current?.contains(e.target)) return;
      e.preventDefault();
      e.stopPropagation();
      cancel();
    };
    window.addEventListener('keydown', onKey, { capture: true });
    return () => window.removeEventListener('keydown', onKey, { capture: true });
  }, []);

  const save = () => {
    if (!empty) void run(() => onSave(value.trim()));
  };

  return (
    <FieldFrame ref={rootRef} data-comment-edit="" inline className="mt-1">
      <CommentField
        ref={fieldRef}
        value={value}
        onChange={setValue}
        onEnter={save}
        held={busy}
        inline
        aria-label={t('pageViewer:comments.editLabel')}
        autoFocus
        onFocus={(e) => e.currentTarget.setSelectionRange(e.currentTarget.value.length, e.currentTarget.value.length)}
      />
      {failed && (
        <p role="alert" className="text-xs" style={{ color: 'var(--color-icon-danger)' }}>
          {/* The server's reason where it gave one (the words reached the agent meanwhile). */}
          {apiErrorDetailMessage(failed.error) ?? t('pageViewer:comments.editFailed')}
        </p>
      )}
      <div className="flex items-center justify-end gap-1">
        <Button type="button" size="sm" variant="ghost" className="h-6 rounded-md px-2 text-xs" disabled={busy} onClick={onCancel}>
          {t('common.cancel')}
        </Button>
        <Button type="button" size="sm" className="h-6 rounded-md px-2.5 text-xs" disabled={empty || busy} onClick={save}>
          {t('common.save')}
        </Button>
      </div>
    </FieldFrame>
  );
}
