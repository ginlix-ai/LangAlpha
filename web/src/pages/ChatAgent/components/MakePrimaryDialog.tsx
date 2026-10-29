/**
 * Make another computer the primary: the one new workspaces and Home land on.
 *
 * Home moves with the primary, so the confirmation says when it will, and a
 * row whose computer is taking Home shows that move instead of the button.
 * Opened over the Computers dialog, which owns the request and the refusal;
 * mounted only while open.
 */
import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Star } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import { useWorkspace } from '@/hooks/useWorkspace';

import { apiErrorReason, moveRefusal } from '../utils/api';

type Translate = ReturnType<typeof useTranslation>['t'];

interface MakePrimaryDialogProps {
  computerId: string;
  name: string;
  onClose: () => void;
  onConfirm: () => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, as the request rejected it. */
  error: unknown;
}

function MakePrimaryDialog({ computerId, name, onClose, onConfirm, busy, error }: MakePrimaryDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  // Home's own query has it as of its first read, before a start put it on a
  // computer, so the detail decides. Until it lands, the copy assumes the
  // usual case: Home is on the current primary and moves.
  const { data: home, isPlaceholderData } = useWorkspace(useHomeWorkspaceId());
  const homeStays =
    !!home && !isPlaceholderData && (!home.computer_id || home.status === 'unbound' || home.computer_id === computerId);

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {t('common.cancel')}
      </Button>
      <Button type="button" onClick={onConfirm} disabled={busy} aria-busy={busy}>
        {t('computer.makePrimary.button', 'Make primary')}
      </Button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.makePrimary.title', 'Make {{name}} primary?', { name })}
      onClose={onClose}
      closeDisabled={busy}
      width="narrow"
      density="form"
      stacked
      footer={footer}
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {homeStays
          ? t('computer.makePrimary.body', 'New workspaces will be created on {{name}}.', { name })
          : t(
              'computer.makePrimary.bodyHomeMoves',
              'New workspaces will be created on {{name}}, and Home, where the Chief of Staff keeps its files, moves there too.',
              { name },
            )}
      </p>
      {error != null && !busy && (
        <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
          {refusalMessage(error, t)}
        </p>
      )}
    </ModalShell>
  );
}

/** A refused make-primary, in the server's words when it gave some. */
function refusalMessage(err: unknown, t: Translate): string {
  return (
    moveRefusal(err)?.message
    ?? apiErrorReason(err)
    ?? t('computer.makePrimary.failed', "Couldn't make this computer primary. Try again.")
  );
}

/**
 * The row's control: the button, or Home's move to this computer while it
 * runs, which is the request the button made still going.
 */
export function MakePrimaryButton({
  movingHome,
  disabled,
  onClick,
}: {
  movingHome: boolean;
  disabled: boolean;
  onClick: () => void;
}) {
  const { t } = useTranslation();
  if (movingHome) {
    return (
      <span
        role="status"
        aria-live="polite"
        className="inline-flex items-center gap-1.5 px-1 text-xs"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        <span aria-hidden="true" className="inline-flex">
          <Loader size={12} className="text-(--color-accent-primary)" />
        </span>
        {t('computer.makePrimary.movingHome', 'Moving Home…')}
      </span>
    );
  }
  return (
    <Button variant="ghost" size="sm" className="gap-1.5" onClick={onClick} disabled={disabled}>
      <Star className="h-3.5 w-3.5" />
      {t('computer.makePrimary.button', 'Make primary')}
    </Button>
  );
}

export default MakePrimaryDialog;
