import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useRemoveAccess } from '@/hooks/useModelAccessActions';
import { apiErrorDetail } from '@/api/errors';

/**
 * Confirms disconnecting a subscription or removing a key, runs it, and keeps
 * the dialog open with the reason when it fails. Mounted only while open.
 */
export function RemoveAccessDialog({
  provider,
  name,
  onClose,
  onRemoved,
  stacked = false,
}: {
  provider: ConfiguredProvider;
  /** How the row names the provider. */
  name: string;
  onClose: () => void;
  onRemoved?: () => void;
  /** Opened over another shell (the manage sheet). */
  stacked?: boolean;
}) {
  const { t } = useTranslation();
  const titleId = useId();
  const remove = useRemoveAccess();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const subscription = provider.access_type === 'oauth';

  const confirm = async () => {
    setBusy(true);
    setError(null);
    try {
      await remove(provider);
    } catch (err) {
      const detail = apiErrorDetail(err);
      setError(typeof detail === 'string' && detail ? detail : t(subscription ? 'modelAccess.remove.disconnectFailed' : 'modelAccess.remove.removeFailed'));
      setBusy(false);
      return;
    }
    setBusy(false);
    onRemoved?.();
    onClose();
  };

  return (
    <ModalShell
      labelId={titleId}
      title={t(subscription ? 'modelAccess.remove.disconnectTitle' : 'modelAccess.remove.removeTitle', { name })}
      onClose={onClose}
      closeDisabled={busy}
      width="narrow"
      stacked={stacked}
      footer={
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button type="button" variant="destructive" onClick={confirm} disabled={busy} aria-busy={busy}>
            {busy
              ? t(subscription ? 'modelAccess.remove.disconnecting' : 'modelAccess.remove.removing')
              : t(subscription ? 'modelAccess.remove.disconnect' : 'modelAccess.remove.remove')}
          </Button>
        </div>
      }
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {t(subscription ? 'modelAccess.remove.disconnectBody' : 'modelAccess.remove.removeBody')}
      </p>
      {error && !busy && (
        <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}
    </ModalShell>
  );
}
