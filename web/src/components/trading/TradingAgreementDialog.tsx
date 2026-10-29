import { useId, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { AlertTriangle } from 'lucide-react';
import { CheckboxChoice } from '@/components/ui/choice';
import { ModalShell } from '@/components/ui/ModalShell';
import { TRADING_LEVEL_INFO } from '@/lib/tradingPermission';
import type { AgreementLevel } from '@/types/api';

/**
 * The responsibility agreement that has to be accepted before orders may skip
 * approval. Mounted per question, so the box starts unticked every time it
 * opens: a tick left over from a cancelled attempt is not an agreement.
 *
 * It stays open on a failed save, with the tick kept, so the retry is one click
 * and the user is never left guessing whether the level changed.
 *
 * Its text is this bundle's copy, so once the server asks for another
 * agreement version the dialog offers a reload in place of Agree: an opt-in
 * never claims text the user was not shown.
 */
export function TradingAgreementDialog({
  level,
  pending,
  outdated,
  onAgree,
  onCancel,
}: {
  level: AgreementLevel;
  pending: boolean;
  /** The server asks for an agreement version other than the one shown. */
  outdated: boolean;
  onAgree: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const titleId = useId();
  const [understood, setUnderstood] = useState(false);
  const info = TRADING_LEVEL_INFO[level];

  // One button for both actions, so focus held on Agree stays put when a
  // failed save reveals a new version and it turns into Reload.
  const primaryLabel = pending
    ? t('common.saving')
    : outdated
      ? t('settings.tradingPermission.agreement.reload')
      : t('settings.tradingPermission.agreement.agree');

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <button
        type="button"
        onClick={onCancel}
        disabled={pending}
        className="px-3 py-1.5 text-xs font-medium rounded-md transition-colors hover:bg-(--color-bg-hover) disabled:opacity-50"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {t('common.cancel')}
      </button>
      <button
        type="button"
        onClick={outdated ? () => window.location.reload() : onAgree}
        disabled={pending || (!outdated && !understood)}
        className="px-3 py-1.5 text-xs font-medium rounded-md transition-opacity hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed"
        style={{
          color: 'var(--color-btn-primary-text)',
          backgroundColor: 'var(--color-btn-primary-bg)',
        }}
      >
        {primaryLabel}
      </button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('settings.tradingPermission.agreement.title')}
      subtitle={t(info.labelKey)}
      onClose={onCancel}
      closeDisabled={pending}
      footer={footer}
    >
      <div className="flex flex-col gap-3 text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
        <p>{t(info.agreement.descriptionKey)}</p>
        <p>
          <Trans
            i18nKey="settings.tradingPermission.agreement.common"
            components={{
              orders: (
                <Link
                  to="/orders"
                  className="underline underline-offset-2"
                  style={{ color: 'var(--color-text-primary)' }}
                />
              ),
            }}
          />
        </p>
      </div>

      {outdated ? (
        <p role="alert" className="flex items-start gap-2.5 text-sm" style={{ color: 'var(--color-text-primary)' }}>
          <AlertTriangle
            aria-hidden="true"
            className="mt-0.5 h-4 w-4 shrink-0"
            style={{ color: 'var(--color-warning)' }}
          />
          <span>{t('settings.tradingPermission.agreement.changed')}</span>
        </p>
      ) : (
        <CheckboxChoice
          checked={understood}
          disabled={pending}
          onCheckedChange={setUnderstood}
          size="md"
          markClassName="mt-0.5"
          className="items-start gap-2.5 rounded-md text-sm"
        >
          <span style={{ color: 'var(--color-text-primary)' }}>
            {t('settings.tradingPermission.agreement.checkbox')}
          </span>
        </CheckboxChoice>
      )}
    </ModalShell>
  );
}
