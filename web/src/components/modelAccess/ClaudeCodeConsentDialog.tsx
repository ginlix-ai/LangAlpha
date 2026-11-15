import { useId } from 'react';
import { ExternalLink, ShieldCheck } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';
import { ModalShell } from '@/components/ui/ModalShell';
import { cn } from '@/lib/utils';

/** Anthropic's consumer terms, which apply to a Claude plan used this way. */
const ANTHROPIC_TERMS_URL = 'https://www.anthropic.com/legal/consumer-terms';

/** What storing the token means, the same wherever the user agrees to it:
 *  in the dialog, or beside the setup step's own ways to add a token. */
export function ConsentTerms({ className }: { className?: string }) {
  const { t } = useTranslation();
  return (
    <div className={cn('flex flex-col gap-3 text-sm', className)} style={{ color: 'var(--color-text-secondary)' }}>
      <p>{t('modelAccess.claudeCode.consent.encrypted')}</p>
      <p>{t('modelAccess.claudeCode.consent.unattended')}</p>
      <p>
        {t('modelAccess.claudeCode.consent.terms')}{' '}
        <a
          href={ANTHROPIC_TERMS_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 underline underline-offset-2 hover:opacity-80"
        >
          {t('modelAccess.claudeCode.consent.termsLink')}
          <ExternalLink aria-hidden="true" className="h-3 w-3" />
        </a>
      </p>
      <p>{t('modelAccess.claudeCode.consent.undo')}</p>
    </div>
  );
}

/** What storing the token means, accepted before the server stores one. */
export function ConsentDialog({ onConfirm, onClose }: { onConfirm: () => void; onClose: () => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  return (
    <ModalShell
      labelId={titleId}
      title={t('modelAccess.claudeCode.consent.title')}
      onClose={onClose}
      stacked
      footer={
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onClose}>{t('common.cancel')}</Button>
          <Button type="button" onClick={onConfirm}>{t('modelAccess.claudeCode.consent.confirm')}</Button>
        </div>
      }
    >
      <ConsentTerms />
    </ModalShell>
  );
}

/**
 * The dialog's agreement in a page's own flow: the same terms, and a box the
 * user ticks to agree, which the ways to add a token wait on. While the
 * agreement is being recorded, and once it is (`locked`), the box stays
 * ticked and can no longer be cleared, since unticking it then would not
 * withdraw it.
 */
export function ConsentForm({ agreed, locked, onAgree }: { agreed: boolean; locked: boolean; onAgree: (agreed: boolean) => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  return (
    <section aria-labelledby={titleId} data-testid="token-consent" className="shell-box flex flex-col gap-2.5 px-4 py-3.5">
      <h3 id={titleId} className="flex items-center gap-2 text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
        <ShieldCheck aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
        {t('modelAccess.claudeCode.consent.title')}
      </h3>
      <ConsentTerms className="gap-2 text-xs leading-relaxed" />
      <CheckboxChoice
        checked={agreed || locked}
        disabled={locked}
        onCheckedChange={onAgree}
        size="md"
        className="mt-1 items-center gap-2.5 self-start text-sm font-medium"
      >
        <span style={{ color: 'var(--color-text-primary)' }}>{t('modelAccess.claudeCode.consent.agree')}</span>
      </CheckboxChoice>
    </section>
  );
}
