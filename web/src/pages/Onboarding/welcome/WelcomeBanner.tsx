import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { useWelcomeOffer } from './useWelcomeOffer';

/**
 * The dashboard's offer of the welcome flow, for a user who has not saved an
 * investing profile: one who skipped it, left it halfway, or had an account
 * before it existed and so never had it open by itself. Resumes a draft where
 * this browser has one.
 */
export function WelcomeBanner({ onDismiss }: { onDismiss: () => void }) {
  const { t } = useTranslation();
  const offer = useWelcomeOffer();
  if (!offer) return null;
  return (
    <div
      data-testid="welcome-banner"
      className="mb-6 flex items-center gap-3 rounded-lg border border-(--color-border-muted) bg-(--color-bg-card) px-4 py-3"
    >
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-(--color-text-primary)">{t('onboarding.welcome.banner.title')}</p>
        <p className="mt-0.5 text-xs text-(--color-text-secondary)">{t('onboarding.welcome.banner.desc')}</p>
      </div>
      <button
        type="button"
        onClick={() => offer.open()}
        className="shrink-0 rounded-md bg-(--color-btn-primary-bg) px-3 py-1.5 text-xs font-medium text-(--color-btn-primary-text) transition-opacity hover:opacity-90"
      >
        <span>{offer.label}</span>
      </button>
      <button
        type="button"
        onClick={onDismiss}
        aria-label={t('common.close')}
        className="shrink-0 rounded p-1 text-(--color-text-tertiary) transition-colors hover:bg-(--color-bg-hover)"
      >
        <X size={14} />
      </button>
    </div>
  );
}
