import React from 'react';
import { useTranslation } from 'react-i18next';
import { Input } from '../../components/ui/input';

interface EmailOnlyViewProps {
  /** i18n keys for the heading, subtitle, and submit-button labels. */
  title: string;
  subtitle: string;
  submitLabel: string;
  sendingLabel: string;
  /** i18n key for the back button — must name where onBack actually lands
      (the login form by default; the method picker for the magic-link view). */
  backLabel?: string;
  email: string;
  onEmailChange: (value: string) => void;
  onSubmit: (e: React.FormEvent<HTMLFormElement>) => void;
  onBack: () => void;
  error: string | null;
  isSubmitting: boolean;
}

/**
 * Shared single-email view: title + subtitle + email field + submit + back.
 * Backs both the magic-link and password-reset request screens, which differ
 * only in their copy and submit target.
 */
function EmailOnlyView({
  title,
  subtitle,
  submitLabel,
  sendingLabel,
  backLabel = 'auth.backToLogin',
  email,
  onEmailChange,
  onSubmit,
  onBack,
  error,
  isSubmitting,
}: EmailOnlyViewProps) {
  const { t } = useTranslation();
  return (
    <form onSubmit={onSubmit} className="login-page__form">
      <div>
        <h2 className="login-page__view-title">{t(title)}</h2>
        <p className="login-page__view-subtitle">{t(subtitle)}</p>
      </div>
      <div className="login-page__field">
        <label className="login-page__label">{t('common.email')}</label>
        <Input
          type="email"
          value={email}
          onChange={(e) => onEmailChange(e.target.value)}
          placeholder={t('auth.enterEmail')}
          className="login-page__input"
          disabled={isSubmitting}
          autoComplete="email"
          required
        />
      </div>
      {error && <div className="login-page__error">{error}</div>}
      <button type="submit" disabled={isSubmitting} className="login-page__submit">
        {isSubmitting ? t(sendingLabel) : t(submitLabel)}
      </button>
      <button type="button" className="login-page__back" onClick={onBack}>
        {t(backLabel)}
      </button>
    </form>
  );
}

export default EmailOnlyView;
