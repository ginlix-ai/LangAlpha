import React, { useState } from 'react';
import { Mail } from 'lucide-react';
import { useTranslation, Trans } from 'react-i18next';
import { authErrorMessage } from '../../lib/authErrors';
import { useResendCooldown } from './useResendCooldown';

export type CheckInboxKind = 'signup' | 'magic-link' | 'reset';

const BODY_KEY: Record<CheckInboxKind, string> = {
  signup: 'auth.checkInboxSignup',
  'magic-link': 'auth.checkInboxMagic',
  reset: 'auth.checkInboxReset',
};

interface CheckInboxProps {
  kind: CheckInboxKind;
  email: string;
  /** Re-triggers the email send; resolves to a supabase-style { error } result. */
  onResend: () => Promise<{ error: unknown } | void>;
  onBack: () => void;
}

/**
 * "Check your inbox" screen shown after any flow that sends an email
 * (signup confirmation, magic link, password reset). Replaces the whole
 * login card body so the pending state is unmistakable.
 */
function CheckInbox({ kind, email, onResend, onBack }: CheckInboxProps) {
  const { t } = useTranslation();
  const { secondsLeft, isCoolingDown, start } = useResendCooldown();
  const [isSending, setIsSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleResend = async () => {
    setIsSending(true);
    setError(null);
    try {
      const result = await onResend();
      if (result?.error) throw result.error;
      start();
    } catch (err: unknown) {
      const info = authErrorMessage(err, t);
      setError(info.message);
      // The server enforces its own send cooldown — restart ours to match.
      if (info.code === 'over_email_send_rate_limit') start();
    } finally {
      setIsSending(false);
    }
  };

  return (
    <div className="login-page__inbox">
      <Mail className="login-page__inbox-icon" aria-hidden="true" />
      <h2 className="login-page__inbox-title">{t('auth.checkInboxTitle')}</h2>
      <p className="login-page__inbox-body">
        <Trans i18nKey={BODY_KEY[kind]} values={{ email }} components={{ 1: <strong /> }} />
      </p>
      {error && <div className="login-page__error">{error}</div>}
      <button
        type="button"
        className="login-page__resend-btn"
        onClick={handleResend}
        disabled={isCoolingDown || isSending}
      >
        {isCoolingDown ? t('auth.resendIn', { seconds: secondsLeft }) : t('auth.resend')}
      </button>
      <button type="button" className="login-page__back" onClick={onBack}>
        {t('auth.useDifferentEmail')}
      </button>
    </div>
  );
}

export default CheckInbox;
