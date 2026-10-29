import { AppWindowMac } from 'lucide-react';
import { useTranslation } from 'react-i18next';

/**
 * What the browser shows while an email link is being passed to the desktop app.
 *
 * The way out is on screen from the first paint rather than revealed after a
 * timeout, because there is no signal to time. A machine without the app fails
 * silently and identically to one that is merely slow, so a user who sees
 * nothing happen needs the alternative already in front of them.
 */
function ShellHandoff({ onContinueHere }: { onContinueHere: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="login-page__inbox">
      <AppWindowMac className="login-page__inbox-icon" aria-hidden="true" />
      <h2 className="login-page__inbox-title">{t('auth.shellHandoffTitle')}</h2>
      <p className="login-page__inbox-body">{t('auth.shellHandoffBody')}</p>
      <button type="button" className="login-page__back" onClick={onContinueHere}>
        {t('auth.shellHandoffFallback')}
      </button>
    </div>
  );
}

export default ShellHandoff;
