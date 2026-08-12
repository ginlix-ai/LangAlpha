import { Trans } from 'react-i18next';

interface AccountRecoveryHintProps {
  i18nKey: string;
  onPrimary: () => void;
  onMagic: () => void;
  onReset: () => void;
  disabled?: boolean;
}

/**
 * Inline recovery hint shown under an auth error: three <Trans>-embedded
 * actions (sign in with password, send a magic link, reset the password).
 * Shared by the login "invalid credentials" and signup "email exists" cases —
 * the caller supplies the copy key and the per-view button behaviors.
 */
function AccountRecoveryHint({ i18nKey, onPrimary, onMagic, onReset, disabled }: AccountRecoveryHintProps) {
  return (
    <p className="login-page__error-hint">
      <Trans
        i18nKey={i18nKey}
        components={{
          1: (
            <button
              type="button"
              className="login-page__error-link"
              onClick={onPrimary}
            />
          ),
          2: (
            <button
              type="button"
              className="login-page__error-link"
              disabled={disabled}
              onClick={onMagic}
            />
          ),
          3: (
            <button
              type="button"
              className="login-page__error-link"
              onClick={onReset}
            />
          ),
        }}
      />
    </p>
  );
}

export default AccountRecoveryHint;
