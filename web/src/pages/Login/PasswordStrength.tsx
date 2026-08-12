import { useTranslation } from 'react-i18next';
import { PASSWORD_REQUIREMENTS, MIN_PASSWORD_LENGTH } from './passwordRequirements';

/**
 * 0 = under the 8-char minimum, 1 = weak, 2 = fair, 3 = strong.
 * Length does most of the work; character variety nudges the score up.
 * Guidance only — the enforced gates are the requirements below and the
 * server's password policy.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function scorePassword(pw: string): 0 | 1 | 2 | 3 {
  if (pw.length < MIN_PASSWORD_LENGTH) return 0;
  const classes =
    Number(/[a-z]/.test(pw)) +
    Number(/[A-Z]/.test(pw)) +
    Number(/[0-9]/.test(pw)) +
    Number(/[^A-Za-z0-9]/.test(pw));
  if (pw.length >= 16 || (pw.length >= 12 && classes >= 3)) return 3;
  if (pw.length >= 10 && classes >= 2) return 2;
  return 1;
}

const LABEL_KEYS = [
  'auth.strengthTooShort',
  'auth.strengthWeak',
  'auth.strengthFair',
  'auth.strengthStrong',
] as const;

/**
 * Guidance under a password-creation field: the requirements checklist is on
 * screen before anything is typed and items check off as the password meets
 * them; once typing starts, a strength meter joins it — ember while weak,
 * setting to ink at full strength.
 */
function PasswordStrength({ password }: { password: string }) {
  const { t } = useTranslation();
  const score = scorePassword(password);
  return (
    <>
      {password.length > 0 && (
        <div className="login-page__strength" data-score={score}>
          <div className="login-page__strength-track" role="presentation">
            <div className="login-page__strength-fill" />
          </div>
          <span className="login-page__strength-label">{t(LABEL_KEYS[score])}</span>
        </div>
      )}
      <ul className="login-page__reqs">
        {PASSWORD_REQUIREMENTS.map((req) => (
          <li
            key={req.key}
            className="login-page__req"
            data-met={req.test(password) || undefined}
          >
            <span className="login-page__req-mark" aria-hidden="true">
              ✓
            </span>
            {t(req.key)}
          </li>
        ))}
      </ul>
    </>
  );
}

export default PasswordStrength;
