/** Minimum password length. The password inputs import this for their
 *  `minLength`, and the requirements list below enforces the same floor. */
export const MIN_PASSWORD_LENGTH = 8;

/**
 * The enforced password floor. PasswordStrength renders this list as the
 * checklist under password-creation fields, and submit handlers re-check it
 * via passwordMeetsRequirements; the password inputs import MIN_PASSWORD_LENGTH
 * for their minLength.
 */
export const PASSWORD_REQUIREMENTS = [
  { key: 'auth.reqLength', test: (pw: string) => pw.length >= MIN_PASSWORD_LENGTH },
  { key: 'auth.reqLetter', test: (pw: string) => /[a-zA-Z]/.test(pw) },
  { key: 'auth.reqNumber', test: (pw: string) => /[0-9]/.test(pw) },
] as const;

export function passwordMeetsRequirements(pw: string): boolean {
  return PASSWORD_REQUIREMENTS.every((req) => req.test(pw));
}

/**
 * Shared validation for the create-password + confirm pair. Returns the i18n
 * message for the first failing check (requirements before match) or null when
 * both pass — used by the signup and reset-password submit handlers.
 */
export function validatePasswordPair(
  password: string,
  confirm: string,
  t: (key: string) => string,
): string | null {
  if (!passwordMeetsRequirements(password)) return t('auth.errors.weakPassword');
  if (password !== confirm) return t('auth.passwordsDontMatch');
  return null;
}
