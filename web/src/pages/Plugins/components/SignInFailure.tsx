import { useTranslation } from 'react-i18next';
import { signInFailureParts } from '../connectOutcome';

/** `signInFailureParts`, a line each. */
export function SignInFailure({
  reasonKey,
  detail,
}: {
  reasonKey?: string;
  detail?: string | null;
}) {
  const { t } = useTranslation();
  const [reason, said] = signInFailureParts(t, reasonKey, detail);
  return (
    <span className="flex flex-col gap-0.5">
      <span>{reason}</span>
      {said && <span>{said}</span>}
    </span>
  );
}
