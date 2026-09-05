import { useTranslation } from 'react-i18next';
import type { PluginDiagnostic } from '@/pages/ChatAgent/utils/api';

/**
 * One finding, with the rule behind it. The backend cites the spec on the
 * diagnostics that have one, and this is the moment the citation is worth
 * anything: the user has just been refused and cannot tell our policy from
 * the specification's.
 */
export function DiagnosticLine({
  diagnostic,
  color = 'var(--color-text-tertiary)',
}: {
  diagnostic: PluginDiagnostic;
  color?: string;
}) {
  const { t } = useTranslation();
  const { target, message, spec_ref: specRef } = diagnostic;
  return (
    <p className="text-[0.6875rem] py-1" style={{ color }}>
      {target ? `${target}: ${message}` : message}
      {specRef && (
        <>
          {' '}
          <a
            href={specRef}
            target="_blank"
            rel="noreferrer"
            className="underline underline-offset-2"
          >
            {t('plugins.install.specRef')}
          </a>
        </>
      )}
    </p>
  );
}

/**
 * Why the install step in front of you cannot go on: the message, and the
 * per-component findings a fatal package error carries under it. Shared so a
 * refusal reads the same wherever the flow raises one.
 */
export function StepError({
  error,
  diagnostics = [],
}: {
  error: string | null;
  diagnostics?: PluginDiagnostic[];
}) {
  if (!error) return null;
  return (
    <div className="flex flex-col gap-1">
      <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
        {error}
      </p>
      {diagnostics.map((d, i) => (
        <DiagnosticLine key={`${d.code}-${i}`} diagnostic={d} />
      ))}
    </div>
  );
}
