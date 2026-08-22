import type { PluginDiagnostic } from '@/pages/ChatAgent/utils/api';

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
        <p
          key={`${d.code}-${i}`}
          className="text-[0.6875rem]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {d.target ? `${d.target}: ${d.message}` : d.message}
        </p>
      ))}
    </div>
  );
}
