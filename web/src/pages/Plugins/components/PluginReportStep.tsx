import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import type {
  PluginComponentResult,
  PluginInstallReport,
} from '@/pages/ChatAgent/utils/api';
import { DiagnosticLine } from './StepError';

/**
 * Step 3 of the install wizard: the structured install report, grouped into
 * what landed, what was skipped (with the backend's reason verbatim), and
 * warnings. sse entries the probe found upgradable render with a consent
 * checkbox — installing them as streamable-http is the user's call, made
 * here, never silently.
 */

const LANDED = new Set(['created', 'updated', 'unchanged', 'exists']);

function ComponentLine({ result }: { result: PluginComponentResult }) {
  const label = result.name || result.key;
  return (
    <div className="flex flex-col gap-0.5 py-1">
      <div className="flex items-center gap-2 min-w-0">
        <span
          className="text-xs font-medium truncate"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {label}
        </span>
        <span
          className="text-[0.6875rem] font-mono px-1.5 py-0.5 rounded flex-shrink-0"
          style={{
            color: 'var(--color-text-tertiary)',
            backgroundColor: 'var(--color-bg-tag)',
          }}
        >
          {result.kind}
        </span>
        <span
          className="text-[0.6875rem] flex-shrink-0"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {result.status}
        </span>
      </div>
      {result.reason && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {result.reason}
        </p>
      )}
      {result.warnings.map((w) => (
        <p key={w} className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {w}
        </p>
      ))}
    </div>
  );
}

function Group({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <div
        className="text-[0.6875rem] font-medium uppercase tracking-wide"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {title}
      </div>
      <div
        className="flex flex-col divide-y rounded-md px-3 py-1"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        {children}
      </div>
    </div>
  );
}

export function PluginReportStep({
  report,
  onUpgradeSse,
  upgrading,
  onContinue,
  continueLabel,
}: {
  report: PluginInstallReport;
  /** Present when held-back sse entries can be consented into http. */
  onUpgradeSse?: (keys: string[]) => void;
  upgrading?: boolean;
  onContinue: () => void;
  continueLabel: string;
}) {
  const { t } = useTranslation();
  const [checkedKeys, setCheckedKeys] = useState<Set<string>>(new Set());

  const landed = report.components.filter((c) => LANDED.has(c.status));
  const upgradable = report.components.filter((c) => c.status === 'upgradable');
  // A component that errored is not a component that was skipped. Both used to
  // fall through the same negation into the neutral "Skipped" group, where the
  // only thing separating a failure from a no-op was the status word in
  // tertiary text.
  const failed = report.components.filter((c) => c.status === 'error');
  const skipped = report.components.filter(
    (c) =>
      !LANDED.has(c.status) &&
      c.status !== 'upgradable' &&
      c.status !== 'error',
  );
  const warnings = report.diagnostics.filter((d) => d.level === 'warning');
  // Errors get their own group rather than riding with the warnings. Filtering
  // the list to one level used to drop them outright, which mattered most for
  // the one finding the reader cannot afford to miss: a credential the entry
  // asked for and did not get.
  const errors = report.diagnostics.filter((d) => d.level === 'error');
  // An update that found nothing to do reports nothing: say so, rather than
  // showing a panel that is empty for a reason the reader has to guess.
  const empty =
    report.components.length === 0 &&
    report.diagnostics.length === 0 &&
    report.dropped_files.length === 0;

  return (
    <div className="flex flex-col gap-3 max-h-[60vh] overflow-y-auto pr-1">
      {empty && (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.install.reportNoChanges')}
        </p>
      )}

      {landed.length > 0 && (
        <Group title={t('plugins.install.reportInstalled')}>
          {landed.map((c) => (
            <ComponentLine key={`${c.kind}:${c.key}`} result={c} />
          ))}
        </Group>
      )}

      {upgradable.length > 0 && (
        <Group title={t('plugins.install.reportUpgradable')}>
          <p
            className="text-[0.6875rem] py-1"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('plugins.install.sseUpgradeHint')}
          </p>
          {upgradable.map((c) => (
            <label
              key={c.key}
              className="flex items-center gap-2 py-1 cursor-pointer"
            >
              <input
                type="checkbox"
                checked={checkedKeys.has(c.key)}
                onChange={(e) => {
                  const next = new Set(checkedKeys);
                  if (e.target.checked) next.add(c.key);
                  else next.delete(c.key);
                  setCheckedKeys(next);
                }}
              />
              <span className="text-xs" style={{ color: 'var(--color-text-primary)' }}>
                {c.name || c.key}
              </span>
            </label>
          ))}
          {onUpgradeSse && (
            <div className="flex justify-end py-1">
              <button
                type="button"
                onClick={() => {
                  // Clear before handing off: the list re-renders from the new
                  // report, and keys that just landed would otherwise stay
                  // ticked and be re-submitted by a second confirm.
                  const keys = [...checkedKeys];
                  setCheckedKeys(new Set());
                  onUpgradeSse(keys);
                }}
                disabled={checkedKeys.size === 0 || upgrading}
                className="inline-flex items-center gap-1.5 px-2 py-1 text-[0.6875rem] rounded-md disabled:opacity-50"
                style={{
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-muted)',
                }}
              >
                {upgrading && <Loader size={12} className="text-current" />}
                {t('plugins.install.sseUpgradeConfirm')}
              </button>
            </div>
          )}
        </Group>
      )}

      {skipped.length > 0 && (
        <Group title={t('plugins.install.reportSkipped')}>
          {skipped.map((c) => (
            <ComponentLine key={`${c.kind}:${c.key}`} result={c} />
          ))}
        </Group>
      )}

      {(errors.length > 0 || failed.length > 0) && (
        <Group title={t('plugins.install.reportErrors')}>
          {failed.map((c) => (
            <ComponentLine key={`${c.kind}:${c.key}`} result={c} />
          ))}
          {errors.map((d, i) => (
            <DiagnosticLine
              key={`${d.code}-${i}`}
              diagnostic={d}
              color="var(--color-loss)"
            />
          ))}
        </Group>
      )}

      {warnings.length > 0 && (
        <Group title={t('plugins.install.reportWarnings')}>
          {warnings.map((d, i) => (
            <DiagnosticLine key={`${d.code}-${i}`} diagnostic={d} />
          ))}
        </Group>
      )}

      {report.dropped_files.length > 0 && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.install.droppedFiles', {
            files: report.dropped_files.join(', '),
          })}
        </p>
      )}

      <div className="flex justify-end">
        {/* Guarded like its sibling Confirm: leaving mid-upgrade unmounts the
            step that is going to report what landed, so the user closes the
            wizard on a request whose outcome they never see. */}
        <button
          type="button"
          onClick={onContinue}
          disabled={upgrading}
          className="px-3 py-1.5 text-xs rounded-md disabled:opacity-50"
          style={{
            color: 'var(--color-btn-primary-text)',
            backgroundColor: 'var(--color-btn-primary-bg)',
          }}
        >
          {continueLabel}
        </button>
      </div>
    </div>
  );
}
