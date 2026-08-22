import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useDialogA11y } from '@/hooks/useDialogA11y';
import { toast } from '@/components/ui/use-toast';
import {
  useBindPluginSecrets,
  useInstallPluginFromUrl,
  useInstallPluginFromZip,
  useUpgradePluginSseEntries,
} from '@/hooks/usePlugins';
import { useUserVaultBlueprints } from '@/hooks/useUserVault';
import {
  formatApiErrorDetail,
  multiplePluginCandidates,
  type PluginCandidate,
  type PluginDiagnostic,
  type PluginInstallReport,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api';
import { PluginSourceStep } from './PluginSourceStep';
import { PluginChooseStep } from './PluginChooseStep';
import { PluginReportStep } from './PluginReportStep';
import { PluginBindingsStep } from './PluginBindingsStep';

/**
 * The install wizard: source → (choose →) installing → report → bindings. A
 * fatal package error (422) returns to the source step with the backend's
 * diagnostics verbatim; a multiple_plugins 422 (a marketplace repo) becomes
 * the chooser, which re-requests with the picked subdirectory; everything
 * survivable lands in the report step. The bindings step appears only when
 * the plugin declares secrets the user vault doesn't hold.
 */

type WizardSource = { file: File } | { url: string };

type WizardStep =
  | { kind: 'source' }
  | { kind: 'choose'; source: WizardSource; candidates: PluginCandidate[] }
  | { kind: 'installing'; pct: number | null } // null = git (indeterminate)
  | { kind: 'report'; result: PluginInstallResponse }
  | { kind: 'bindings'; result: PluginInstallResponse };

function fatalDiagnostics(err: unknown): PluginDiagnostic[] {
  const detail = (err as { response?: { data?: { detail?: unknown } } })
    ?.response?.data?.detail;
  const diags = (detail as { diagnostics?: unknown })?.diagnostics;
  return Array.isArray(diags) ? (diags as PluginDiagnostic[]) : [];
}

export function PluginInstallWizard({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  const dialogRef = useDialogA11y<HTMLDivElement>(onClose);
  const [step, setStep] = useState<WizardStep>({ kind: 'source' });
  const [error, setError] = useState<string | null>(null);
  const [diagnostics, setDiagnostics] = useState<PluginDiagnostic[]>([]);
  const [bindError, setBindError] = useState<string | null>(null);

  const installZip = useInstallPluginFromZip();
  const installUrl = useInstallPluginFromUrl();
  const upgradeSse = useUpgradePluginSseEntries();
  const bindSecrets = useBindPluginSecrets();
  const { data: blueprintData } = useUserVaultBlueprints();

  async function install(source: WizardSource, subdir?: string) {
    setError(null);
    setDiagnostics([]);
    setStep({ kind: 'installing', pct: 'file' in source ? 0 : null });
    try {
      const result =
        'file' in source
          ? await installZip.mutateAsync({
              file: source.file,
              subdir,
              onProgress: (pct) =>
                setStep((s) =>
                  s.kind === 'installing' ? { kind: 'installing', pct } : s,
                ),
            })
          : await installUrl.mutateAsync({ sourceUrl: source.url, subdir });
      setStep({ kind: 'report', result });
    } catch (err) {
      const candidates = multiplePluginCandidates(err);
      if (candidates) {
        setStep({ kind: 'choose', source, candidates });
        return;
      }
      setError(formatApiErrorDetail(err));
      setDiagnostics(fatalDiagnostics(err));
      setStep({ kind: 'source' });
    }
  }

  function handlePick(candidate: PluginCandidate) {
    if (step.kind !== 'choose') return;
    // An external marketplace entry lives in another repo: install it from
    // its own source URL. In-repo candidates re-run the original source
    // with the picked subdirectory.
    if (candidate.source_url) {
      void install({ url: candidate.source_url });
    } else {
      void install(step.source, candidate.path);
    }
  }

  /** Merge the sse-upgrade follow-up into the displayed report by key. */
  function mergeReport(
    base: PluginInstallReport,
    followUp: PluginInstallReport,
  ): PluginInstallReport {
    const byKey = new Map(
      followUp.components.map((c) => [`${c.kind}:${c.key}`, c]),
    );
    return {
      ...base,
      components: base.components.map(
        (c) => byKey.get(`${c.kind}:${c.key}`) ?? c,
      ),
    };
  }

  async function handleUpgradeSse(keys: string[]) {
    if (step.kind !== 'report') return;
    try {
      const followUp = await upgradeSse.mutateAsync({
        name: step.result.plugin.name,
        keys,
      });
      setStep({
        kind: 'report',
        result: {
          plugin: followUp.plugin,
          report: mergeReport(step.result.report, followUp.report),
        },
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.install.sseUpgradeFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  function continueFromReport() {
    if (step.kind !== 'report') return;
    if (step.result.report.secrets_required.length > 0) {
      setStep({ kind: 'bindings', result: step.result });
    } else {
      onClose();
    }
  }

  async function handleBind(secrets: Record<string, string>) {
    if (step.kind !== 'bindings') return;
    setBindError(null);
    try {
      await bindSecrets.mutateAsync({ name: step.result.plugin.name, secrets });
      toast({
        title: t('plugins.install.boundTitle'),
        description: t('plugins.install.boundDesc', {
          count: Object.keys(secrets).length,
        }),
      });
      onClose();
    } catch (err) {
      setBindError(formatApiErrorDetail(err));
    }
  }

  const stepTitles: Record<WizardStep['kind'], string> = {
    source: t('plugins.install.stepSource'),
    choose: t('plugins.install.stepChoose'),
    installing: t('plugins.install.stepInstalling'),
    report: t('plugins.install.stepReport'),
    bindings: t('plugins.install.stepBindings'),
  };

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      onClick={onClose}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className="relative w-full max-w-lg rounded-lg p-5"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 p-1 rounded-full transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-primary)' }}
          aria-label={t('common.close')}
        >
          <X className="h-4 w-4" />
        </button>

        <h3
          id={titleId}
          className="text-lg font-semibold mb-1"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('plugins.install.title')}
        </h3>
        <p className="text-xs mb-4" style={{ color: 'var(--color-text-tertiary)' }}>
          {stepTitles[step.kind]}
        </p>

        {step.kind === 'source' && (
          <>
            <PluginSourceStep onSubmit={install} busy={false} />
            {error && (
              <div className="flex flex-col gap-1 mt-3">
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
            )}
          </>
        )}

        {step.kind === 'choose' && (
          <PluginChooseStep
            candidates={step.candidates}
            onPick={handlePick}
            busy={false}
          />
        )}

        {step.kind === 'installing' && (
          <div
            className="flex items-center justify-center gap-2 py-10 text-xs"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <Loader size={14} className="text-current" />
            {step.pct === null
              ? t('plugins.install.fetching')
              : t('plugins.install.uploading', { percent: step.pct })}
          </div>
        )}

        {step.kind === 'report' && (
          <PluginReportStep
            report={step.result.report}
            onUpgradeSse={handleUpgradeSse}
            upgrading={upgradeSse.isPending}
            onContinue={continueFromReport}
            continueLabel={
              step.result.report.secrets_required.length > 0
                ? t('plugins.install.continueToBindings')
                : t('common.done')
            }
          />
        )}

        {step.kind === 'bindings' && (
          <PluginBindingsStep
            required={step.result.report.secrets_required}
            blueprints={blueprintData?.blueprints ?? []}
            onBind={handleBind}
            binding={bindSecrets.isPending}
            error={bindError}
            onSkip={onClose}
          />
        )}
      </div>
    </div>
  );
}
