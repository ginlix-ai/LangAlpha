import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import {
  useInstallPluginFromUrl,
  useInstallPluginFromZip,
} from '@/hooks/usePlugins';
import {
  fatalDiagnostics,
  formatApiErrorDetail,
  multiplePluginCandidates,
  type PluginCandidate,
  type PluginDiagnostic,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api';
import { PluginDialog } from './PluginDialog';
import { PluginSourceStep, type PluginSource } from './PluginSourceStep';
import { PluginChooseStep } from './PluginChooseStep';
import { PluginOutcome } from './PluginOutcome';

/**
 * The install wizard: source → (choose →) installing → outcome. A fatal
 * package error (422) returns to the step the attempt was launched from,
 * carrying the backend's diagnostics verbatim; a multiple_plugins 422 (a
 * marketplace repo) becomes the chooser, which re-requests with the picked
 * subdirectory; everything survivable lands in `PluginOutcome`, the
 * report-then-credentials tail an update walks through too.
 */

/**
 * Each step carries what only it can show, including the input it is showing
 * it about. A failed attempt puts both back on the step it came from: an
 * error that outlives the source it refers to leaves the user reading a
 * complaint about an empty field.
 */
type WizardStep =
  | {
      kind: 'source';
      /** The attempted source, handed back so the retry is one click. */
      draft: PluginSource | null;
      error: string | null;
      diagnostics: PluginDiagnostic[];
    }
  | {
      kind: 'choose';
      source: PluginSource;
      candidates: PluginCandidate[];
      error: string | null;
    }
  | { kind: 'installing'; pct: number | null } // null = git (indeterminate)
  | { kind: 'outcome'; result: PluginInstallResponse };

export function PluginInstallWizard({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const [step, setStep] = useState<WizardStep>({
    kind: 'source',
    draft: null,
    error: null,
    diagnostics: [],
  });

  const installZip = useInstallPluginFromZip();
  const installUrl = useInstallPluginFromUrl();

  async function install(source: PluginSource, subdir?: string) {
    const from = step;
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
      setStep({ kind: 'outcome', result });
    } catch (err) {
      const candidates = multiplePluginCandidates(err);
      if (candidates) {
        setStep({ kind: 'choose', source, candidates, error: null });
        return;
      }
      const error = formatApiErrorDetail(err);
      // A pick that failed goes back to its own list, which is still the
      // right question to ask: the repo resolved, one entry in it did not.
      if (from.kind === 'choose') {
        setStep({ ...from, error });
        return;
      }
      setStep({
        kind: 'source',
        draft: source,
        error,
        diagnostics: fatalDiagnostics(err),
      });
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

  /** A source-step refusal replaces whatever the last install attempt left:
   *  the diagnostics belong to that attempt, not to the new input. */
  function setSourceError(error: string | null) {
    setStep((s) => (s.kind === 'source' ? { ...s, error, diagnostics: [] } : s));
  }

  if (step.kind === 'outcome') {
    return (
      <PluginOutcome
        response={step.result}
        title={t('plugins.install.title')}
        onDone={onClose}
      />
    );
  }

  const stepTitles: Record<Exclude<WizardStep['kind'], 'outcome'>, string> = {
    source: t('plugins.install.stepSource'),
    choose: t('plugins.install.stepChoose'),
    installing: t('plugins.install.stepInstalling'),
  };

  return (
    <PluginDialog
      title={t('plugins.install.title')}
      subtitle={stepTitles[step.kind]}
      // Closing mid-install does not cancel it: the request is already with
      // the server, and the report it answers with is the only statement of
      // which components landed, which credentials are still missing, and
      // which sse entries can be upgraded. Dismissing here installed the
      // plugin and threw all of that away, leaving a plugin that silently
      // does nothing until the user thinks to run an update.
      dismissable={step.kind !== 'installing'}
      onClose={onClose}
    >
      {step.kind === 'source' && (
        <PluginSourceStep
          initial={step.draft}
          onSubmit={install}
          onError={setSourceError}
          error={step.error}
          diagnostics={step.diagnostics}
        />
      )}

      {step.kind === 'choose' && (
        <PluginChooseStep
          candidates={step.candidates}
          onPick={handlePick}
          onBack={() =>
            setStep({
              kind: 'source',
              draft: step.source,
              error: null,
              diagnostics: [],
            })
          }
          error={step.error}
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
    </PluginDialog>
  );
}
