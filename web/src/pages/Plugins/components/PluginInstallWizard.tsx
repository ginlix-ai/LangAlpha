import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { Loader } from '@/components/ui/loader';
import { useBfcacheRestore } from '@/hooks/useBfcacheRestore';
import { useNow } from '@/hooks/useNow';
import { cn } from '@/lib/utils';
import {
  useInstallPluginFromUrl,
  useInstallPluginFromZip,
} from '@/hooks/usePlugins';
import type { PluginCandidate } from '@/pages/ChatAgent/utils/api';
import { refusalSaying, type Refusal } from '@/pages/ChatAgent/utils/api/refusal';
import { markJustAdded } from '../hooks/useJustAdded';
import { useMcpOauthActions } from '../hooks/useMcpOauthActions';
import { detailHref } from '../utils/detailParam';
import { nextKeysAsk, readInstallError, type InstallError, type KeysAsk } from '../utils/installRefusal';
import { pluginSignInRequest } from '../utils/pluginSignIn';
import { PluginDialog } from './PluginDialog';
import { PluginKeysStep } from './PluginKeysStep';
import { PluginSourceStep, type PluginSource } from './PluginSourceStep';
import { PluginChooseStep } from './PluginChooseStep';
import { usePluginOutcome, type PluginOutcomeOf } from './PluginOutcome';

/**
 * The install wizard: source → (choose →) (keys →) installing → outcome.
 *
 * Nothing installs until every server the plugin carries has passed its
 * check, so each refusal is a step back with nothing to undo. A fatal package
 * error (422) returns to the step the attempt was launched from, carrying the
 * backend's diagnostics verbatim; a multiple_plugins 422 (a marketplace repo)
 * becomes the chooser, which re-requests with the picked subdirectory; a
 * plugin that declares keys the vault does not hold asks for them and
 * re-requests with them; a server that failed its check says why on the step
 * that sent it; and a server that signs in hands the whole install to its
 * sign-in, whose callback installs it.
 *
 * Every step, the outcome included, renders into one dialog: a second dialog
 * for the outcome would replay the entrance animation over the first.
 */

// Where a sign-in started here comes back to: the installed plugin's card.
const RETURN_TO = '/plugins?tab=plugins';

// The install request is one round trip that reports no stages of its own
// past the upload, so the phases after it are the client's estimate: a public
// repo usually downloads in a couple of seconds and a check takes a few more.
// The last phase is the one on screen until the answer arrives however long
// that takes, which is why it is the general one.
const DOWNLOAD_MS = 2500;
const CHECK_MS = 4000;

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
      refusal: Refusal | null;
      /** Why the wizard opened here, when it was not the user's own start. */
      note: string | null;
    }
  | {
      kind: 'choose';
      source: PluginSource;
      candidates: PluginCandidate[];
      refusal: Refusal | null;
    }
  | {
      kind: 'keys';
      source: PluginSource;
      subdir?: string;
      ask: KeysAsk;
      /** Where Back goes: the list the plugin was picked from, or the source. */
      back: WizardStep;
    }
  | InstallingStep
  /** On the way to the vendor; `back` is where a sign-in that never
   *  started (or whose page never left) returns the user, with what they had
   *  entered. */
  | { kind: 'signing-in'; back: WizardStep; minHeight: number | null }
  | { kind: 'outcome'; outcome: PluginOutcomeOf };

interface InstallingStep {
  kind: 'installing';
  /** A zip reports its upload; a git URL is fetched by the server. */
  transport: 'zip' | 'git';
  pct: number;
  startedAt: number;
  /** When the upload reached 100%, from which the check is timed. */
  uploadedAt: number | null;
  /** Whether the plugin is known to carry connectors, which only an earlier
   *  attempt can tell: keys were asked for, or a check refused one. */
  hasConnectors: boolean;
  /** The step's height as it left, held so the dialog does not shrink to a
   *  spinner and grow again for the report. */
  minHeight: number | null;
}

const sourceStep = (
  draft: PluginSource | null,
  extra: { error?: string | null; note?: string | null } = {},
): WizardStep => ({
  kind: 'source',
  draft,
  refusal: extra.error ? refusalSaying(extra.error) : null,
  note: extra.note ?? null,
});

/** The step an attempt was sent from, as it stood before the attempt said
 *  anything about it. */
function leftFrom(from: WizardStep, source: PluginSource): WizardStep {
  if (from.kind === 'keys') return { ...from, ask: { ...from.ask, refusal: null } };
  if (from.kind === 'choose') return { ...from, refusal: null };
  return sourceStep(source);
}

export function PluginInstallWizard({
  onClose,
  initial,
}: {
  onClose: () => void;
  /** Open on the source step with this source in it: an install handed over
   *  from somewhere else. `error` says why it did not go through there,
   *  `note` says neutrally why the wizard is open. */
  initial?: { source: PluginSource | null; error?: string | null; note?: string | null };
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [step, setStep] = useState<WizardStep>(() =>
    sourceStep(initial?.source ?? null, { error: initial?.error, note: initial?.note }),
  );
  // The keys typed so far, outside the step so a refused attempt, or a Back
  // to change the source, hands them back instead of making the user paste
  // them again.
  const [keys, setKeys] = useState<Record<string, string>>({});
  const bodyRef = useRef<HTMLDivElement>(null);

  const installZip = useInstallPluginFromZip();
  const installUrl = useInstallPluginFromUrl();
  const oauth = useMcpOauthActions({ returnTo: RETURN_TO });
  const now = useNow(500, step.kind === 'installing');

  // A bfcache restore from the vendor's page brings back the page as it was
  // left, on its way out. That sign-in is over; the step it left from is
  // where the user can try again.
  useBfcacheRestore(() => setStep((s) => (s.kind === 'signing-in' ? s.back : s)));

  /**
   * The step an attempt that left from `from` comes back to, short of a
   * sign-in. Keys go to the keys step, which keeps any key a connector asked
   * for before; a pick that failed goes back to its own list, which is still
   * the right question (the repo resolved, one entry in it did not); anything
   * else goes back to the source, saying why.
   */
  function stepAfter(
    from: WizardStep,
    source: PluginSource,
    subdir: string | undefined,
    error: InstallError,
  ): WizardStep {
    if (error.kind === 'choose') {
      return { kind: 'choose', source, candidates: error.candidates, refusal: null };
    }
    const ask = nextKeysAsk(from.kind === 'keys' ? from.ask : null, error);
    if (ask) {
      // Back from the keys goes past any earlier keys step, to the list the
      // plugin was picked from, or to the source that was sent.
      const back = from.kind === 'keys' ? from.back : leftFrom(from, source);
      return { kind: 'keys', source, subdir, ask, back };
    }
    if (from.kind === 'choose') return { ...from, refusal: error.refusal };
    return { kind: 'source', draft: source, refusal: error.refusal, note: null };
  }

  async function install(
    source: PluginSource,
    subdir?: string,
    secrets?: Record<string, string>,
  ) {
    const from = step;
    const startedAt = Date.now();
    setStep({
      kind: 'installing',
      transport: 'file' in source ? 'zip' : 'git',
      pct: 0,
      startedAt,
      uploadedAt: null,
      hasConnectors:
        from.kind === 'keys' ||
        ((from.kind === 'source' || from.kind === 'choose') && !!from.refusal?.failures?.length),
      minHeight: bodyRef.current?.offsetHeight ?? null,
    });
    try {
      const result =
        'file' in source
          ? await installZip.mutateAsync({
              file: source.file,
              subdir,
              secrets,
              onProgress: (pct) => {
                const at = Date.now();
                setStep((s) =>
                  s.kind === 'installing'
                    ? { ...s, pct, uploadedAt: pct >= 100 ? (s.uploadedAt ?? at) : null }
                    : s,
                );
              },
            })
          : await installUrl.mutateAsync({ sourceUrl: source.url, subdir, secrets });
      setKeys({});
      markJustAdded('plugin', result.plugin.name);
      setStep({ kind: 'outcome', outcome: { response: result, savedKeys: Object.keys(secrets ?? {}) } });
    } catch (err) {
      const error = readInstallError(err);
      const back = leftFrom(from, source);
      const request =
        error.kind === 'signIn' &&
        pluginSignInRequest(error.servers, { source, subdir, secrets }, {
          onNotStarted: () => setStep(back),
          // Refused at the start (the keys failed a re-check, the name was
          // taken meanwhile): said on the step that can change it.
          onRefused: (startErr) => setStep(stepAfter(from, source, subdir, readInstallError(startErr))),
          // Gone on in the system browser (the desktop app), where this dialog
          // cannot follow: the install travels with the sign-in, and its
          // landing says how it went.
          onHandedOff: onClose,
        });
      if (request) {
        setStep((s) => ({
          kind: 'signing-in',
          back,
          minHeight: s.kind === 'installing' ? s.minHeight : null,
        }));
        oauth.connect(request);
        return;
      }
      setStep(stepAfter(from, source, subdir, error));
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
    setStep((s) =>
      s.kind === 'source'
        ? { ...s, refusal: error ? refusalSaying(error) : null, note: null }
        : s,
    );
  }

  const outcome = usePluginOutcome(step.kind === 'outcome' ? step.outcome : null, {
    onDone: onClose,
    onOpenPlugin: (name) => {
      onClose();
      navigate(detailHref({ kind: 'plugin', name }));
    },
  });

  function phaseLabel(s: InstallingStep): string {
    if (s.transport === 'zip' && s.uploadedAt === null) {
      return t('plugins.install.uploading', { percent: s.pct });
    }
    // A git source spends its first seconds downloading; a zip is already
    // there once its upload completes.
    const download = s.transport === 'git' ? DOWNLOAD_MS : 0;
    const elapsed = Math.max(0, now - (s.uploadedAt ?? s.startedAt));
    if (elapsed < download) return t('plugins.install.downloading');
    if (elapsed < download + CHECK_MS) {
      return s.hasConnectors ? t('plugins.install.checking') : t('plugins.install.checkingPlugin');
    }
    return t('plugins.install.installingPhase');
  }

  // A sign-in that asks first (a brokerage's capabilities) puts its consent
  // over the step it left from, which is what the user was looking at, rather
  // than over a spinner for a page that is not opening yet.
  const shown = step.kind === 'signing-in' && oauth.pendingConfirm ? step.back : step;

  const stepTitles: Record<WizardStep['kind'], string> = {
    source: t('plugins.install.stepSource'),
    choose: t('plugins.install.stepChoose'),
    keys: t('plugins.install.stepKeys'),
    installing: t('plugins.install.stepInstalling'),
    'signing-in': t('plugins.install.stepSigningIn'),
    outcome: t('plugins.install.stepReport'),
  };
  const working = { color: 'var(--color-text-tertiary)' };

  return (
    <>
      <PluginDialog
        title={t('plugins.install.title')}
        subtitle={stepTitles[shown.kind]}
        // Closing mid-install does not cancel it: the request is already with
        // the server, and the report it answers with is the only statement of
        // which components landed and which sse entries can be upgraded.
        dismissable={step.kind !== 'installing' && step.kind !== 'signing-in' && !outcome?.upgrading}
        onClose={onClose}
        footer={outcome?.footer}
      >
        <div ref={bodyRef}>
          {shown.kind === 'source' && (
            <PluginSourceStep
              initial={shown.draft}
              onSubmit={install}
              onError={setSourceError}
              refusal={shown.refusal}
              note={shown.note}
              onBrowseDiscover={onClose}
            />
          )}

          {shown.kind === 'choose' && (
            <PluginChooseStep
              candidates={shown.candidates}
              onPick={handlePick}
              onBack={() => setStep(sourceStep(shown.source))}
              refusal={shown.refusal}
            />
          )}

          {shown.kind === 'keys' && (
            <PluginKeysStep
              ask={shown.ask}
              values={keys}
              onChange={setKeys}
              onSubmit={(secrets) => void install(shown.source, shown.subdir, secrets)}
              onBack={() => setStep(shown.back)}
              submitting={false}
              submitLabel={t('plugins.install.keysInstall')}
            />
          )}

          {(shown.kind === 'installing' || shown.kind === 'signing-in') && (
            <div
              className={cn(
                'flex flex-col items-center justify-center gap-1.5 text-xs',
                shown.minHeight === null && 'py-10',
              )}
              style={{ ...working, minHeight: shown.minHeight ?? undefined }}
              data-testid="plugin-install-progress"
            >
              <span className="flex items-center gap-2">
                <Loader size={14} className="text-current" />
                {shown.kind === 'signing-in'
                  ? t('plugins.install.openingSignIn')
                  : phaseLabel(shown)}
              </span>
              {shown.kind === 'installing' && (
                <span className="text-[0.6875rem]">{t('plugins.install.takesTime')}</span>
              )}
            </div>
          )}

          {outcome?.body}
        </div>
      </PluginDialog>
    </>
  );
}
