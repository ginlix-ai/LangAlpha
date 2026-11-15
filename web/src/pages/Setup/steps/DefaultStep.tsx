import { useEffect, useId, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { Navigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';
import { motion, AnimatePresence, useIsPresent } from '@/lib/framer';
import { DURATION, TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { WizardStepHeader } from '@/components/ui/wizard';
import { NameTag, PayerBadge } from '@/components/model/PayerBadge';
import { ModelSelector } from '@/components/model/ModelSelector';
import { FallbackModelsPicker } from '@/components/model/FallbackModelsPicker';
import { useModelSlotCopy } from '@/components/model/useModelSlotCopy';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { isPlatformMode } from '@/config/hostMode';
import { isDesktopShell } from '@/lib/desktop';
import { useAllModels } from '@/hooks/useAllModels';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { SERVER_KEY, useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useAccount } from '@/hooks/useAccount';
import { useUser } from '@/hooks/useUser';
import { allows, RULE } from '@/lib/accountRules';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { BACKGROUND_AUTO, backgroundDefaultChoice, modelPrefs, splitPreferenceWrite } from '@/lib/modelPreferences';
import { SetupFooter } from '../SetupFooter';
import { useAnswerFlow } from '../answers/flow';
import { defaultChoices, type DefaultChoices } from '../answers/defaultChoices';
import { AnswersStrip, PickCard, StepError, StepLoadFailed, StepLoading } from '../answers/parts';
import { SETUP_STEP_PATHS } from '../answers/setupSteps';
import { ProgressList, ProgressStep } from '../answers/ProgressSteps';

/**
 * The models every conversation and its Background work run on, from what the
 * answers connected. It opens on the setup it recommends, from the manifest's
 * ratings, with the reason for each pick; Customize holds the Background,
 * fetch, compaction and fallback models Settings also sets. Continue sets the
 * user up in steps it shows as they resolve (`SETTING_UP`), then hands over
 * to /welcome. With no model to pick, it finishes setup on the deployment's
 * defaults, so the step never holds the user.
 */
export default function DefaultStep() {
  const flow = useAnswerFlow('default');
  const { user } = useUser();
  const all = useAllModels();
  const { models, slotModels, payers, metadata, customModels, systemDefaults, isLoading } = all;
  const { connectionOf } = useProviderCatalog();
  const { preferences, isLoaded, unread, retry } = usePreferences();
  const { providers, byokEnabled } = useConfiguredProviders();
  const { policy } = useAccount();
  const behind = stepBehind(flow);
  // Once shown, the form stays: a later refetch flipping a read back to
  // loading would otherwise unmount it, and with it a setup under way.
  const ready = !flow.isLoading && !isLoading && !all.unread && isLoaded && !behind;
  const [shown, setShown] = useState(false);
  if (ready && !shown) setShown(true);

  if (!shown && !ready) {
    if (behind) return <Navigate to={SETUP_STEP_PATHS[behind]} replace />;
    if (unread) return <StepLoadFailed onRetry={retry} />;
    // An unread catalog offers no model, and Continue would finish on none.
    if (all.unread) return <StepLoadFailed onRetry={flow.retryFacts} />;
    return <StepLoading />;
  }
  if (!flow.answers) return <Navigate to="/setup" replace />;

  const saved = modelPrefs(preferences);
  const choices = defaultChoices({
    models: flow.answers.models,
    offered: models,
    slotModels: Object.values(slotModels).flatMap((p) => p.models ?? []),
    payers,
    // Not a connection the account's rules keep off, which no turn runs on.
    ownProviders: providers.filter((p) => p.masked_key !== SERVER_KEY && p.state !== 'needs_reauth'
      && (p.access_type === 'oauth' ? allows(policy, RULE.ownSubscriptions)
        : allows(policy, RULE.ownApiKeys) && (!isPlatformMode || byokEnabled))).map((p) => p.provider),
    metadata,
    customModels,
    connectionOf,
    systemDefault: systemDefaults?.default_model ?? null,
    systemBackground: systemDefaults?.background_model ?? null,
    preferred: saved.preferred_model ?? null,
  });
  // Mounted once the preferences are read, so the picks are seeded from what
  // is saved and never from an unread row; keyed by the account, whose drafts
  // and setup under way they are.
  return <DefaultForm key={user?.user_id ?? ''} flow={flow} choices={choices} saved={saved} all={all} />;
}

type Flow = ReturnType<typeof useAnswerFlow>;

/** A step before this one whose outcome is gone (a computer removed, a token
 *  taken off, from here or another device), which is where setup resumes, as
 *  a visit to /setup would go. */
const stepBehind = (flow: Flow) => (flow.next === 'computer' || flow.next === 'model' ? flow.next : null);

/** What Continue does, in order: each is real work, ticked off as it lands. */
const SETTING_UP = ['models', 'home', 'finish'] as const;
/** How long finishing waits on Home, which /welcome reads again anyway. */
const HOME_WAIT_MS = 8_000;
type SetupPhase = (typeof SETTING_UP)[number] | 'done';
type Saved = ReturnType<typeof modelPrefs>;

function DefaultForm({ flow, choices, saved, all }: { flow: Flow; choices: DefaultChoices; saved: Saved; all: ReturnType<typeof useAllModels> }) {
  const { t } = useTranslation();
  const copy = useModelSlotCopy();
  const { slotModels, payers, metadata, systemDefaults, cannotRun } = all;
  const update = useUpdatePreferences();
  const headingId = useId();
  const groupName = useId();
  const { recommended } = choices;
  const [primaryPick, setPrimary] = useState(choices.initial);
  // Background as Settings holds it: a model, or Auto (the deployment's, which
  // `background_follows` keeps beside a saved primary).
  const systemBackground = systemDefaults?.background_model ?? null;
  const recommendedBackground = recommended.background ?? (systemBackground ? BACKGROUND_AUTO : '');
  // A saved Background confirmed not to run any more (retired, or a plan
  // that lost it) starts on the recommended one instead; one only unlisted
  // while a read failed or loads is kept.
  const [seededBackground] = useState(() => {
    const kept = backgroundDefaultChoice(saved);
    const runs = kept === BACKGROUND_AUTO || !cannotRun(kept);
    return (runs && kept) || recommendedBackground;
  });
  const [backgroundPick, setBackground] = useState(seededBackground);
  // A refetch can take a pick away (a plan that lost it, a key removed): one
  // no longer offered goes back to the recommendation, as does none where the
  // form opened on no model and a refetch offers some. The saved Background
  // the form opened on keeps the rule it opened with: it goes only once it
  // is confirmed not to run.
  const settled = !all.isLoading && !all.unread;
  const backgroundOffered = Object.values(slotModels).flatMap((p) => p.models ?? []);
  const primary = settled && (!primaryPick || !choices.list.includes(primaryPick)) ? recommended.primary : primaryPick;
  const background = backgroundPick && backgroundPick !== BACKGROUND_AUTO && (cannotRun(backgroundPick)
    || (settled && backgroundPick !== seededBackground && !backgroundOffered.includes(backgroundPick)))
    ? recommendedBackground : backgroundPick;
  // Routing seeded from what is saved, else what the deployment runs, and
  // written only when changed here: an unset one keeps following the
  // deployment.
  const [routing] = useState(() => ({
    fetch: saved.fetch_model ?? '',
    compaction: saved.compaction_model ?? '',
    fallbacks: saved.fallback_models ?? systemDefaults?.fallback_models ?? [],
  }));
  const [fetchModel, setFetchModel] = useState(routing.fetch);
  const [compactionModel, setCompactionModel] = useState(routing.compaction);
  const [fallbackModels, setFallbackModels] = useState<string[]>(routing.fallbacks);
  // A routing draft a refetch took away is not written, as the picks above;
  // one saved before keeps its rule and goes only once confirmed not to run.
  const routes = (m: string, saved: boolean) => !cannotRun(m) && (saved || !settled || backgroundOffered.includes(m));
  const fetchDraft = !fetchModel || routes(fetchModel, false) ? fetchModel : routing.fetch;
  const compactionDraft = !compactionModel || routes(compactionModel, false) ? compactionModel : routing.compaction;
  const fallbackDraft = fallbackModels.filter((m) => routes(m, routing.fallbacks.includes(m)));
  const [customizing, setCustomizing] = useState(false);
  const [phase, setPhase] = useState<SetupPhase | null>(null);
  const queryClient = useQueryClient();
  // Still the step in hand: mounted, and not a step the wizard is animating
  // out after the user left it.
  const present = useIsPresent();
  const live = useRef(true);
  useEffect(() => {
    live.current = present;
    return () => {
      live.current = false;
    };
  }, [present]);
  // Focus follows the swap: to the panel as it opens, back to the form when
  // a failure returns it.
  const settingUpRef = useRef<HTMLDivElement>(null);
  const formRef = useRef<HTMLDivElement>(null);
  const settingUp = phase !== null;
  const wasSettingUp = useRef(false);
  useEffect(() => {
    if (settingUp) settingUpRef.current?.focus();
    else if (wasSettingUp.current) formRef.current?.focus();
    wasSettingUp.current = settingUp;
  }, [settingUp]);
  // Read again as setup finishes, since a step behind can turn up mid-run.
  const behind = stepBehind(flow);
  const behindNow = useRef(behind);
  useEffect(() => {
    behindNow.current = behind;
  }, [behind]);

  const name = (m: string) => getModelDisplayName(m, metadata);
  const deploymentBackground = systemBackground ? t('settings.autoModel', { model: name(systemBackground) }) : undefined;
  const onRecommended = primary === recommended.primary && background === recommendedBackground;
  // Self-hosted runs it on the token the server saved, wherever the turn is.
  const target = flow.tokenTarget;
  // Setup finished, or taken by the other surface, is no longer this form's
  // to save; a key or plan answer saves no setup without a model of its own.
  const setupOn = flow.progress?.setup_on ?? null;
  const held = !!flow.progress?.setup_completed_at || (setupOn !== null && setupOn !== (isDesktopShell() ? 'app' : 'browser'))
    || (!primary && flow.answers?.models === 'subscription_or_key');
  const where = flow.answers?.models !== 'claude_code' || !isPlatformMode ? null
    : target?.kind === 'computer'
      ? t('setup.answers.default.claudeCodeOn', { name: target.name })
      : t('setup.answers.default.claudeCodeOnCloud');

  // Every step checks the form is still up: leaving setup mid-way (Back,
  // Exit) ends the run, and no later write or navigation lands for it.
  const setUp = async () => {
    void import('@/pages/Onboarding/welcome/Welcome');
    setPhase('models');
    if (primary) {
      const auto = background === BACKGROUND_AUTO;
      const sameList = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((m, i) => m === b[i]);
      try {
        // A blank routing model is left unset, so the server follows the
        // Background or conversation model at turn time, as Settings does.
        await update.mutateAsync(splitPreferenceWrite({
          preferred_model: primary,
          preferred_background_model: auto ? null : background || null,
          background_follows: auto ? 'deployment' : null,
          ...(fetchDraft !== routing.fetch ? { fetch_model: fetchDraft || null } : {}),
          ...(compactionDraft !== routing.compaction ? { compaction_model: compactionDraft || null } : {}),
          ...(sameList(fallbackDraft, routing.fallbacks) ? {} : { fallback_models: fallbackDraft }),
        }));
      } catch {
        if (live.current) setPhase(null);
        return;
      }
    }
    if (!live.current) return;
    // Home, which /welcome hands its first turn to; one that can't be read
    // in time is read again there.
    setPhase('home');
    await Promise.race([
      queryClient.ensureQueryData(homeWorkspaceQuery(queryClient)).catch(() => {}),
      new Promise((resolve) => setTimeout(resolve, HOME_WAIT_MS)),
    ]);
    if (!live.current) return;
    if (behindNow.current) {
      setPhase(null);
      return;
    }
    setPhase('finish');
    const finished = await flow.finish();
    if (!live.current) return;
    if (!finished) {
      setPhase(null);
      return;
    }
    // Long enough for the last mark to land before the page goes.
    setPhase('done');
    await new Promise((resolve) => setTimeout(resolve, DURATION.reveal * 1000));
    if (live.current) flow.toWelcome();
  };

  if (behind && !phase) return <Navigate to={SETUP_STEP_PATHS[behind]} replace />;

  if (phase) {
    const at = phase === 'done' ? SETTING_UP.length : SETTING_UP.indexOf(phase);
    return (
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={TWEEN.enter} data-testid="setup-setting-up">
        <div ref={settingUpRef} tabIndex={-1} className="outline-none">
          <WizardStepHeader titleId={headingId} title={t('setup.answers.default.settingUp.title')} />
        </div>
        <p role="status" className="sr-only">
          {phase === 'done' ? t('setup.answers.default.settingUp.done') : t(`setup.answers.default.settingUp.${phase}`)}
        </p>
        <div>
          <ProgressList>
            {SETTING_UP.filter((step) => step !== 'models' || primary).map((step, i) => {
              const index = SETTING_UP.indexOf(step);
              return (
                <ProgressStep
                  key={step}
                  n={i + 1}
                  state={index < at ? 'done' : index === at ? 'active' : 'todo'}
                  title={t(`setup.answers.default.settingUp.${step}`)}
                />
              );
            })}
          </ProgressList>
        </div>
      </motion.div>
    );
  }

  return (
    <div ref={formRef} tabIndex={-1} className="outline-none">
      {flow.first && flow.answers && <AnswersStrip answers={flow.answers} />}
      <WizardStepHeader titleId={headingId} title={t('setup.answers.default.title')} helper={t('setup.answers.default.lead')} />
      {choices.list.length === 0 ? (
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('setup.answers.default.none')}</p>
      ) : (
        <div className="flex flex-col gap-6">
          {recommended.primary && (
            <section aria-labelledby={headingId + '-rec'} className="shell-box flex flex-col gap-3 px-4 py-3.5">
              <div className="flex items-center justify-between gap-3">
                <h3 id={headingId + '-rec'} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t('setup.answers.default.recommendedSetup')}
                </h3>
                {!onRecommended && (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setPrimary(recommended.primary);
                      setBackground(recommendedBackground);
                    }}
                  >
                    {t('setup.answers.default.useRecommended')}
                  </Button>
                )}
              </div>
              <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-xs">
                <dt style={{ color: 'var(--color-text-tertiary)' }}>{copy.default.label}</dt>
                <dd className="flex flex-col gap-0.5">
                  <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{name(recommended.primary)}</span>
                  {recommended.primaryWhy && (
                    <span style={{ color: 'var(--color-text-secondary)' }}>{t(`setup.answers.default.why.${recommended.primaryWhy}`)}</span>
                  )}
                </dd>
                {(recommended.background || deploymentBackground) && (
                  <>
                    <dt style={{ color: 'var(--color-text-tertiary)' }}>{copy.background.label}</dt>
                    <dd className="flex flex-col gap-0.5">
                      <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
                        {recommended.background ? name(recommended.background) : deploymentBackground}
                      </span>
                      {recommended.backgroundWhy && (
                        <span style={{ color: 'var(--color-text-secondary)' }}>
                          {t(`setup.answers.default.why.${recommended.backgroundWhy}Background`, {
                            model: systemBackground ? name(systemBackground) : '',
                          })}
                        </span>
                      )}
                    </dd>
                  </>
                )}
              </dl>
            </section>
          )}

          <div role="radiogroup" aria-labelledby={headingId} className="flex flex-col gap-2">
            {choices.list.map((m) => (
              <PickCard key={m} name={groupName} value={m} checked={m === primary} onSelect={setPrimary}>
                <span className="flex min-w-0 flex-1 flex-col gap-1">
                  <span className="flex flex-wrap items-center gap-2 text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {name(m)}
                    {recommended.primary === m && <NameTag>{t('setup.answers.default.recommended')}</NameTag>}
                  </span>
                  <Ratings meta={metadata[m]} />
                  {m === primary && where && <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{where}</span>}
                </span>
                <PayerBadge payer={payers[m]} />
              </PickCard>
            ))}
          </div>

          <div>
            <button
              type="button"
              onClick={() => setCustomizing((v) => !v)}
              className="tap-target inline-flex items-center gap-1 text-xs font-medium transition-colors"
              style={{ color: 'var(--color-text-tertiary)' }}
              aria-expanded={customizing}
            >
              <ChevronRight className={cn('h-3 w-3 transition-transform', customizing && 'rotate-90')} />
              {t('setup.answers.default.customize')}
            </button>
            <AnimatePresence initial={false}>
              {customizing && (
                <motion.div
                  initial={{ opacity: 0, y: -4 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, transition: TWEEN.exit }}
                  transition={TWEEN.quick}
                >
                  <div className="flex flex-col gap-6 pt-4">
                    <ModelSelector
                      label={copy.background.label}
                      description={copy.background.description}
                      value={background}
                      onChange={setBackground}
                      models={slotModels}
                      missingLabel={name}
                      placeholder={copy.background.placeholder}
                      autoOption={deploymentBackground ? { value: BACKGROUND_AUTO, label: deploymentBackground } : undefined}
                      payers={payers}
                      metadata={metadata}
                    />
                    <ModelSelector
                      label={t('settings.modelTuning.fetchModel')}
                      description={t('settings.modelTuning.fetchModelDesc')}
                      value={fetchModel}
                      onChange={setFetchModel}
                      models={slotModels}
                      placeholder={copy.defaultsToBackground}
                      payers={payers}
                      metadata={metadata}
                    />
                    <ModelSelector
                      label={t('settings.modelTuning.compactionModel')}
                      description={t('settings.modelTuning.compactionModelDesc')}
                      value={compactionModel}
                      onChange={setCompactionModel}
                      models={slotModels}
                      placeholder={copy.defaultsToConversation}
                      payers={payers}
                      metadata={metadata}
                    />
                    <FallbackModelsPicker
                      selected={fallbackModels}
                      onChange={setFallbackModels}
                      models={slotModels}
                      metadata={metadata}
                      payers={payers}
                    />
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </div>
      )}
      {(update.isError || flow.finishFailed) && <StepError>{t('setup.answers.default.saveFailed')}</StepError>}
      <SetupFooter
        back={flow.back ? { label: t('setup.back'), onClick: flow.back } : null}
        primary={{
          label: t('setup.continue'),
          onClick: () => void setUp(),
          disabled: update.isPending || flow.finishing || held,
          busy: update.isPending || flow.finishing,
        }}
      />
    </div>
  );
}

const RATINGS = ['intelligence', 'speed'] as const;

/** The manifest's 1 to 5 ratings, as dots; nothing for a model it doesn't rate. */
function Ratings({ meta }: { meta: ModelMetadataEntry | undefined }) {
  const { t } = useTranslation();
  const rated = RATINGS.filter((key) => typeof meta?.[key] === 'number');
  if (rated.length === 0) return null;
  return (
    <span className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {rated.map((key) => {
        const value = meta?.[key] ?? 0;
        const label = t(`setup.answers.default.${key}`);
        return (
          <span key={key} role="img" aria-label={t('setup.answers.default.rating', { label, value })} className="inline-flex items-center gap-1.5">
            <span aria-hidden>{label}</span>
            <span aria-hidden className="inline-flex gap-0.5">
              {[1, 2, 3, 4, 5].map((i) => (
                <span
                  key={i}
                  className="h-1.5 w-1.5 rounded-full"
                  style={{ backgroundColor: i <= value ? 'var(--color-text-primary)' : 'var(--color-border-default)' }}
                />
              ))}
            </span>
          </span>
        );
      })}
    </span>
  );
}
