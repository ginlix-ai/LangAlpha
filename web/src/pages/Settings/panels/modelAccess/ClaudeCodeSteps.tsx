import { useEffect, useState, type ReactNode } from 'react';
import { Check } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { ClaudeCodeTokenSheet } from '@/components/modelAccess/ClaudeCodeToken';
import { StatusLabel } from '@/components/modelAccess/parts';
import { SAVED_TOKEN } from '@/components/modelAccess/tokenTarget';
import { useClaudeCodeSetup, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import type { ClaudeCodeSteps as Steps } from '@/hooks/useClaudeCodeSteps';
import { useNow } from '@/hooks/useNow';
import { useTurnOnClaudeCode } from '@/hooks/useTurnOnClaudeCode';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_MIN_TIER } from '@/lib/claudeCode';
import { openConsolePage } from '@/lib/desktop';
import { planPageUrl } from '@/lib/planTrip';
import { userSessionStorage } from '@/lib/userStorage';
import { cn } from '@/lib/utils';
import { TierRadioGroup, tierLabel } from '@/pages/ChatAgent/components/tierUi';
import { useTierPick } from '@/pages/ChatAgent/components/tierPick';
import { specErrorFiles, specErrorFrom, type SpecError } from '@/pages/ChatAgent/components/specErrors';
import { effectiveTier, specChangeLabel } from '@/pages/ChatAgent/components/specChangeUi';
import { useLocale } from '@/hooks/useLocale';
import { PLAN_CHANGED_PARAM, plansOpenInPlace } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { useComputerActions } from '@/pages/ChatAgent/hooks/useComputerActions';
import { useComputerProviders } from '@/pages/ChatAgent/hooks/useComputers';
import { readPlanAfterChange, useTierQuota } from '@/pages/ChatAgent/hooks/useTierQuota';
import type { Computer, ComputerSpecChange, ResourceTier } from '@/types/api';

const K = 'modelAccess.claudeCode.steps';
/** The plan the user left for the plan page on, kept for the tab so the way
 *  back can tell a plan change from a plan the user already had. */
const PLAN_BEFORE_KEY = 'claudeCode:planBeforeTrip';

/** Where the plan page sends the user back to: this sheet, which reads the
 *  plan again and opens the step the new plan unlocks. */
const PLAN_BACK = `${CLAUDE_CODE_CHOICE_URL}&${PLAN_CHANGED_PARAM}=1`;

/** How long a change to Medium usually takes, for a bar that fills toward
 *  it and stops short of the end until the change lands. */
const TYPICAL_CHANGE_MS = 5 * 60_000;

type StepState = 'done' | 'active' | 'running' | 'locked';

/**
 * One step: done, a check and its title, with a line under it where there
 * is one; the step to take now, open on its own card; one that waits on an
 * earlier step, its title and what it will ask, quiet.
 */
function Step({ n, state, title, detail, action, children }: {
  n: number;
  state: StepState;
  title: string;
  detail?: ReactNode;
  action?: ReactNode;
  children?: ReactNode;
}) {
  const { t } = useTranslation();
  const open = state === 'active' || state === 'running';
  return (
    <li
      aria-current={open ? 'step' : undefined}
      className={cn('flex gap-3 rounded-lg border px-4 py-3', open && 'py-4')}
      style={{
        borderColor: open ? 'var(--color-border-strong)' : 'var(--color-border-muted)',
        background: open ? 'var(--color-bg-card)' : undefined,
      }}
    >
      {state === 'done' ? (
        <span className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full" style={{ background: 'var(--color-success)' }}>
          <Check aria-hidden="true" className="h-3 w-3" style={{ color: 'var(--color-text-on-accent)' }} strokeWidth={3} />
          <span className="sr-only">{t(`${K}.done`)}</span>
        </span>
      ) : (
        <span
          aria-hidden="true"
          className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border text-xs font-medium"
          style={{
            borderColor: state === 'running' ? 'var(--color-accent-primary)' : open ? 'var(--color-text-primary)' : 'var(--color-border-default)',
            color: state === 'running' ? 'var(--color-accent-primary)' : open ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {n}
        </span>
      )}
      <div className="flex min-w-0 flex-1 flex-col gap-1">
        <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-2">
          <div className="flex min-w-0 flex-1 basis-60 flex-col gap-0.5">
            <span className="text-sm font-medium" style={{ color: state === 'locked' ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}>
              {title}
            </span>
            {detail && (
              <span className="text-sm text-pretty" style={{ color: state === 'locked' ? 'var(--color-text-tertiary)' : 'var(--color-text-secondary)' }}>
                {detail}
              </span>
            )}
          </div>
          {action}
        </div>
        {children}
      </div>
    </li>
  );
}

/** Home's computer changed in place: what the change does, the sizes with
 *  `wanted` picked, and the git work it does not keep. */
function MediumForm({ computer, wanted, rereadPlan }: { computer: Computer; wanted: ResourceTier; rereadPlan: boolean }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { data: providers } = useComputerProviders({ enabled: true });
  const provider = providers?.providers.find((p) => p.kind === computer.kind);
  const { data: planQuota } = useTierQuota({ enabled: true });
  const { currentTier, offer, quota, tier, setPicked } = useTierPick(computer, provider, planQuota, wanted);
  const { spec } = useComputerActions();
  const [error, setError] = useState<SpecError | null>(null);
  const files = specErrorFiles(error?.files, t, locale);
  const submit = () => {
    setError(null);
    spec.mutate(
      { computerId: computer.computer_id, tier, planChanged: rereadPlan },
      { onError: (err) => setError(specErrorFrom(err, t, tier)) },
    );
  };
  return (
    <div className="mt-2 flex flex-col gap-3">
      <TierRadioGroup
        offer={offer}
        value={tier}
        onChange={setPicked}
        label={t('computer.tierLabel', 'Computer spec')}
        quota={quota}
        currentTier={currentTier}
      />
      <p role="note" className="rounded-md px-3 py-2 text-xs leading-relaxed" style={{ background: 'var(--color-warning-soft)', color: 'var(--color-text-primary)' }}>
        {t('computer.spec.gitNotKept')} {t(`${K}.medium.pushFirst`)}
      </p>
      {error && !spec.isPending && (
        <div role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>
          <p>{error.message}</p>
          {files.length > 0 && (
            <ul className="mt-1 list-disc pl-5 font-mono break-all" style={{ color: 'var(--color-text-secondary)' }}>
              {files.map((line) => <li key={line}>{line}</li>)}
            </ul>
          )}
        </div>
      )}
      <Button
        type="button"
        size="sm"
        className="tap-target self-end"
        disabled={spec.isPending || tier === currentTier}
        aria-busy={spec.isPending}
        onClick={submit}
      >
        {spec.isPending ? t('computer.spec.applying') : t(`${K}.medium.apply`, { size: tierLabel(t, tier) })}
      </Button>
    </div>
  );
}

/** The change to Medium running: a bar that fills toward the time it
 *  usually takes, and that the change keeps going if the sheet closes. */
function MediumProgress({ change }: { change: ComputerSpecChange }) {
  const { t } = useTranslation();
  const now = useNow(5_000);
  const elapsed = Math.max(0, now - Date.parse(change.started_at));
  const share = Number.isFinite(elapsed) ? Math.min(0.9, elapsed / TYPICAL_CHANGE_MS) : 0;
  return (
    <div className="mt-1 flex flex-col gap-1.5">
      <div
        role="progressbar"
        aria-label={t(`${K}.medium.running`, { size: tierLabel(t, change.target_tier) })}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(share * 100)}
        className="h-1.5 w-full overflow-hidden rounded-full"
        style={{ background: 'var(--color-bg-surface)' }}
      >
        <div className="h-full rounded-full transition-[width] duration-enter ease-out" style={{ width: `${Math.max(4, share * 100)}%`, background: 'var(--color-accent-primary)' }} />
      </div>
      <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t(`${K}.medium.runningNote`)}</span>
    </div>
  );
}

/**
 * Signing in to Claude, in this sheet: the token's own ways in, under the box
 * the user ticks to let LangAlpha store it, as /setup asks (`askConsent`).
 * Claude Code is turned on first where it is off, since the server takes no
 * token while it is.
 */
function TokenStep({ n }: { n: number }) {
  const { t } = useTranslation();
  const { setup } = useClaudeCodeSetup();
  const turnOn = useTurnOnClaudeCode(true, setup);
  const off = !turnOn.pending && setup?.on === false;
  return (
    <Step n={n} state="active" title={t(`${K}.token.title`)} detail={t(`${K}.token.detail`)}>
      <div className="mt-2">
        {turnOn.failed || off ? (
          <p className="flex flex-wrap items-center gap-2 text-sm" role="alert" style={{ color: 'var(--color-text-secondary)' }}>
            {t(`${K}.token.turnOnFailed`)}
            <Button type="button" size="sm" variant="outline" className="tap-target" onClick={turnOn.retry}>{t('modelAccess.tryAgain')}</Button>
          </p>
        ) : turnOn.pending || !setup ? (
          <StatusLabel status="working" />
        ) : (
          <ClaudeCodeTokenSheet target={SAVED_TOKEN} askConsent onSaved={() => undefined} />
        )}
      </div>
    </Step>
  );
}

/**
 * Claude Code on a cloud computer as ordered steps (`useClaudeCodeSteps`),
 * for a user with no computer of their own: the plan, Home's computer to
 * Medium, then signing in to Claude. A step done shows as its check; the
 * step to take is open; later ones wait. The plan page comes back to this
 * sheet, which reads the plan again; one that opened in another tab or
 * window is read again when this window has the focus back.
 *
 * The footer leads to running Claude Code on a computer of the user's own
 * instead (`onOwnComputer`), the sheet's other view.
 */
export function ClaudeCodeSteps({ steps, onOwnComputer }: {
  steps: Steps;
  onOwnComputer: () => void;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const status = useClaudeCodeStatus();
  const { plan, medium, token, home } = steps;
  const size = tierLabel(t, CLAUDE_CODE_MIN_TIER);
  const target = tierLabel(t, steps.size);
  const homeSize = home ? tierLabel(t, effectiveTier(home)) : size;
  const homeName = t('agents.chiefOfStaff');

  // While the plan step is open, coming back to this window reads the plan
  // again: the plan page may have opened in another tab or window, and been
  // left after checkout, or before it.
  const planOpen = !!plan && !plan.done;
  useEffect(() => {
    if (!planOpen) return;
    // Only once the plan page was opened from here, and once per return: a
    // tab switch fires both events.
    let last = 0;
    const back = () => {
      if (document.visibilityState !== 'visible' || Date.now() - last < 2000) return;
      if (userSessionStorage.getItem(PLAN_BEFORE_KEY) === null) return;
      last = Date.now();
      void readPlanAfterChange(queryClient);
    };
    window.addEventListener('focus', back);
    document.addEventListener('visibilitychange', back);
    return () => {
      window.removeEventListener('focus', back);
      document.removeEventListener('visibilitychange', back);
    };
  }, [planOpen, queryClient]);
  const planUrl = planPageUrl(PLAN_BACK, 'claude-code', steps.largest);
  // The plan before the trip reaches render as state: read from storage in
  // render, the compiler may keep its first value. Seeded from the marker for
  // a sheet the plan page's way back mounts anew.
  const [planBefore, setPlanBefore] = useState(() => userSessionStorage.getItem(PLAN_BEFORE_KEY));
  const pastMedium = steps.next === 'resizing' || steps.next === 'token';
  if (pastMedium && planBefore !== null) setPlanBefore(null);
  const changedTo = plan?.done && plan.current && planBefore !== null && planBefore !== plan.current ? plan.current : null;
  useEffect(() => {
    if (pastMedium) userSessionStorage.removeItem(PLAN_BEFORE_KEY);
  }, [pastMedium]);

  const offset = plan ? 1 : 0;
  const lead = {
    plan: t(`${K}.lead.plan`, { home: homeName }),
    medium: changedTo ? t(`${K}.lead.mediumPlan`, { plan: changedTo, home: homeName, size: target }) : t(`${K}.lead.medium`, { home: homeName, size: target }),
    resizing: t(`${K}.lead.tokenWhile`),
    token: t(`${K}.lead.token`),
  }[steps.next];

  return (
    <div className="flex flex-col gap-4">
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{lead}</p>
      <ol className="flex flex-col gap-2.5">
        {plan && (plan.done ? (
          <Step
            n={1}
            state="done"
            title={plan.current ? t(`${K}.plan.doneTitle`, { plan: plan.current }) : t(`${K}.plan.doneTitleUnnamed`)}
            detail={t(`${K}.plan.doneDetail`, { size: target })}
          />
        ) : (
          <Step
            n={1}
            state="active"
            title={t(`${K}.plan.title`, { size })}
            detail={t(`${K}.plan.why`, { size })}
            action={planUrl && (
              <Button type="button" size="sm" className="tap-target" onClick={() => {
                userSessionStorage.setItem(PLAN_BEFORE_KEY, plan.current ?? '');
                setPlanBefore(plan.current ?? '');
                openConsolePage(planUrl, { inPlace: plansOpenInPlace() });
              }}>
                {t(`${K}.plan.choose`)}
              </Button>
            )}
          />
        ))}
        {medium.state === 'done' ? (
          <Step n={offset + 1} state="done" title={t(`${K}.medium.doneTitle`, { size: homeSize })} detail={t(`${K}.medium.doneDetail`, { home: homeName, size: homeSize })} />
        ) : medium.state === 'running' && medium.change ? (
          <Step n={offset + 1} state="running" title={t(`${K}.medium.running`, { size: tierLabel(t, medium.change.target_tier) })}>
            <MediumProgress change={medium.change} />
          </Step>
        ) : (
          <Step
            n={offset + 1}
            state={medium.state === 'active' ? 'active' : 'locked'}
            title={t(`${K}.medium.title`, { home: homeName, size: target })}
            detail={medium.state === 'active' && home
              ? `${t(home.status === 'running' ? 'computer.spec.descRunning' : 'computer.spec.descStopped')} ${t('computer.spec.notKept')}`
              : medium.change ? specChangeLabel(t, medium.change) : t(`${K}.medium.locked`)}
          >
            {medium.state === 'active' && home && <MediumForm computer={home} wanted={steps.size} rereadPlan={!!plan} />}
          </Step>
        )}
        {token.state === 'done' ? (
          <Step n={offset + 2} state="done" title={t(`${K}.token.doneTitle`)} detail={status.email ?? undefined} />
        ) : token.state === 'active' ? (
          <TokenStep n={offset + 2} />
        ) : (
          <Step n={offset + 2} state="locked" title={t(`${K}.token.title`)} detail={t(`${K}.token.locked`)} />
        )}
      </ol>
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t(`${K}.own.lead`)}{' '}
        <button
          type="button"
          className="tap-target underline underline-offset-2 hover:opacity-80"
          style={{ color: 'var(--color-text-primary)' }}
          onClick={onOwnComputer}
        >
          {t(`${K}.own.link`)}
        </button>
      </p>
    </div>
  );
}
