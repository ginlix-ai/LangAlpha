import { Suspense, useEffect, useState, type ReactNode } from 'react';
import { Routes, Route, Navigate, useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Shield } from 'lucide-react';
import { ExternalRedirect } from '@/components/ExternalRedirect';
import { WizardExit, WizardProgress, WizardShell } from '@/components/ui/wizard';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useOnboardingRoute } from '@/hooks/useOnboardingRoute';
import { useUser } from '@/hooks/useUser';
import { skipSetup } from '@/hooks/useSetupGate';
import { captureSetupReturn, returnFromSetup, withoutSetupReturnParam } from '@/lib/setupReturn';
import { SetupFooterSlot } from './SetupFooter';
import { setupStartTarget } from './answers/gate';
import { StepLoadFailed, StepLoading } from './answers/parts';
import type { SetupStep } from './answers/setupSteps';
import { useSetupFacts } from './answers/useSetupFacts';

import { isPlatformMode } from '@/config/hostMode';
import { ONBOARDING_USE_URL } from '@/config/supportLinks';
import React from 'react';

const MethodStep = React.lazy(() => import('./steps/MethodStep'));
const ProviderStep = React.lazy(() => import('./steps/ProviderStep'));
const ConnectStep = React.lazy(() => import('./steps/ConnectStep'));
const ClaudeCodeStep = React.lazy(() => import('./steps/ClaudeCodeStep'));
const ModelPickStep = React.lazy(() => import('./steps/ModelPickStep'));
const DefaultsStep = React.lazy(() => import('./steps/DefaultsStep'));
const DoneStep = React.lazy(() => import('./steps/DoneStep'));
const UseStep = React.lazy(() => import('./steps/UseStep'));
const ComputerStep = React.lazy(() => import('./steps/ComputerStep'));
const SshStep = React.lazy(() => import('./steps/SshStep'));
const ModelStep = React.lazy(() => import('./steps/ModelStep'));
const DefaultStep = React.lazy(() => import('./steps/DefaultStep'));
// The Mac route's pages.
const MacHandoffStep = React.lazy(() => import('./mac/MacHandoffStep'));
const ThisMacStep = React.lazy(() => import('./mac/ThisMacStep'));
const ContinueStep = React.lazy(() => import('./mac/ContinueStep'));

/** Macro stepper i18n keys (4 dots for user clarity) */
const STEPPER_KEYS = ['stepMethod', 'stepProvider', 'stepModels', 'stepReady'] as const;

type AnswerRailStep = SetupStep | 'use';

/** Where a provider page stands on the four dots of connecting a provider:
 *  its place in the flow (`index`), its dot, and whether the note on keys
 *  shows under it (`keys` while keys are entered, not on a subscription's
 *  sign-in, which has its own note on the tokens it stores). */
interface ProviderPlace {
  index: number;
  dot: number;
  securityNote: 'always' | 'keys' | false;
}

/**
 * Every page of setup, once: its element and its place on a rail.
 * `answers` pages stand under a step of the answers' rail (the Mac and SSH
 * pages under the computer step). `provider` pages keep the four dots;
 * `connect` pages are provider pages that stand under the answers' model
 * step while setup runs on the answers, since that step sends a
 * subscription or key through them. Claude Code's step stands where a
 * sign-in's connect step does.
 */
type SetupPage =
  | { path: string; element: ReactNode; rail: 'answers'; step: AnswerRailStep }
  | { path: string; element: ReactNode; rail: 'provider' | 'connect'; place: ProviderPlace };

const PAGES: readonly SetupPage[] = [
  { path: 'method', element: <MethodStep />, rail: 'provider', place: { index: 0, dot: 0, securityNote: 'always' } },
  { path: 'provider', element: <ProviderStep />, rail: 'connect', place: { index: 1, dot: 1, securityNote: 'always' } },
  { path: 'connect', element: <ConnectStep />, rail: 'connect', place: { index: 2, dot: 1, securityNote: 'keys' } },
  { path: 'claude-code', element: <ClaudeCodeStep />, rail: 'connect', place: { index: 2, dot: 1, securityNote: false } },
  { path: 'models', element: <ModelPickStep />, rail: 'provider', place: { index: 3, dot: 2, securityNote: false } },
  { path: 'defaults', element: <DefaultsStep />, rail: 'provider', place: { index: 4, dot: 2, securityNote: false } },
  { path: 'ready', element: <DoneStep />, rail: 'provider', place: { index: 5, dot: 3, securityNote: false } },
  { path: 'use', element: <UseStep />, rail: 'answers', step: 'use' },
  { path: 'computer', element: <ComputerStep />, rail: 'answers', step: 'computer' },
  { path: 'ssh', element: <SshStep />, rail: 'answers', step: 'computer' },
  { path: 'mac', element: <MacHandoffStep />, rail: 'answers', step: 'computer' },
  { path: 'this-mac', element: <ThisMacStep />, rail: 'answers', step: 'computer' },
  { path: 'continue', element: <ContinueStep />, rail: 'answers', step: 'computer' },
  { path: 'model', element: <ModelStep />, rail: 'answers', step: 'model' },
  { path: 'default', element: <DefaultStep />, rail: 'answers', step: 'default' },
];

const pageOf = (pathname: string): SetupPage | null => {
  const path = pathname.replace(/^\/setup\/?/, '').replace(/\/$/, '');
  return PAGES.find((p) => p.path === path) ?? null;
};

/**
 * `/setup` itself, an explicit visit: on to where onboarding stands
 * (`setupStartTarget`), on any surface.
 */
function SetupStart() {
  const route = useOnboardingRoute();
  if (route.kind === 'loading') return <StepLoading />;
  const target = setupStartTarget(route);
  if (target?.kind === 'platform') return ONBOARDING_USE_URL ? <ExternalRedirect to={ONBOARDING_USE_URL} /> : null;
  return <Navigate to={target?.to ?? '/setup/method'} replace />;
}

// ---------------------------------------------------------------------------
// SetupWizard
// ---------------------------------------------------------------------------

export default function SetupWizard() {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { hasAny: hasProvider } = useConfiguredProviders();
  const { user } = useUser();
  const hasPlatformAccess = (user?.access_tier ?? -1) >= 0;
  const canExit = hasProvider || hasPlatformAccess || !isPlatformMode;
  // Captured on entry, in the initializer so the first render already knows
  // whether to show the exit. Writing the same target twice is harmless.
  const [hasReturn, setHasReturn] = useState(() => captureSetupReturn(location.search) !== null);
  // A page that sent the user here has somewhere to go back to, so the exit
  // shows even before they have any model access.
  const showExit = canExit || hasReturn;
  const { rail, factsFailed, retryFacts } = useSetupFacts();
  // A connect page the answers' model step sent the user to.
  const answering = (location.state as { answering?: boolean } | null)?.answering === true;

  // The answers' pages draw their own rail ("Computer", "Model", "Default",
  // after "Your use" where this app asks the questions); the pages Settings
  // links to keep the four dots of connecting a provider.
  const page = pageOf(location.pathname);
  const answerStep: AnswerRailStep | null = page?.rail === 'answers' ? page.step
    : page?.rail === 'connect' && answering ? 'model' : null;
  const place = page && page.rail !== 'answers' ? page.place : null;
  // The answers' pages and /setup itself resume on the facts, so one that
  // could not be read holds them on the way to read it again.
  const held = factsFailed && (page ? page.rail === 'answers' && page.step !== 'use' : true);
  const answerRail: AnswerRailStep[] = [...(isPlatformMode ? [] : ['use' as const]), ...(rail.length ? rail : (['computer', 'model', 'default'] as const))];
  const stepperLabels = answerStep
    ? answerRail.map((k) => t(`setup.answers.rail.${k}`))
    : STEPPER_KEYS.map((k) => t(`setup.${k}`));
  // A page whose step is not on this account's rail lights no step.
  const current = answerStep ? answerRail.indexOf(answerStep) : place?.index ?? 0;
  const dot = answerStep ? current : place?.dot ?? 0;
  const stepKey = answerStep ? location.pathname : String(current);
  const connectMethod = (location.state as { method?: string } | null)?.method;
  const securityNote = !!place
    && (place.securityNote === 'always' || (place.securityNote === 'keys' && connectMethod !== 'oauth'));

  // Where the steps draw their buttons; see SetupFooter.
  const [footerSlot, setFooterSlot] = useState<HTMLElement | null>(null);

  // Once captured, the param leaves the address bar, so a copied or reloaded
  // URL doesn't carry it. The router state the steps rely on is kept.
  useEffect(() => {
    const search = withoutSetupReturnParam(location.search);
    if (search === null) return;
    navigate(
      { pathname: location.pathname, search, hash: location.hash },
      { replace: true, state: location.state },
    );
  }, [location, navigate]);

  const handleExit = () => {
    if (returnFromSetup()) return;
    if (!canExit) {
      // The return expired while the page was open; without access there is
      // still nowhere else to go.
      setHasReturn(false);
      return;
    }
    skipSetup();
    navigate('/dashboard');
  };

  return (
    <WizardShell
      testId="setup-flow"
      stepKey={stepKey}
      stepIndex={Math.max(0, current)}
      progress={
        <WizardProgress steps={stepperLabels} current={dot} label={t('setup.progressLabel')} />
      }
      // Shown when the user already has model access configured, or when
      // another page sent them here and expects them back.
      exit={showExit ? <WizardExit label={t('setup.exitSetup', 'Exit setup')} onClick={handleExit} /> : undefined}
      footer={<div ref={setFooterSlot} />}
    >
      <SetupFooterSlot value={footerSlot}>
        {held ? <StepLoadFailed onRetry={retryFacts} /> : (
          <Suspense fallback={<StepLoading />}>
            <Routes location={location}>
              {PAGES.map((p) => <Route key={p.path} path={p.path} element={p.element} />)}
              <Route index element={<SetupStart />} />
              <Route path="*" element={<Navigate to="/setup" replace />} />
            </Routes>
          </Suspense>
        )}
      </SetupFooterSlot>
      {securityNote && (
        <p className="mt-8 flex items-center gap-1.5 text-xs text-(--color-text-tertiary)">
          <Shield aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
          <span>{t('setup.securityNote')}</span>
        </p>
      )}
    </WizardShell>
  );
}
