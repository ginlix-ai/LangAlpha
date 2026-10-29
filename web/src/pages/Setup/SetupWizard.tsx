import { Suspense, useEffect, useState } from 'react';
import { Routes, Route, Navigate, useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Shield } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { WizardExit, WizardProgress, WizardShell } from '@/components/ui/wizard';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useUser } from '@/hooks/useUser';
import { skipSetup } from '@/hooks/useSetupGate';
import { captureSetupReturn, returnFromSetup, withoutSetupReturnParam } from '@/lib/setupReturn';
import { SetupFooterSlot } from './SetupFooter';

import { isPlatformMode } from '@/config/hostMode';
import React from 'react';

const MethodStep = React.lazy(() => import('./steps/MethodStep'));
const ProviderStep = React.lazy(() => import('./steps/ProviderStep'));
const ConnectStep = React.lazy(() => import('./steps/ConnectStep'));
const ModelPickStep = React.lazy(() => import('./steps/ModelPickStep'));
const DefaultsStep = React.lazy(() => import('./steps/DefaultsStep'));
const DoneStep = React.lazy(() => import('./steps/DoneStep'));

// ---------------------------------------------------------------------------
// Step metadata — 4 macro dots, 6 micro routes
// ---------------------------------------------------------------------------

const ROUTES = [
  '/setup/method',
  '/setup/provider',
  '/setup/connect',
  '/setup/models',
  '/setup/defaults',
  '/setup/ready',
] as const;

/** Macro stepper i18n keys (4 dots for user clarity) */
const STEPPER_KEYS = ['stepMethod', 'stepProvider', 'stepModels', 'stepReady'] as const;

/** Map route index → stepper dot index */
function stepperDot(routeIdx: number): number {
  // method(0),provider(1),connect(2) → dot 0-1
  // models(3) → dot 2
  // defaults(4) → dot 2
  // ready(5) → dot 3
  if (routeIdx <= 1) return routeIdx;
  if (routeIdx === 2) return 1;
  if (routeIdx <= 4) return 2;
  return 3;
}

function routeIndex(pathname: string): number {
  const idx = ROUTES.findIndex((r) => pathname.startsWith(r));
  return idx === -1 ? 0 : idx;
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
  const stepperLabels = STEPPER_KEYS.map((k) => t(`setup.${k}`));

  const current = routeIndex(location.pathname);
  const connectMethod = (location.state as { method?: string } | null)?.method;
  const currentDot = stepperDot(current);

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
      stepKey={String(current)}
      stepIndex={current}
      progress={
        <WizardProgress steps={stepperLabels} current={currentDot} label={t('setup.progressLabel')} />
      }
      // Shown when the user already has model access configured, or when
      // another page sent them here and expects them back.
      exit={showExit ? <WizardExit label={t('setup.exitSetup', 'Exit setup')} onClick={handleExit} /> : undefined}
      footer={<div ref={setFooterSlot} />}
    >
      <SetupFooterSlot value={footerSlot}>
        <Suspense
          fallback={
            <div className="flex items-center justify-center py-20 text-(--color-text-tertiary)">
              <Loader size={20} label={t('common.loading')} />
            </div>
          }
        >
          <Routes location={location}>
            <Route path="method" element={<MethodStep />} />
            <Route path="provider" element={<ProviderStep />} />
            <Route path="connect" element={<ConnectStep />} />
            <Route path="models" element={<ModelPickStep />} />
            <Route path="defaults" element={<DefaultsStep />} />
            <Route path="ready" element={<DoneStep />} />
            <Route path="*" element={<Navigate to="/setup/method" replace />} />
          </Routes>
        </Suspense>
      </SetupFooterSlot>
      {/* Said while keys are being chosen and entered, not after. A
          subscription sign-in has its own note about the tokens it stores. */}
      {(current <= 1 || (current === 2 && connectMethod !== 'oauth')) && (
        <p className="mt-8 flex items-center gap-1.5 text-xs text-(--color-text-tertiary)">
          <Shield aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
          <span>{t('setup.securityNote')}</span>
        </p>
      )}
    </WizardShell>
  );
}
