import { Suspense, useMemo } from 'react';
import { Routes, Route, Navigate, useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { Shield, X } from 'lucide-react';
import { useTheme } from '@/contexts/ThemeContext';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { skipSetup } from '@/hooks/useSetupGate';

// Self-hosted / local dev: always allow exiting the wizard since keys are in .env
const AUTH_ENABLED = !!import.meta.env.VITE_SUPABASE_URL;
import logoLight from '@/assets/img/logo.svg';
import logoDark from '@/assets/img/logo-dark.svg';
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
// Progress stepper
// ---------------------------------------------------------------------------

function ProgressStepper({ currentDot, labels }: { currentDot: number; labels: string[] }) {
  return (
    <div
      className="flex items-center justify-center gap-0"
      role="progressbar"
      aria-valuenow={currentDot + 1}
      aria-valuemin={1}
      aria-valuemax={4}
      aria-label={`Step ${currentDot + 1} of ${labels.length}: ${labels[currentDot]}`}
    >
      {labels.map((label, i) => (
        <div key={label} className="flex items-center">
          {i > 0 && (
            <div
              className="h-px w-10 sm:w-16 transition-colors duration-200"
              style={{
                backgroundColor: i <= currentDot
                  ? 'var(--color-accent-primary)'
                  : 'var(--color-border-default)',
              }}
            />
          )}
          <div className="flex flex-col items-center gap-1.5">
            <div
              className="flex items-center justify-center rounded-full transition-all duration-200"
              style={{
                width: 28,
                height: 28,
                backgroundColor: i <= currentDot
                  ? 'var(--color-accent-primary)'
                  : 'transparent',
                border: i <= currentDot
                  ? 'none'
                  : '2px solid var(--color-border-default)',
                color: i <= currentDot
                  ? '#fff'
                  : 'var(--color-text-tertiary)',
                fontSize: '0.75rem',
                fontWeight: 600,
              }}
            >
              {i + 1}
            </div>
            <span
              className="text-xs font-medium select-none"
              style={{
                color: i <= currentDot
                  ? 'var(--color-text-primary)'
                  : 'var(--color-text-tertiary)',
              }}
            >
              {label}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Transition variants
// ---------------------------------------------------------------------------

function slideVariants(direction: number) {
  return {
    initial: { x: direction > 0 ? 40 : -40, opacity: 0 },
    animate: { x: 0, opacity: 1 },
    exit: { x: direction > 0 ? -40 : 40, opacity: 0 },
  };
}

// ---------------------------------------------------------------------------
// SetupWizard
// ---------------------------------------------------------------------------

export default function SetupWizard() {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;
  const location = useLocation();
  const navigate = useNavigate();
  const { hasAny: hasProvider } = useConfiguredProviders();
  const canExit = hasProvider || !AUTH_ENABLED;
  const stepperLabels = STEPPER_KEYS.map((k) => t(`setup.${k}`));

  const current = routeIndex(location.pathname);
  const currentDot = stepperDot(current);

  const direction = useMemo(() => 1, []);
  const variants = slideVariants(direction);

  return (
    <div
      className="h-screen h-dvh flex flex-col overflow-y-auto"
      style={{ backgroundColor: 'var(--color-bg-page)' }}
    >
      {/* Exit button — visible when user already has model access configured */}
      {canExit && (
        <button
          type="button"
          onClick={() => { skipSetup(); navigate('/dashboard'); }}
          className="fixed top-4 right-4 z-50 flex items-center gap-1.5 px-2 sm:px-3 py-1.5 rounded-md text-xs font-medium transition-colors hover:opacity-80"
          style={{
            color: 'var(--color-text-secondary)',
            background: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-default)',
          }}
        >
          <X className="h-3.5 w-3.5" />
          <span className="hidden sm:inline">{t('setup.exitSetup', 'Exit setup')}</span>
        </button>
      )}

      {/* Branded header */}
      <header className="flex flex-col items-center gap-3 sm:gap-4 pt-5 sm:pt-10 pb-2 px-4">
        <img src={logo} alt="LangAlpha" className="h-7" />
        <h1
          className="text-center font-semibold text-lg sm:text-2xl"
          style={{
            lineHeight: 1.3,
            color: 'var(--color-text-primary)',
          }}
        >
          {t('setup.brandedHeader')}
        </h1>
        <p
          className="hidden sm:flex items-center gap-1.5 text-xs"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <Shield className="h-3.5 w-3.5" />
          {t('setup.securityNote')}
        </p>
      </header>

      {/* Progress stepper */}
      <div className="py-4 sm:py-6">
        <ProgressStepper currentDot={currentDot} labels={stepperLabels} />
      </div>

      {/* Step content */}
      <main
        role="main"
        aria-label="Account setup"
        className="flex-1 w-full mx-auto"
        style={{
          maxWidth: 640,
          padding: 'clamp(16px, 5vw, 48px)',
          paddingTop: 0,
          paddingBottom: 'calc(env(safe-area-inset-bottom, 0px) + 32px)',
        }}
      >
        <AnimatePresence mode="wait" initial={false}>
          <motion.div
            key={current}
            variants={variants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={{ duration: 0.2, ease: 'easeInOut' }}
          >
            <Suspense
              fallback={
                <div
                  className="flex items-center justify-center py-20"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  <span className="text-sm">Loading...</span>
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
          </motion.div>
        </AnimatePresence>
      </main>
    </div>
  );
}
