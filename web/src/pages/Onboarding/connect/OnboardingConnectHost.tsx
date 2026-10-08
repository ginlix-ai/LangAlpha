import { Suspense, useState } from 'react';
import { useSearchParams } from 'react-router';
import { useAllWorkspacesAgent } from '@/hooks/useAllWorkspacesAgent';
import { lazyWithMotion } from '@/lib/lazyWithMotion';
import { ONBOARDING_CONNECT, ONBOARDING_PARAM } from './useStartOnboarding';

// Lazy so the connect flow (the Plugins page's brokerage and OAuth graph)
// stays off the first load; most sessions never open it.
const OnboardingConnectLayer = lazyWithMotion(() => import('./OnboardingConnectSheet'));

/**
 * Opens the onboarding connect sheet over any page in the app shell while the
 * URL carries `?onboarding=connect`. Latched once opened, so closing animates
 * out rather than unmounting.
 */
export function OnboardingConnectHost() {
  const available = useAllWorkspacesAgent();
  const [searchParams, setSearchParams] = useSearchParams();
  const open = available && searchParams.get(ONBOARDING_PARAM) === ONBOARDING_CONNECT;
  const [needed, setNeeded] = useState(false);
  if (open && !needed) setNeeded(true);
  if (!needed) return null;

  function close() {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete(ONBOARDING_PARAM);
        return next;
      },
      { replace: true },
    );
  }

  return (
    <Suspense fallback={null}>
      <OnboardingConnectLayer open={open} onClose={close} />
    </Suspense>
  );
}
