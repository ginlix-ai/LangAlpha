import { lazy, Suspense, useRef } from 'react';
import { useOnboarding } from './OnboardingProvider';

// Lazy so the modal + visual-mockup graph (the bulk of the onboarding code)
// stays out of the main chunk.
const OnboardingHost = lazy(() =>
  import('./OnboardingHost').then((m) => ({ default: m.OnboardingHost }))
);

/**
 * Mounts the lazy OnboardingHost only when a surface needs the screen, so a
 * fully-onboarded user never downloads the chunk. Latched: once needed it
 * stays mounted for the session, so phase flips back to idle don't churn the
 * mount.
 */
export function OnboardingHostGate() {
  const { phase, gettingStarted } = useOnboarding();
  const neededRef = useRef(false);
  if (phase !== 'idle' || gettingStarted.visible) neededRef.current = true;
  if (!neededRef.current) return null;
  return (
    <Suspense fallback={null}>
      <OnboardingHost />
    </Suspense>
  );
}
