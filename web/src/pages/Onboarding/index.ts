// OnboardingHost is intentionally NOT re-exported: it pulls the heavy modal +
// visuals graph. OnboardingHostGate lazy-imports it on demand, so the chunk is
// never fetched in a session where no onboarding surface shows.
export { OnboardingProvider, useOnboarding } from './OnboardingProvider';
export { OnboardingHostGate } from './OnboardingHostGate';
