import { useOnboarding } from './OnboardingProvider';
import { PageIntroModal } from './engine/PageIntroModal';
import { WhatsNewModal } from './engine/WhatsNewModal';
import { GettingStartedCard } from './engine/GettingStartedCard';

/**
 * Renders the active onboarding surfaces: the contextual page intro or the
 * versioned What's-New modal (one popup at a time), plus the persistent
 * getting-started checklist card. Mounted once inside the authenticated shell.
 */
export function OnboardingHost() {
  const { phase, activeIntro, unseen, dismissPageIntro, acknowledgeWhatsNew } = useOnboarding();

  return (
    <>
      {phase === 'pageIntro' && activeIntro && (
        // Keyed so step state never leaks between two different intros.
        <PageIntroModal key={activeIntro.id} intro={activeIntro} onClose={dismissPageIntro} />
      )}
      {phase === 'whatsNew' && (
        <WhatsNewModal announcements={unseen} onAcknowledge={acknowledgeWhatsNew} />
      )}
      <GettingStartedCard />
    </>
  );
}
