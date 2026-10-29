import { useOnboarding } from './OnboardingProvider';
import { PageIntroModal } from './engine/PageIntroModal';
import { WhatsNewModal } from './engine/WhatsNewModal';

/**
 * Renders the active onboarding popup: the contextual page intro or the
 * versioned What's-New modal, one at a time. Mounted once inside the
 * authenticated shell. The getting-started list lives in the sidebar.
 */
export function OnboardingHost() {
  const { phase, activeIntro, unseen, dismissPageIntro, abandonPageIntro, acknowledgeWhatsNew } =
    useOnboarding();

  return (
    <>
      {phase === 'pageIntro' && activeIntro && (
        // Keyed so step state never leaks between two different intros.
        <PageIntroModal
          key={activeIntro.id}
          intro={activeIntro}
          onClose={dismissPageIntro}
          onAbandon={abandonPageIntro}
        />
      )}
      {phase === 'whatsNew' && (
        <WhatsNewModal announcements={unseen} onAcknowledge={acknowledgeWhatsNew} />
      )}
    </>
  );
}
