import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { useUser } from '@/hooks/useUser';
import { readWelcomeDraft } from './draft';
import { WELCOME_PATH } from './paths';

export interface WelcomeOffer {
  /**
   * "Resume" where this browser holds a draft, else "Set up". The flow opens
   * on the draft either way, so the button says which it is about to do. Short,
   * for a button beside a heading that names the profile; one with nothing
   * beside it to say so (the empty gallery, the end of setup) keeps words of
   * its own that name it.
   */
  label: string;
  /** Opens the flow. `replace` for a screen the user is done with, so Back
   *  does not return to it. */
  open: (options?: { replace?: boolean }) => void;
}

/**
 * The welcome flow, offered wherever a user who has not saved an investing
 * profile might take it up: the dashboard banner, Settings, the empty
 * workspace gallery and the end of setup. Null once the profile is saved.
 *
 * Read off `onboarding_completed` alone, as the onboarding provider is:
 * `personalization_completed` means the user has a key of their own and says
 * nothing about a profile. An account still loading counts as not done, so an
 * offer can show before it arrives and goes once it says otherwise.
 *
 * Every offer opens the flow's plain URL, which picks up this browser's draft
 * where there is one. Settings' editor of a saved profile is another way in
 * (`welcomeUrl({ edit: true })`), open after completion too, and not an offer.
 */
export function useWelcomeOffer(): WelcomeOffer | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { user } = useUser();
  if (user?.onboarding_completed === true) return null;
  const resumable = readWelcomeDraft(user?.user_id) !== null;
  return {
    label: resumable ? t('onboarding.welcome.banner.resume') : t('onboarding.welcome.banner.start'),
    open: ({ replace = false } = {}) => navigate(WELCOME_PATH, { replace }),
  };
}
