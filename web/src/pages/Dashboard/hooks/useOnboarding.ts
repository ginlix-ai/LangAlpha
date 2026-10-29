import { useState, useEffect, type Dispatch, type SetStateAction } from 'react';
import { useUser } from '@/hooks/useUser';

interface PersonalizationResult {
  showPersonalizationBanner: boolean;
  setShowPersonalizationBanner: Dispatch<SetStateAction<boolean>>;
}

const PERSONALIZATION_SNOOZE_KEY = 'langalpha-personalization-snoozed-at';
const PERSONALIZATION_SNOOZE_MS = 24 * 60 * 60 * 1000; // 24 hours

export function isPersonalizationSnoozed(): boolean {
    try {
        const stored = localStorage.getItem(PERSONALIZATION_SNOOZE_KEY);
        if (!stored) return false;
        const timestamp = parseInt(stored, 10);
        if (Number.isNaN(timestamp)) return false;
        return Date.now() - timestamp < PERSONALIZATION_SNOOZE_MS;
    } catch {
        return false;
    }
}

const PERSONALIZATION_SNOOZE_EVENT = 'langalpha:personalization-snoozed';

export function snoozePersonalization(): void {
    try {
        localStorage.setItem(PERSONALIZATION_SNOOZE_KEY, String(Date.now()));
    } catch (e) {
        console.warn('[Dashboard] Could not persist personalization snooze', e);
    }
    // Same-tab listeners (the onboarding provider) re-read the snooze state —
    // a localStorage write alone doesn't re-render other components.
    window.dispatchEvent(new Event(PERSONALIZATION_SNOOZE_EVENT));
}

/** Subscribe to same-tab snooze changes (for useSyncExternalStore). */
export function subscribePersonalizationSnooze(callback: () => void): () => void {
    window.addEventListener(PERSONALIZATION_SNOOZE_EVENT, callback);
    return () => window.removeEventListener(PERSONALIZATION_SNOOZE_EVENT, callback);
}

/**
 * Whether to show the banner offering the welcome flow: the user has not saved
 * an investing profile and has not snoozed the offer in the last 24 hours.
 */
export function useOnboarding(): PersonalizationResult {
    const { user: authUser } = useUser() as {
        user: {
            onboarding_completed?: boolean;
            [key: string]: unknown;
        } | null;
    };

    const [showPersonalizationBanner, setShowPersonalizationBanner] = useState(false);

    // Check onboarding completion reactively from user data. Not
    // `personalization_completed`, which says the user has a key of their own
    // and nothing about a profile.
    useEffect(() => {
        if (!authUser) return;
        if (authUser.onboarding_completed === true) {
            setShowPersonalizationBanner(false);
            return;
        }
        if (!isPersonalizationSnoozed()) {
            setShowPersonalizationBanner(true);
        }
    }, [authUser]);

    return { showPersonalizationBanner, setShowPersonalizationBanner };
}
