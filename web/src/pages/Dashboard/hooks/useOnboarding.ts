import { useState, useEffect, useCallback, type Dispatch, type SetStateAction } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import { useUser } from '@/hooks/useUser';
import { getFlashWorkspace } from '../../ChatAgent/utils/api';

interface PersonalizationResult {
  showPersonalizationBanner: boolean;
  setShowPersonalizationBanner: Dispatch<SetStateAction<boolean>>;
  isCreatingWorkspace: boolean;
  navigateToPersonalization: () => Promise<void>;
  navigateToModifyPreferences: () => Promise<void>;
  /** @deprecated Use showPersonalizationBanner instead */
  showOnboardingDialog: boolean;
  /** @deprecated Use setShowPersonalizationBanner instead */
  setShowOnboardingDialog: Dispatch<SetStateAction<boolean>>;
  /** @deprecated Use navigateToPersonalization instead */
  navigateToOnboarding: () => Promise<void>;
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

/** @deprecated Use isPersonalizationSnoozed instead */
export const isOnboardingIgnoredFor24h = isPersonalizationSnoozed;
/** @deprecated Use snoozePersonalization instead */
export const setOnboardingIgnoredFor24h = snoozePersonalization;

/**
 * useOnboarding Hook
 * Shows an optional "Personalize your experience" banner if the user has not
 * completed personalization (formerly onboarding). The banner is non-blocking
 * and can be dismissed / snoozed for 24 hours.
 */
export function useOnboarding(): PersonalizationResult {
    const navigate = useNavigate();
    const { t } = useTranslation();
    const { toast } = useToast();

    const { user: authUser } = useUser() as {
        user: {
            onboarding_completed?: boolean;
            personalization_completed?: boolean;
            [key: string]: unknown;
        } | null;
    };

    const [showPersonalizationBanner, setShowPersonalizationBanner] = useState(false);
    const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);

    // Check personalization / onboarding completion reactively from user data
    useEffect(() => {
        if (!authUser) return;
        // Treat either flag as "completed" for backward compatibility
        if (authUser.personalization_completed === true || authUser.onboarding_completed === true) {
            setShowPersonalizationBanner(false);
            return;
        }
        if (!isPersonalizationSnoozed()) {
            setShowPersonalizationBanner(true);
        }
    }, [authUser]);

    const navigateToPersonalization = useCallback(async (): Promise<void> => {
        setIsCreatingWorkspace(true);
        try {
            const flashWs = await getFlashWorkspace() as { workspace_id: string };
            navigate(`/chat/t/__default__`, {
                state: {
                    workspaceId: flashWs.workspace_id,
                    isPersonalizing: true,
                    // Keep isOnboarding for backward compat with ChatView
                    isOnboarding: true,
                    agentMode: 'flash',
                    workspaceStatus: 'flash',
                },
            });
        } catch (error) {
            console.error('Error setting up personalization:', error);
            toast({
                variant: 'destructive',
                title: t('common.error'),
                description: t('dashboard.failedOnboarding'),
            });
        } finally {
            setIsCreatingWorkspace(false);
        }
    }, [navigate, toast, t]);

    const navigateToModifyPreferences = useCallback(async (): Promise<void> => {
        try {
            const flashWs = await getFlashWorkspace() as { workspace_id: string };
            navigate(`/chat/t/__default__`, {
                state: {
                    workspaceId: flashWs.workspace_id,
                    isModifyingPreferences: true,
                    agentMode: 'flash',
                    workspaceStatus: 'flash',
                },
            });
        } catch (error) {
            console.error('Error navigating to modify preferences:', error);
            toast({
                variant: 'destructive',
                title: t('common.error'),
                description: t('dashboard.failedPrefUpdate'),
            });
        }
    }, [navigate, toast, t]);

    return {
        showPersonalizationBanner,
        setShowPersonalizationBanner,
        isCreatingWorkspace,
        navigateToPersonalization,
        navigateToModifyPreferences,
        // Backward-compat aliases
        showOnboardingDialog: showPersonalizationBanner,
        setShowOnboardingDialog: setShowPersonalizationBanner,
        navigateToOnboarding: navigateToPersonalization,
    };
}
