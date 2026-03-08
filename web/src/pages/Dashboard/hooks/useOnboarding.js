import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import { getCurrentUser } from '../utils/api';
import { getFlashWorkspace } from '../../ChatAgent/utils/api';

const ONBOARDING_IGNORE_STORAGE_KEY = 'langalpha-onboarding-ignored-at';
const ONBOARDING_IGNORE_MS = 24 * 60 * 60 * 1000; // 24 hours

export function isOnboardingIgnoredFor24h() {
    try {
        const stored = localStorage.getItem(ONBOARDING_IGNORE_STORAGE_KEY);
        if (!stored) return false;
        const timestamp = parseInt(stored, 10);
        if (Number.isNaN(timestamp)) return false;
        return Date.now() - timestamp < ONBOARDING_IGNORE_MS;
    } catch {
        return false;
    }
}

export function setOnboardingIgnoredFor24h() {
    try {
        localStorage.setItem(ONBOARDING_IGNORE_STORAGE_KEY, String(Date.now()));
    } catch (e) {
        console.warn('[Dashboard] Could not persist onboarding ignore', e);
    }
}

/**
 * useOnboarding Hook
 * Manages the onboarding states, showing the modal correctly on mount if 
 * the user has not completed onboarding, and wraps navigation logic.
 */
export function useOnboarding() {
    const navigate = useNavigate();
    const { t } = useTranslation();
    const { toast } = useToast();

    const [showOnboardingDialog, setShowOnboardingDialog] = useState(false);
    const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);

    // Checks onboarding completion status strictly once at mount.
    useEffect(() => {
        const checkOnboarding = async () => {
            try {
                const userData = await getCurrentUser();
                const onboardingCompleted = userData?.user?.onboarding_completed;

                if (onboardingCompleted === true) {
                    setShowOnboardingDialog(false);
                    return;
                }
                if (onboardingCompleted === false && !isOnboardingIgnoredFor24h()) {
                    setShowOnboardingDialog(true);
                }
            } catch (error) {
                console.error('[Dashboard] Error checking onboarding status:', error);
            }
        };

        checkOnboarding();
    }, []);

    const navigateToOnboarding = useCallback(async () => {
        setIsCreatingWorkspace(true);
        try {
            const flashWs = await getFlashWorkspace();
            navigate(`/chat/${flashWs.workspace_id}/__default__`, {
                state: {
                    isOnboarding: true,
                    agentMode: 'flash',
                    workspaceStatus: 'flash',
                },
            });
        } catch (error) {
            console.error('Error setting up onboarding:', error);
            toast({
                variant: 'destructive',
                title: t('common.error'),
                description: t('dashboard.failedOnboarding'),
            });
        } finally {
            setIsCreatingWorkspace(false);
        }
    }, [navigate, toast, t]);

    const navigateToModifyPreferences = useCallback(async () => {
        try {
            const flashWs = await getFlashWorkspace();
            navigate(`/chat/${flashWs.workspace_id}/__default__`, {
                state: {
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
        showOnboardingDialog,
        setShowOnboardingDialog,
        isCreatingWorkspace,
        navigateToOnboarding,
        navigateToModifyPreferences
    };
}
