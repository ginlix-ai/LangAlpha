import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import type { UserPreferences } from '@/types/api';

/** The toast that says a setting did not save. */
export function useSaveFailedToast() {
  const { t } = useTranslation();
  const { toast } = useToast();
  return () => {
    toast({ variant: 'destructive', title: t('common.error'), description: t('settings.failedToSaveSettings') });
  };
}

/**
 * Settings apply as they are picked and say nothing when the save lands, so a
 * failed one has to be said out loud: this runs a save and toasts if it fails.
 */
export function useSaveFailureToast() {
  const toastSaveFailed = useSaveFailedToast();
  return (save: Promise<unknown>) => save.then(() => undefined, toastSaveFailed);
}

/**
 * Writes one preference patch, said out loud if it fails. Each control writes
 * only the keys it owns: the server merges a preference group key by key, and
 * writing a cached group back whole would overwrite a sibling another tab
 * changed since this one last fetched.
 */
export function usePreferenceWrite() {
  const reportFailure = useSaveFailureToast();
  const { mutateAsync } = useUpdatePreferences();
  return (patch: Partial<UserPreferences>) => reportFailure(mutateAsync(patch));
}
