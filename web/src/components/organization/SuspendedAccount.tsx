import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { Building2, LogOut, RotateCcw } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { userQueryOptions } from '@/hooks/useUser';
import { resetAccountSuspended } from '@/lib/accountSuspension';
import { orgWords, standingOf } from '@/lib/accountRules';
import { useAccount } from '@/hooks/useAccount';

/**
 * The whole app, for an account whose access through its organization has
 * ended: every route but `/users/me` refuses it, so instead of a page of
 * failed lists and toasts it says once what happened, what the person can
 * still do, and lets them sign out. "Check again" reads `/users/me`, the one
 * answer a suspended account still gets, and opens the app again if access
 * came back.
 */
export function SuspendedAccount({ organization }: { organization: string | null }) {
  const { t } = useTranslation();
  const { logout } = useAuth();
  const queryClient = useQueryClient();
  const [checking, setChecking] = useState(false);
  const [stillEnded, setStillEnded] = useState(false);

  const checkAgain = async () => {
    setChecking(true);
    setStillEnded(false);
    try {
      // Asked now and once: the button is the retry.
      const user = await queryClient.fetchQuery({ ...userQueryOptions, staleTime: 0, retry: false });
      if (standingOf(user).suspended) {
        setStillEnded(true);
      } else {
        resetAccountSuspended();
        // What every page read while refused is read again.
        void queryClient.invalidateQueries();
      }
    } catch {
      setStillEnded(true);
    } finally {
      setChecking(false);
    }
  };

  return (
    <div className="h-full min-h-dvh w-full flex items-center justify-center px-4 py-10" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <div
        role="alert"
        className="w-full max-w-md rounded-xl border px-6 py-6 flex flex-col gap-4"
        style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
      >
        <div
          className="h-10 w-10 rounded-lg flex items-center justify-center"
          style={{ backgroundColor: 'var(--color-bg-subtle)' }}
          aria-hidden="true"
        >
          <Building2 className="h-5 w-5" style={{ color: 'var(--color-text-secondary)' }} />
        </div>
        <div className="flex flex-col gap-2">
          <h1 className="text-lg font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            {orgWords(t, organization, 'organization.suspended.title')}
          </h1>
          <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
            {orgWords(t, organization, 'organization.suspended.body')}
          </p>
        </div>
        <div className="flex flex-col gap-1.5">
          <h2 className="text-xs font-medium uppercase tracking-wide" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('organization.suspended.canStill')}
          </h2>
          <ul className="text-sm flex flex-col gap-1 list-disc pl-5" style={{ color: 'var(--color-text-secondary)' }}>
            <li>{orgWords(t, organization, 'organization.suspended.askAdmin')}</li>
            <li>{t('organization.suspended.signOther')}</li>
          </ul>
        </div>
        {stillEnded && (
          <p role="status" className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('organization.suspended.stillEnded')}
          </p>
        )}
        <div className="flex flex-wrap gap-2 pt-1">
          <Button className="gap-2" onClick={() => { void logout(); }}>
            <LogOut className="h-4 w-4" aria-hidden="true" />
            {t('organization.suspended.signOut')}
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => { void checkAgain(); }} disabled={checking}>
            <RotateCcw className="h-4 w-4" aria-hidden="true" />
            {checking ? t('organization.suspended.checking') : t('organization.suspended.checkAgain')}
          </Button>
        </div>
      </div>
    </div>
  );
}

/** Renders the app, or the suspended account's one state in its place. */
export function SuspendedGate({ children }: { children: ReactNode }) {
  const account = useAccount();
  if (account.suspended) return <SuspendedAccount organization={account.organizationName} />;
  return <>{children}</>;
}
