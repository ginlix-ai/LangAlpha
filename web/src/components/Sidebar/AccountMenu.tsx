import { User, Settings, LogOut, CreditCard, ChevronRight } from 'lucide-react';
import React, { useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../../contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { isPlatformMode } from '@/config/hostMode';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';

const AccountMenu: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { logout } = useAuth();
  const { user } = useUser();
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

  // OSS forks: hide the Usage & Plan link entirely, even if VITE_ACCOUNT_URL
  // is accidentally set (the default web/.env points it at /account, which
  // doesn't exist outside platform deployments).
  const accountUrl = isPlatformMode
    ? ((import.meta.env.VITE_ACCOUNT_URL as string | undefined) || '/account')
    : null;

  const avatarUrl = useMemo(() => {
    const url = user?.avatar_url;
    const version = user?.updated_at;
    return url ? `${url}?v=${version}` : null;
  }, [user?.avatar_url, user?.updated_at]);

  const displayName = (user?.display_name as string) || user?.name || '';
  const email = user?.email || '';
  // 'Free' is the default-plan fallback (auth_plans.is_default), not a paid
  // tier — flair is reserved for paid plans so it reads as a status signal.
  // Mirrors ginlix-platform/Layout.jsx so both sidebars agree on what to show.
  // isPlatformMode (= HOST_MODE === 'platform') is the canonical gate — OSS
  // builds never render plan flair regardless of what's on the user object.
  const rawPlanDisplayName = isPlatformMode
    ? ((user?.plan_display_name as string | null | undefined) || null)
    : null;
  const planDisplayName = rawPlanDisplayName && rawPlanDisplayName !== 'Free' ? rawPlanDisplayName : null;
  const initials = useMemo(() => {
    const source = displayName || email;
    if (!source) return '';
    return source
      .split(/[\s@.]+/)
      .filter(Boolean)
      .slice(0, 2)
      .map((s) => s[0]?.toUpperCase() ?? '')
      .join('');
  }, [displayName, email]);

  const [avatarError, setAvatarError] = useState(false);
  useEffect(() => setAvatarError(false), [avatarUrl]);

  const isSettingsActive = location.pathname === '/settings';
  const isTriggerActive = open || isSettingsActive;

  return (
    <>
      <DropdownMenu open={open} onOpenChange={setOpen} modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            aria-label={t('account.menuLabel', 'Account menu')}
            title={t('account.menuLabel', 'Account menu')}
            className="sidebar-account-trigger"
            data-active={isTriggerActive ? 'true' : undefined}
          >
            {avatarUrl && !avatarError ? (
              <img
                src={avatarUrl}
                alt=""
                className="sidebar-account-avatar-img"
                onError={() => setAvatarError(true)}
              />
            ) : initials ? (
              <span className="sidebar-account-initials">{initials}</span>
            ) : (
              <User className="sidebar-account-icon" />
            )}
            {planDisplayName && (
              <span className="sidebar-account-plan-flair" aria-hidden="true">
                {planDisplayName}
              </span>
            )}
          </button>
        </DropdownMenuTrigger>

        <DropdownMenuContent
          side="right"
          align="end"
          sideOffset={12}
          className="w-64"
        >
          {(displayName || email) && (
            <>
              <div className="flex items-center gap-2.5 px-3 py-2">
                <div
                  className="h-9 w-9 rounded-full flex items-center justify-center overflow-hidden flex-shrink-0"
                  style={{ backgroundColor: 'var(--color-accent-soft)' }}
                >
                  {avatarUrl && !avatarError ? (
                    <img src={avatarUrl} alt="" className="h-full w-full object-cover" />
                  ) : initials ? (
                    <span
                      className="text-xs font-semibold"
                      style={{ color: 'var(--color-accent-light)' }}
                    >
                      {initials}
                    </span>
                  ) : (
                    <User className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  {displayName && (
                    <div className="flex items-center gap-1.5 min-w-0">
                      <span
                        className="text-sm font-semibold truncate"
                        style={{ color: 'var(--color-text-primary)' }}
                      >
                        {displayName}
                      </span>
                      {planDisplayName && (
                        <span
                          className="text-[10px] font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded-full flex-shrink-0"
                          style={{
                            backgroundColor: 'var(--color-accent-soft)',
                            color: 'var(--color-accent-light)',
                          }}
                        >
                          {planDisplayName}
                        </span>
                      )}
                    </div>
                  )}
                  {email && (
                    <div
                      className="text-xs truncate"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      {email}
                    </div>
                  )}
                </div>
              </div>
              <DropdownMenuSeparator />
            </>
          )}

          {accountUrl && (
            <DropdownMenuItem asChild>
              <a
                href={accountUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  border: '1px solid var(--color-accent-overlay)',
                }}
              >
                <CreditCard
                  className="h-4 w-4"
                  style={{ color: 'var(--color-accent-light)' }}
                />
                <span className="flex-1" style={{ color: 'var(--color-text-primary)' }}>
                  {t('sidebar.account', 'Usage & Plan')}
                </span>
                <ChevronRight
                  className="h-3.5 w-3.5"
                  style={{ color: 'var(--color-accent-light)' }}
                />
              </a>
            </DropdownMenuItem>
          )}

          <DropdownMenuItem onSelect={() => navigate('/settings')}>
            <Settings className="h-4 w-4" />
            {t('sidebar.settings', 'Settings')}
          </DropdownMenuItem>

          <DropdownMenuSeparator />

          <DropdownMenuItem
            variant="destructive"
            onSelect={() => setShowLogoutConfirm(true)}
          >
            <LogOut className="h-4 w-4" />
            {t('settings.logout', 'Log out')}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <ConfirmDialog
        open={showLogoutConfirm}
        title={t('settings.logout', 'Log out')}
        message={t('settings.logoutConfirmMsg', 'Are you sure you want to log out?')}
        confirmLabel={t('settings.logout', 'Log out')}
        onConfirm={() => {
          logout();
          setShowLogoutConfirm(false);
        }}
        onOpenChange={setShowLogoutConfirm}
      />
    </>
  );
};

export default AccountMenu;
