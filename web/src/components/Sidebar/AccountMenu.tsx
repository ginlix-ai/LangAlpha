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

interface AccountMenuProps {
  /** 'rail' = avatar-only trigger (collapsed sidebar); 'row' = full-width
   *  avatar + name row for the expanded panel's bottom slot. */
  variant?: 'rail' | 'row';
}

const AccountMenu: React.FC<AccountMenuProps> = ({ variant = 'rail' }) => {
  const navigate = useNavigate();
  const location = useLocation();
  const { logout } = useAuth();
  const { user } = useUser();
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

  // OSS forks: hide the Usage & Plan link entirely, even if VITE_PLATFORM_URL
  // is accidentally set (the default web/.env points it at /account, which
  // doesn't exist outside platform deployments).
  const platformUrl = isPlatformMode
    ? ((import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account')
    : null;

  const avatarUrl = useMemo(() => {
    const url = user?.avatar_url;
    const version = user?.updated_at;
    return url ? `${url}?v=${version}` : null;
  }, [user?.avatar_url, user?.updated_at]);

  const displayName = (user?.display_name as string) || user?.name || '';
  const email = user?.email || '';
  // 'Free' is the default-plan fallback, not a paid tier — flair is reserved
  // for paid plans so it reads as a status signal.
  // isPlatformMode (= HOST_MODE === 'platform') is the canonical gate — OSS
  // builds never render plan flair regardless of what's on the user object.
  const rawPlanDisplayName = isPlatformMode
    ? ((user?.plan_display_name as string | null | undefined) || null)
    : null;
  const planDisplayName = rawPlanDisplayName && rawPlanDisplayName !== 'Free' ? rawPlanDisplayName : null;
  const initials = useMemo(() => {
    const source = displayName || email;
    if (!source) return '';
    const letters = source
      .split(/[\s@.]+/)
      .filter(Boolean)
      .slice(0, 2)
      // Array.from, not [0]: an emoji or an astral CJK character is two code
      // units, and indexing takes half of one and renders it as U+FFFD.
      .map((s) => Array.from(s)[0]?.toUpperCase() ?? '')
      .join('');
    // Bound the characters, not the parts. Uppercasing can turn one code point
    // into two ('ß' -> 'SS', 'fi' ligature -> 'FI'), so slicing the parts leaves
    // the glyph count to the font: measured in Chromium, "ssner Muller" spelled
    // with an eszett gives SSM at 28.6px inside a 28px disc and clips.
    return Array.from(letters).slice(0, 2).join('');
  }, [displayName, email]);

  const [avatarError, setAvatarError] = useState(false);
  useEffect(() => setAvatarError(false), [avatarUrl]);

  const isSettingsActive = location.pathname === '/settings';
  const isTriggerActive = open || isSettingsActive;

  return (
    <>
      <DropdownMenu open={open} onOpenChange={setOpen} modal={false}>
        <DropdownMenuTrigger asChild>
          {variant === 'row' ? (
            // No aria-label here, unlike the collapsed trigger: this row spells
            // the account out, so a fixed label would replace what is on screen
            // with something less (WCAG 2.5.3). The plan reads out for the same
            // reason -- it is a tier, and the rail is where it is stated.
            <button
              type="button"
              className="sidebar-account-row"
              data-active={isTriggerActive ? 'true' : undefined}
            >
              {/* Decorative in this variant. Dropping the fixed `aria-label`
                  exposed every descendant, and the initials are a picture of the
                  name spelled out beside them: without this the row announces
                  "JD John Doe Pro". */}
              <span className="sidebar-account-row-avatar" aria-hidden="true">
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
              </span>
              <span className="sidebar-account-row-name">
                {displayName || email || t('account.menuLabel', 'Account menu')}
              </span>
              {planDisplayName && (
                <span className="sidebar-account-row-plan">
                  {planDisplayName}
                </span>
              )}
            </button>
          ) : (
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
          )}
        </DropdownMenuTrigger>

        <DropdownMenuContent
          side={variant === 'row' ? 'top' : 'right'}
          align={variant === 'row' ? 'start' : 'end'}
          sideOffset={variant === 'row' ? 8 : 12}
          // Expanded, the panel is an extension of the row it came from, so it
          // takes that row's width rather than a number of its own: the row is
          // inset 10px inside the sidebar, so any fixed width lines up on the
          // left (align="start") and misses on the right. Collapsed, the
          // trigger is a 32px avatar and has no width worth inheriting.
          className={variant === 'row' ? 'w-[var(--radix-dropdown-menu-trigger-width)]' : 'w-64'}
        >
          {/* The expanded trigger is a full row already carrying this avatar and
              this name, so repeating them here stacks the same identity twice.
              Collapsed, the trigger is a bare circle and this block is the only
              place the account is named at all. The email is what neither
              trigger shows, so the row variant keeps that line and nothing else. */}
          {variant === 'rail' && (displayName || email) && (
            <>
              <div className="flex items-center gap-2.5 px-2.5 py-1.5">
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
                          className="text-[0.625rem] font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded-full flex-shrink-0"
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

          {variant === 'row' && email && (
            <div
              className="px-2.5 pt-1 pb-1.5 text-xs truncate"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {email}
            </div>
          )}

          {platformUrl && (
            <DropdownMenuItem asChild>
              <a href={platformUrl} className="flex items-center gap-2">
                <CreditCard
                  className="h-4 w-4"
                  style={{ color: 'var(--color-accent-light)' }}
                />
                <span className="flex-1" style={{ color: 'var(--color-text-primary)' }}>
                  {t('sidebar.account', 'Usage & Plan')}
                </span>
                <ChevronRight
                  className="h-3.5 w-3.5"
                  style={{ color: 'var(--color-text-tertiary)' }}
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
