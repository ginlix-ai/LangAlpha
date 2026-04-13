import { User, Settings, LogOut, ChevronDown, CreditCard } from 'lucide-react';
import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../../../contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import ConfirmDialog from './ConfirmDialog';

const AvatarDropdown: React.FC = () => {
  const navigate = useNavigate();
  const { isLoggedIn: _isLoggedIn, logout } = useAuth();
  const { user } = useUser();
  const { t } = useTranslation();
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const accountUrl = (import.meta.env.VITE_ACCOUNT_URL as string | undefined) || null;

  const avatarUrl = useMemo(() => {
    const url = user?.avatar_url;
    const version = user?.updated_at;
    return url ? `${url}?v=${version}` : null;
  }, [user?.avatar_url, user?.updated_at]);

  const displayName = (user?.display_name as string) || user?.name || '';
  const [avatarError, setAvatarError] = useState(false);
  useEffect(() => setAvatarError(false), [avatarUrl]);

  return (
    <>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            className="flex items-center gap-2 text-sm font-medium transition-colors hover:text-[var(--color-text-primary)]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <div
              className="h-8 w-8 rounded-full flex items-center justify-center overflow-hidden"
              style={{ backgroundColor: 'var(--color-accent-soft)' }}
            >
              {avatarUrl && !avatarError ? (
                <img src={avatarUrl} alt="avatar" className="h-full w-full object-cover" onError={() => setAvatarError(true)} />
              ) : (
                <User className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
              )}
            </div>
            {displayName && <span className="hidden sm:inline">{displayName}</span>}
            <ChevronDown size={14} style={{ color: 'var(--color-text-secondary)' }} />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" sideOffset={8}>
          {displayName && (
            <>
              <DropdownMenuLabel>{displayName}</DropdownMenuLabel>
              <DropdownMenuSeparator />
            </>
          )}
          <DropdownMenuItem onSelect={() => navigate('/settings')}>
            <Settings className="h-4 w-4" />
            {t('settings.title', 'Settings')}
          </DropdownMenuItem>
          {accountUrl && (
            <DropdownMenuItem asChild>
              <a href={accountUrl} target="_blank" rel="noopener noreferrer" className="flex items-center gap-2">
                <CreditCard className="h-4 w-4" />
                {t('sidebar.account', 'Usage & Plan')}
              </a>
            </DropdownMenuItem>
          )}
          <DropdownMenuSeparator />
          <DropdownMenuItem variant="destructive" onSelect={() => setShowLogoutConfirm(true)}>
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
        onConfirm={() => { logout(); setShowLogoutConfirm(false); }}
        onOpenChange={setShowLogoutConfirm}
      />
    </>
  );
};

export default AvatarDropdown;
