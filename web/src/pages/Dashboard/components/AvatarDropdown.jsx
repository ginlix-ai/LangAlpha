import { User, Settings, LogOut, ChevronDown, CreditCard } from 'lucide-react';
import React, { useState, useEffect, useRef, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAuth } from '../../../contexts/AuthContext';
import ConfirmDialog from './ConfirmDialog';

const AvatarDropdown = () => {
  const navigate = useNavigate();
  const { isLoggedIn, logout, user } = useAuth();
  const { t } = useTranslation();
  const [showDropdown, setShowDropdown] = useState(false);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const dropdownRef = useRef(null);
  const accountUrl = import.meta.env.VITE_ACCOUNT_URL || null;

  const avatarUrl = useMemo(() => {
    const url = user?.avatar_url;
    const version = user?.updated_at;
    return url ? `${url}?v=${version}` : null;
  }, [user?.avatar_url, user?.updated_at]);

  const displayName = user?.display_name || user?.name || '';
  const [avatarError, setAvatarError] = useState(false);
  useEffect(() => setAvatarError(false), [avatarUrl]);

  // Close dropdown on outside click
  useEffect(() => {
    if (!showDropdown) return;
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showDropdown]);

  return (
    <>
      <div className="relative" ref={dropdownRef}>
        <button
          className="flex items-center gap-2 text-sm font-medium transition-colors"
          style={{ color: 'var(--color-text-secondary)' }}
          onClick={() => setShowDropdown((prev) => !prev)}
          onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--color-text-primary)')}
          onMouseLeave={(e) => {
            if (!showDropdown) e.currentTarget.style.color = 'var(--color-text-secondary)';
          }}
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

        {showDropdown && (
          <div
            className="absolute right-0 top-full mt-2 z-50 rounded-lg shadow-lg py-1"
            style={{
              backgroundColor: 'var(--color-bg-elevated)',
              border: '1px solid var(--color-border-elevated)',
              minWidth: '180px',
            }}
          >
            {displayName && (
              <div
                className="px-4 py-2 text-sm font-medium truncate"
                style={{ color: 'var(--color-text-primary)', borderBottom: '1px solid var(--color-border-muted)' }}
              >
                {displayName}
              </div>
            )}
            <button
              className="w-full flex items-center gap-2 px-4 py-2 text-sm transition-colors text-left"
              style={{ color: 'var(--color-text-secondary)' }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = 'var(--color-bg-input)';
                e.currentTarget.style.color = 'var(--color-text-primary)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = 'transparent';
                e.currentTarget.style.color = 'var(--color-text-secondary)';
              }}
              onClick={() => {
                setShowDropdown(false);
                navigate('/settings');
              }}
            >
              <Settings className="h-4 w-4" />
              {t('settings.title', 'Settings')}
            </button>
            {accountUrl && (
              <a
                href={accountUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="w-full flex items-center gap-2 px-4 py-2 text-sm transition-colors text-left no-underline"
                style={{ color: 'var(--color-text-secondary)' }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = 'var(--color-bg-input)';
                  e.currentTarget.style.color = 'var(--color-text-primary)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = 'transparent';
                  e.currentTarget.style.color = 'var(--color-text-secondary)';
                }}
                onClick={() => setShowDropdown(false)}
              >
                <CreditCard className="h-4 w-4" />
                {t('sidebar.account', 'Usage & Plan')}
              </a>
            )}
            <button
              className="w-full flex items-center gap-2 px-4 py-2 text-sm transition-colors text-left"
              style={{ color: 'var(--color-loss)' }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = 'var(--color-bg-input)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = 'transparent';
              }}
              onClick={() => {
                setShowDropdown(false);
                setShowLogoutConfirm(true);
              }}
            >
              <LogOut className="h-4 w-4" />
              {t('settings.logout', 'Log out')}
            </button>
          </div>
        )}
      </div>

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
