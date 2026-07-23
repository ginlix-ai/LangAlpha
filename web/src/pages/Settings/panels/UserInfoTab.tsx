import React, { useCallback, useEffect, useRef, useState } from 'react';
import { User, LogOut, Sun, Moon, Monitor } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { ToggleSwitch } from '@/components/ui/switch';
import { updateCurrentUser, uploadAvatar } from '@/pages/Dashboard/utils/api';
import { useAuth } from '@/contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useTheme } from '@/contexts/ThemeContext';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { useDebouncedSave } from '@/hooks/useDebouncedSave';
import { isSupported, setLocaleCookie } from '@/lib/locale';
import type { Preferences } from './types';

interface TimezoneOption {
  value: string;
  label: string;
}

interface TimezoneGroup {
  group: string;
  options: TimezoneOption[];
}

type TimezoneEntry = TimezoneOption | TimezoneGroup;

/** User-info tab: avatar, name/timezone/locale with debounced auto-save,
 * theme preference, voice-input toggle, and logout. */
export function UserInfoTab() {
  const { toast } = useToast();
  const { logout } = useAuth();
  const { user: authUser } = useUser();
  const { preferences: prefsData } = usePreferences();
  const updatePrefsMutation = useUpdatePreferences();
  const queryClient = useQueryClient();
  const { theme: _theme, preference, setTheme: setThemePref } = useTheme();
  const { t, i18n } = useTranslation();

  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);
  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

  const timezones: TimezoneEntry[] = [
    { value: '', label: t('settings.selectTimezone') },
    {
      group: 'Americas', options: [
        { value: 'America/New_York', label: 'Eastern Time (America/New_York)' },
        { value: 'America/Chicago', label: 'Central Time (America/Chicago)' },
        { value: 'America/Denver', label: 'Mountain Time (America/Denver)' },
        { value: 'America/Los_Angeles', label: 'Pacific Time (America/Los_Angeles)' },
        { value: 'America/Toronto', label: 'Eastern - Canada (America/Toronto)' },
        { value: 'America/Sao_Paulo', label: 'Brasília Time (America/Sao_Paulo)' },
      ]
    },
    {
      group: 'Europe', options: [
        { value: 'Europe/London', label: 'GMT (Europe/London)' },
        { value: 'Europe/Paris', label: 'CET (Europe/Paris)' },
        { value: 'Europe/Berlin', label: 'CET (Europe/Berlin)' },
      ]
    },
    {
      group: 'Asia', options: [
        { value: 'Asia/Shanghai', label: 'China Standard Time (Asia/Shanghai)' },
        { value: 'Asia/Tokyo', label: 'Japan Standard Time (Asia/Tokyo)' },
        { value: 'Asia/Hong_Kong', label: 'Hong Kong Time (Asia/Hong_Kong)' },
        { value: 'Asia/Singapore', label: 'Singapore Time (Asia/Singapore)' },
        { value: 'Asia/Kolkata', label: 'India Standard Time (Asia/Kolkata)' },
      ]
    },
    {
      group: 'Oceania', options: [
        { value: 'Australia/Sydney', label: 'Australian Eastern (Australia/Sydney)' },
      ]
    },
    {
      group: 'Other', options: [
        { value: 'UTC', label: 'UTC' },
      ]
    },
  ];

  const locales = [
    { value: '', label: t('settings.selectLocale') },
    { value: 'en-US', label: 'English (United States)' },
    { value: 'zh-CN', label: '中文（简体）' },
  ];

  // Initialize form state from user data (provided by useUser hook)
  useEffect(() => {
    if (authUser) {
      setName(authUser.name || '');
      setTimezone((authUser.timezone as string) || '');
      setLocale((authUser.locale as string) || '');
      const url = authUser.avatar_url;
      setAvatarUrl(url ? `${url}?v=${authUser.updated_at || ''}` : null);
    }
  }, [authUser]);

  const handleAvatarChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setIsUploadingAvatar(true);
    try {
      const { avatar_url } = await uploadAvatar(file) as { avatar_url: string };
      setAvatarUrl(`${avatar_url}?t=${Date.now()}`);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    } catch {
      setError(t('settings.failedToUploadAvatar'));
    } finally {
      setIsUploadingAvatar(false);
    }
  };

  // Auto-save user info: use refs so the debounced callback always reads latest state
  const userInfoRef = useRef({ name, timezone, locale });
  userInfoRef.current = { name, timezone, locale };

  const dirtyRef = useRef(false);

  const saveUserInfo = useCallback(async () => {
    setError(null);
    dirtyRef.current = false;
    const s = userInfoRef.current;
    const userData: Record<string, string> = {};
    if (s.name.trim()) userData.name = s.name.trim();
    if (s.timezone) userData.timezone = s.timezone;
    if (s.locale) userData.locale = s.locale;
    if (Object.keys(userData).length > 0) {
      await updateCurrentUser(userData);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    }
  }, [queryClient]);

  const { trigger: triggerUserInfoSave, flush: flushUserInfoSave, status: userInfoSaveStatus } = useDebouncedSave(saveUserInfo, 800);

  const handleNameChange = (value: string) => {
    setName(value);
    dirtyRef.current = true;
    triggerUserInfoSave();
  };

  const handleTimezoneChange = (value: string) => {
    setTimezone(value);
    userInfoRef.current = { ...userInfoRef.current, timezone: value };
    flushUserInfoSave();
  };

  const handleLocaleChange = (newLocale: string) => {
    setLocale(newLocale);
    if (isSupported(newLocale)) {
      i18n.changeLanguage(newLocale);
      setLocaleCookie(newLocale);
    }
    userInfoRef.current = { ...userInfoRef.current, locale: newLocale };
    flushUserInfoSave();
  };

  // This panel unmounts on tab switch, and useDebouncedSave cancels its timer
  // on unmount — flush a pending edit so it isn't silently lost.
  useEffect(() => () => { if (dirtyRef.current) flushUserInfoSave(); }, [flushUserInfoSave]);

  const handleVoiceInputToggle = async () => {
    const currentOtherPref = (prefsData as any)?.other_preference || {};
    const currentEnabled = !!currentOtherPref.voice_input_enabled;
    try {
      await updatePrefsMutation.mutateAsync({
        other_preference: {
          ...currentOtherPref,
          voice_input_enabled: !currentEnabled,
        },
      });
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  const handleLogoutConfirm = () => {
    logout();
    setShowLogoutConfirm(false);
  };

  return (
    <>
    <div className="space-y-5">
      <div className="flex items-center gap-4 mb-6 pb-6" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
        <div
          className="h-16 w-16 rounded-full flex items-center justify-center cursor-pointer overflow-hidden flex-shrink-0"
          style={{ backgroundColor: 'var(--color-accent-soft)' }}
          onClick={() => fileInputRef.current?.click()}
        >
          {avatarUrl ? (
            <img src={avatarUrl} alt="avatar" className="h-full w-full object-cover" onError={() => setAvatarUrl(null)} />
          ) : (
            <User className="h-8 w-8" style={{ color: 'var(--color-accent-primary)' }} />
          )}
        </div>
        <div>
          <button type="button" onClick={() => fileInputRef.current?.click()} disabled={isUploadingAvatar}
            className="px-3 py-1.5 rounded-md text-sm font-medium"
            style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
          >
            {isUploadingAvatar ? t('settings.uploading') : t('settings.changeAvatar')}
          </button>
        </div>
        <input type="file" ref={fileInputRef} onChange={handleAvatarChange} accept="image/png,image/jpeg,image/gif,image/webp" style={{ display: 'none' }} />
        <div className="ml-auto">
          <button
            type="button"
            onClick={() => setShowLogoutConfirm(true)}
            className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors"
            style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
          >
            <LogOut className="h-4 w-4" /> {t('settings.logout')}
          </button>
        </div>
      </div>

      <div>
        <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('common.email')}</label>
        <Input
          type="email"
          value={authUser?.email || ''}
          readOnly
          disabled
          className="w-full opacity-80"
          style={{
            backgroundColor: 'var(--color-bg-card)',
            border: '1px solid var(--color-border-muted)',
            color: 'var(--color-text-primary)',
          }}
        />
        <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.emailCannotBeChanged')}</p>
      </div>

      <div>
        <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('common.name')}</label>
        <Input
          type="text"
          value={name}
          onChange={(e) => handleNameChange(e.target.value)}
          onBlur={() => flushUserInfoSave()}
          placeholder={t('auth.enterName')}
          className="w-full"
          style={{
            backgroundColor: 'var(--color-bg-card)',
            border: '1px solid var(--color-border-muted)',
            color: 'var(--color-text-primary)',
          }}
        />
      </div>

      <div>
        <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('settings.timezone')}</label>
        <Select
          value={timezone}
          onChange={(e) => handleTimezoneChange(e.target.value)}
        >
          {timezones.map((item, i) => (
            'value' in item ? (
              <option key={i} value={item.value}>{item.label}</option>
            ) : (
              <optgroup key={i} label={item.group}>
                {item.options.map((opt, j) => (
                  <option key={`${i}-${j}`} value={opt.value}>{opt.label}</option>
                ))}
              </optgroup>
            )
          ))}
        </Select>
      </div>

      <div>
        <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('settings.locale')}</label>
        <Select
          value={locale}
          onChange={(e) => handleLocaleChange(e.target.value)}
        >
          {locales.map((item, i) => (
            <option key={i} value={item.value}>{item.label}</option>
          ))}
        </Select>
      </div>

      {/* Theme Toggle */}
      <div className="flex items-center justify-between p-3 rounded-lg" style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
        <div className="space-y-0.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.theme')}</label>
        </div>
        <div className="inline-flex rounded-lg overflow-hidden" style={{ border: '1px solid var(--color-border-muted)' }}>
          <button
            type="button"
            onClick={() => setThemePref('dark')}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium transition-colors"
            style={{
              backgroundColor: preference === 'dark' ? 'var(--color-accent-soft)' : 'transparent',
              color: preference === 'dark' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
            }}
          >
            <Moon className="h-3.5 w-3.5" />
            {t('settings.dark')}
          </button>
          <button
            type="button"
            onClick={() => setThemePref('light')}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium transition-colors"
            style={{
              backgroundColor: preference === 'light' ? 'var(--color-accent-soft)' : 'transparent',
              color: preference === 'light' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
            }}
          >
            <Sun className="h-3.5 w-3.5" />
            {t('settings.light')}
          </button>
          <button
            type="button"
            onClick={() => setThemePref('auto')}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium transition-colors"
            style={{
              backgroundColor: preference === 'auto' ? 'var(--color-accent-soft)' : 'transparent',
              color: preference === 'auto' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
            }}
          >
            <Monitor className="h-3.5 w-3.5" />
            {t('settings.auto', 'Auto')}
          </button>
        </div>
      </div>

      {/* Voice Input Toggle */}
      <div className="flex items-center justify-between p-3 rounded-lg" style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
        <div className="space-y-0.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.voiceInput')}</label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.voiceInputDesc')}</p>
        </div>
        <ToggleSwitch
          checked={(prefsData as Preferences | null)?.other_preference?.voice_input_enabled === true}
          onChange={handleVoiceInputToggle}
          ariaLabel={t('settings.voiceInput')}
        />
      </div>

      {error && (
        <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
        </div>
      )}

      {userInfoSaveStatus !== 'idle' && (
        <div className="flex items-center justify-end pt-2">
          {userInfoSaveStatus === 'saving' && (
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span>
          )}
          {userInfoSaveStatus === 'saved' && (
            <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
          )}
          {userInfoSaveStatus === 'error' && (
            <span className="text-xs" style={{ color: 'var(--color-loss)' }}>{t('settings.failedToSaveSettings')}</span>
          )}
        </div>
      )}
    </div>

    <ConfirmDialog
      open={showLogoutConfirm}
      title={t('settings.logout')}
      message={t('settings.logoutConfirmMsg')}
      confirmLabel={t('settings.logout')}
      onConfirm={handleLogoutConfirm}
      onOpenChange={setShowLogoutConfirm}
    />
    </>
  );
}
