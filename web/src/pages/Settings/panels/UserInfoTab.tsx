import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { User, LogOut, Sun, Moon, Monitor } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { ToggleSwitch } from '@/components/ui/switch';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { updateCurrentUser, uploadAvatar } from '@/pages/Dashboard/utils/api';
import { useAuth } from '@/contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useTheme } from '@/contexts/ThemeContext';
import { FONT_SCALES, getFontScale, setFontScale, type FontScale } from '@/lib/fontScale';
import { turnEndScrollPatch, readTurnEndScroll, type TurnEndScroll } from '@/lib/turnEndScroll';
import {
  readStreamingMode,
  readTurnDisplay,
  streamingModePatch,
  turnDisplayPatch,
  type StreamingMode,
  type TurnDisplay,
} from '@/lib/transcriptDisplay';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { useDebouncedSave } from '@/hooks/useDebouncedSave';
import { isSupported, setLocaleCookie } from '@/lib/locale';
import TimezonePicker from '@/components/TimezonePicker';
import { deviceTimezone } from '@/lib/deviceTimezone';
import type { Preferences } from './types';

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
  const [fontScale, setFontScaleState] = useState(getFontScale);
  const themeLabelId = useId();
  const fontSizeLabelId = useId();
  const turnEndLabelId = useId();
  const turnDisplayLabelId = useId();
  const streamingModeLabelId = useId();
  const { t, i18n } = useTranslation();

  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);
  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);


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
    const currentEnabled = !!((prefsData as Preferences | null)?.other_preference?.voice_input_enabled);
    try {
      // One key only: the server merges other_preference key by key, and
      // writing the cached object back would overwrite a sibling another tab
      // changed since this one last fetched.
      await updatePrefsMutation.mutateAsync({
        other_preference: { voice_input_enabled: !currentEnabled },
      });
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  const turnEndScroll = readTurnEndScroll(prefsData);
  const handleTurnEndScrollChange = async (next: TurnEndScroll) => {
    if (next === turnEndScroll) return;
    try {
      await updatePrefsMutation.mutateAsync(turnEndScrollPatch(next));
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  const turnDisplay = readTurnDisplay(prefsData);
  const handleTurnDisplayChange = async (next: TurnDisplay) => {
    if (next === turnDisplay) return;
    try {
      await updatePrefsMutation.mutateAsync(turnDisplayPatch(next));
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  const streamingMode = readStreamingMode(prefsData);
  const handleStreamingModeChange = async (next: StreamingMode) => {
    if (next === streamingMode) return;
    try {
      await updatePrefsMutation.mutateAsync(streamingModePatch(next));
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
    <div className="space-y-4">
      <div className="flex items-center gap-4 mb-5 pb-5" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
        <div
          className="h-12 w-12 rounded-full flex items-center justify-center cursor-pointer overflow-hidden flex-shrink-0"
          style={{ backgroundColor: 'var(--color-accent-soft)' }}
          onClick={() => fileInputRef.current?.click()}
        >
          {avatarUrl ? (
            <img src={avatarUrl} alt="avatar" className="h-full w-full object-cover" onError={() => setAvatarUrl(null)} />
          ) : (
            <User className="h-6 w-6" style={{ color: 'var(--color-accent-primary)' }} />
          )}
        </div>
        <div>
          <button type="button" onClick={() => fileInputRef.current?.click()} disabled={isUploadingAvatar}
            className="px-3 py-1.5 rounded-md text-sm font-medium border transition-opacity hover:opacity-90"
            style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)', color: 'var(--color-text-primary)' }}
          >
            {isUploadingAvatar ? t('settings.uploading') : t('settings.changeAvatar')}
          </button>
        </div>
        <input type="file" ref={fileInputRef} onChange={handleAvatarChange} accept="image/png,image/jpeg,image/gif,image/webp" style={{ display: 'none' }} />
        <div className="ml-auto">
          <button
            type="button"
            onClick={() => setShowLogoutConfirm(true)}
            className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-colors"
            style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
          >
            <LogOut className="h-4 w-4" /> {t('settings.logout')}
          </button>
        </div>
      </div>

      <div>
        <label className="block text-[0.8125rem] font-medium mb-1.5" style={{ color: 'var(--color-text-primary)' }}>{t('common.email')}</label>
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
        <label className="block text-[0.8125rem] font-medium mb-1.5" style={{ color: 'var(--color-text-primary)' }}>{t('common.name')}</label>
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
        <label className="block text-[0.8125rem] font-medium mb-1.5" style={{ color: 'var(--color-text-primary)' }}>{t('settings.timezone')}</label>
        <TimezonePicker
          value={timezone}
          onChange={handleTimezoneChange}
          home={{ zone: deviceTimezone(), label: t('timezone.thisDevice') }}
          placeholder={t('settings.selectTimezone')}
          className="w-full"
          // The card fill the name and language fields beside it take.
          triggerClassName="bg-[color:var(--color-bg-card)]"
        />
      </div>

      <div>
        <label className="block text-[0.8125rem] font-medium mb-1.5" style={{ color: 'var(--color-text-primary)' }}>{t('settings.locale')}</label>
        <Select
          value={locale}
          onChange={(e) => handleLocaleChange(e.target.value)}
        >
          {locales.map((item, i) => (
            <option key={i} value={item.value}>{item.label}</option>
          ))}
        </Select>
      </div>

      <div className="settings-rows">
      {/* Theme Toggle */}
      <div className="settings-row">
        <div className="space-y-0.5">
          <label id={themeLabelId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.theme')}</label>
        </div>
        <SegmentedControl
          labelledBy={themeLabelId}
          value={preference}
          onChange={setThemePref}
          options={[
            { value: 'dark', label: <><Moon className="h-3.5 w-3.5" />{t('settings.dark')}</> },
            { value: 'light', label: <><Sun className="h-3.5 w-3.5" />{t('settings.light')}</> },
            { value: 'auto', label: <><Monitor className="h-3.5 w-3.5" />{t('settings.auto', 'Auto')}</> },
          ]}
        />
      </div>

      {/* Font size — multiplies the browser's own font-size preference */}
      <div className="settings-row">
        <div className="space-y-0.5">
          <label id={fontSizeLabelId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.fontSize', 'Font size')}</label>
        </div>
        <SegmentedControl
          labelledBy={fontSizeLabelId}
          value={String(fontScale)}
          onChange={(v) => { const next = Number(v) as FontScale; setFontScale(next); setFontScaleState(next); }}
          options={FONT_SCALES.map((scale) => ({ value: String(scale), label: `${Math.round(scale * 100)}%` }))}
        />
      </div>

      {/* Voice Input Toggle */}
      <div className="settings-row">
        <div className="space-y-0.5">
          <label className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.voiceInput')}</label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.voiceInputDesc')}</p>
        </div>
        <ToggleSwitch
          checked={(prefsData as Preferences | null)?.other_preference?.voice_input_enabled === true}
          onChange={handleVoiceInputToggle}
          ariaLabel={t('settings.voiceInput')}
        />
      </div>

      {/* Where the transcript lands when a reply finishes */}
      <div className="settings-row">
        <div className="space-y-0.5">
          <label id={turnEndLabelId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.turnEndScroll')}</label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.turnEndScrollDesc')}</p>
        </div>
        <SegmentedControl
          labelledBy={turnEndLabelId}
          value={turnEndScroll}
          onChange={(v) => { void handleTurnEndScrollChange(v); }}
          options={[
            { value: 'bottom', label: t('settings.turnEndScrollBottom') },
            { value: 'reply_start', label: t('settings.turnEndScrollReplyStart') },
          ]}
        />
      </div>

      {/* Whether reasoning is shown as it streams */}
      <div className="settings-row flex-wrap">
        <div className="flex-1 basis-64 space-y-0.5">
          <label id={turnDisplayLabelId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.turnDisplay')}</label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.turnDisplayDesc')}</p>
        </div>
        <div className="flex shrink-0">
          <SegmentedControl
            labelledBy={turnDisplayLabelId}
            value={turnDisplay}
            onChange={(v) => { void handleTurnDisplayChange(v); }}
            options={[
              { value: 'lean', label: t('settings.turnDisplayLean') },
              { value: 'verbose', label: t('settings.turnDisplayVerbose') },
            ]}
          />
        </div>
      </div>

      {/* How response text appears while it streams */}
      <div className="settings-row flex-wrap">
        <div className="flex-1 basis-64 space-y-0.5">
          <label id={streamingModeLabelId} className="text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.streamingMode')}</label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.streamingModeDesc')}</p>
        </div>
        <div className="flex shrink-0">
          <SegmentedControl
            labelledBy={streamingModeLabelId}
            value={streamingMode}
            onChange={(v) => { void handleStreamingModeChange(v); }}
            options={[
              { value: 'token', label: t('settings.streamingModeToken') },
              { value: 'paragraph', label: t('settings.streamingModeParagraph') },
            ]}
          />
        </div>
      </div>
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
