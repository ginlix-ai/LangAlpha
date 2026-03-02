import React, { useState, useEffect, useRef } from 'react';
import { X, User, LogOut, Eye, EyeOff, Trash2, HelpCircle, MessageSquareText, Sun, Moon, Monitor, Link2, Unlink, ExternalLink, Shield, ClipboardCopy } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '../../../components/ui/dialog';
import { updateCurrentUser, getCurrentUser, updatePreferences, getPreferences, clearPreferences, uploadAvatar, getAvailableModels, getUserApiKeys, updateUserApiKeys, deleteUserApiKey, initiateCodexDevice, pollCodexDevice, getCodexOAuthStatus, disconnectCodexOAuth } from '../utils/api';
import { useAuth } from '../../../contexts/AuthContext';
import { useTheme } from '../../../contexts/ThemeContext';
import { useTranslation } from 'react-i18next';
import ConfirmDialog from './ConfirmDialog';

/**
 * UserConfigPanel Component
 *
 * Modal panel for logged-in users: User info (email read-only), preferences, and logout button.
 *
 * @param {boolean} isOpen - Whether the panel is open
 * @param {Function} onClose - Callback to close the panel
 */
function UserConfigPanel({ isOpen, onClose, onModifyPreferences, onStartOnboarding, initialTab }) {
  const { user: authUser, logout, refreshUser } = useAuth();
  const { theme, preference, setTheme: setThemePref } = useTheme();
  const { t, i18n } = useTranslation();
  const [activeTab, setActiveTab] = useState(initialTab || 'userInfo');
  const [avatarUrl, setAvatarUrl] = useState(null);
  const fileInputRef = useRef(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');

  const [preferences, setPreferences] = useState(null);

  // Model tab state
  const [availableModels, setAvailableModels] = useState({});
  const [preferredModel, setPreferredModel] = useState('');
  const [preferredFlashModel, setPreferredFlashModel] = useState('');
  const [byokEnabled, setByokEnabled] = useState(false);
  const [byokProviders, setByokProviders] = useState([]);
  const [keyInputs, setKeyInputs] = useState({});
  const [visibleKeys, setVisibleKeys] = useState({});
  const [deletingProvider, setDeletingProvider] = useState(null);
  const [modelTabError, setModelTabError] = useState(null);
  const [modelSaveSuccess, setModelSaveSuccess] = useState(false);

  // Connected Accounts (Codex OAuth — Device Code Flow)
  const [codexOAuthStatus, setCodexOAuthStatus] = useState({ connected: false });
  const [showCodexDisclaimer, setShowCodexDisclaimer] = useState(false);
  const [isConnectingCodex, setIsConnectingCodex] = useState(false);
  const [isDisconnectingCodex, setIsDisconnectingCodex] = useState(false);
  const [codexDeviceCode, setCodexDeviceCode] = useState(null); // { user_code, verification_url, interval }
  const [codexDeviceError, setCodexDeviceError] = useState(null);
  const [isPollingCodex, setIsPollingCodex] = useState(false);
  const codexPollRef = useRef(null);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);

  const timezones = [
    { value: '', label: t('settings.selectTimezone') },
    { group: 'Americas', options: [
      { value: 'America/New_York', label: 'Eastern Time (America/New_York)' },
      { value: 'America/Chicago', label: 'Central Time (America/Chicago)' },
      { value: 'America/Denver', label: 'Mountain Time (America/Denver)' },
      { value: 'America/Los_Angeles', label: 'Pacific Time (America/Los_Angeles)' },
      { value: 'America/Toronto', label: 'Eastern - Canada (America/Toronto)' },
      { value: 'America/Sao_Paulo', label: 'Brasília Time (America/Sao_Paulo)' },
    ]},
    { group: 'Europe', options: [
      { value: 'Europe/London', label: 'GMT (Europe/London)' },
      { value: 'Europe/Paris', label: 'CET (Europe/Paris)' },
      { value: 'Europe/Berlin', label: 'CET (Europe/Berlin)' },
    ]},
    { group: 'Asia', options: [
      { value: 'Asia/Shanghai', label: 'China Standard Time (Asia/Shanghai)' },
      { value: 'Asia/Tokyo', label: 'Japan Standard Time (Asia/Tokyo)' },
      { value: 'Asia/Hong_Kong', label: 'Hong Kong Time (Asia/Hong_Kong)' },
      { value: 'Asia/Singapore', label: 'Singapore Time (Asia/Singapore)' },
      { value: 'Asia/Kolkata', label: 'India Standard Time (Asia/Kolkata)' },
    ]},
    { group: 'Oceania', options: [
      { value: 'Australia/Sydney', label: 'Australian Eastern (Australia/Sydney)' },
    ]},
    { group: 'Other', options: [
      { value: 'UTC', label: 'UTC' },
    ]},
  ];

  const locales = [
    { value: '', label: t('settings.selectLocale') },
    { value: 'en-US', label: 'English (United States)' },
    { value: 'zh-CN', label: '中文（简体）' },
  ];

  // Sync initialTab when panel opens
  useEffect(() => {
    if (isOpen && initialTab) {
      setActiveTab(initialTab);
    }
  }, [isOpen, initialTab]);

  useEffect(() => {
    if (isOpen) {
      setIsLoading(true);
      Promise.all([loadUserData(), loadPreferencesData()])
        .finally(() => setIsLoading(false));
    }
  }, [isOpen]);

  // Load model tab data lazily when tab is selected
  useEffect(() => {
    if (isOpen && activeTab === 'model') {
      loadModelTabData();
    }
  }, [isOpen, activeTab]);

  // Cleanup device code polling on unmount or panel close
  useEffect(() => {
    if (!isOpen && codexPollRef.current) {
      clearInterval(codexPollRef.current);
      codexPollRef.current = null;
      setIsPollingCodex(false);
      setCodexDeviceCode(null);
    }
    return () => {
      if (codexPollRef.current) {
        clearInterval(codexPollRef.current);
        codexPollRef.current = null;
      }
    };
  }, [isOpen]);

  const loadUserData = async () => {
    try {
      const userData = await getCurrentUser();
      if (userData?.user) {
        setName(userData.user.name || '');
        setTimezone(userData.user.timezone || '');
        setLocale(userData.user.locale || '');
        const url = userData.user.avatar_url;
        const version = userData.user.updated_at;
        setAvatarUrl(url ? `${url}?v=${version}` : null);
      }
    } catch {
      // User data load failed - keep existing state
    }
  };

  const loadPreferencesData = async () => {
    try {
      const preferencesData = await getPreferences();
      setPreferences(preferencesData || null);
    } catch {}
  };

  const loadModelTabData = async () => {
    setModelTabError(null);
    try {
      const [modelsRes, keysRes, prefsRes, codexStatus] = await Promise.all([
        getAvailableModels(),
        getUserApiKeys(),
        getPreferences(),
        getCodexOAuthStatus(),
      ]);
      setAvailableModels(modelsRes?.models || {});
      setByokEnabled(keysRes?.byok_enabled || false);
      setByokProviders(keysRes?.providers || []);
      setPreferredModel(prefsRes?.other_preference?.preferred_model || '');
      setPreferredFlashModel(prefsRes?.other_preference?.preferred_flash_model || '');
      setCodexOAuthStatus(codexStatus || { connected: false });
    } catch {
      setModelTabError(t('settings.failedToLoadModels'));
    }
  };

  const handleModelTabSave = async () => {
    setModelTabError(null);
    setModelSaveSuccess(false);
    setIsSubmitting(true);
    try {
      // 1. Save model preferences
      await updatePreferences({
        other_preference: {
          preferred_model: preferredModel || null,
          preferred_flash_model: preferredFlashModel || null,
        },
      });

      // 2. Save any pending API key inputs
      const pendingKeys = Object.entries(keyInputs).filter(([, v]) => v?.trim());
      for (const [provider, key] of pendingKeys) {
        const result = await updateUserApiKeys({ api_keys: { [provider]: key.trim() } });
        setByokProviders(result.providers);
      }
      if (pendingKeys.length > 0) {
        setKeyInputs({});
      }

      setModelSaveSuccess(true);
      setTimeout(() => setModelSaveSuccess(false), 3000);
    } catch {
      setModelTabError(t('settings.failedToSaveSettings'));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleByokToggle = async () => {
    setModelTabError(null);
    const newValue = !byokEnabled;
    try {
      const result = await updateUserApiKeys({ byok_enabled: newValue });
      setByokEnabled(result.byok_enabled);
      setByokProviders(result.providers);
    } catch {
      setModelTabError(t('settings.failedToToggleByok'));
    }
  };

  const handleDeleteProviderKey = async (provider) => {
    setDeletingProvider(provider);
    setModelTabError(null);
    try {
      const result = await deleteUserApiKey(provider);
      setByokProviders(result.providers);
    } catch {
      setModelTabError(t('settings.failedToDeleteKey', { provider }));
    } finally {
      setDeletingProvider(null);
    }
  };

  const handleCodexConnectClick = () => {
    setShowCodexDisclaimer(true);
  };

  const handleCodexConnect = async () => {
    setShowCodexDisclaimer(false);
    setIsConnectingCodex(true);
    setModelTabError(null);
    setCodexDeviceError(null);
    try {
      const device = await initiateCodexDevice();
      setCodexDeviceCode(device);
      // Open verification URL in new tab
      window.open(device.verification_url, '_blank', 'noopener');
      // Start polling
      setIsPollingCodex(true);
      const interval = (device.interval || 5) * 1000;
      const startTime = Date.now();
      const maxDuration = 15 * 60 * 1000; // 15 minutes
      codexPollRef.current = setInterval(async () => {
        if (Date.now() - startTime > maxDuration) {
          handleCodexDeviceCancel();
          setCodexDeviceError(t('settings.codexTimeout'));
          return;
        }
        try {
          const result = await pollCodexDevice();
          if (result.success) {
            handleCodexDeviceCancel(); // stop polling
            setCodexOAuthStatus({
              connected: true,
              account_id: result.account_id,
              email: result.email,
              plan_type: result.plan_type,
            });
          }
          // result.pending → keep polling
        } catch {
          handleCodexDeviceCancel();
          setCodexDeviceError(t('settings.codexPollFailed'));
        }
      }, interval);
    } catch {
      setModelTabError(t('settings.codexFlowFailed'));
    } finally {
      setIsConnectingCodex(false);
    }
  };

  const handleCodexDeviceCancel = () => {
    if (codexPollRef.current) {
      clearInterval(codexPollRef.current);
      codexPollRef.current = null;
    }
    setIsPollingCodex(false);
    setCodexDeviceCode(null);
    setCodexDeviceError(null);
  };

  const handleCodexDisconnect = async () => {
    setIsDisconnectingCodex(true);
    setModelTabError(null);
    try {
      await disconnectCodexOAuth();
      setCodexOAuthStatus({ connected: false, account_id: null, email: null, plan_type: null });
    } catch {
      setModelTabError('Failed to disconnect Codex');
    } finally {
      setIsDisconnectingCodex(false);
    }
  };

  const handleAvatarChange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setIsUploadingAvatar(true);
    try {
      const { avatar_url } = await uploadAvatar(file);
      setAvatarUrl(`${avatar_url}?t=${Date.now()}`);
      refreshUser();
    } catch {
      setError(t('settings.failedToUploadAvatar'));
    } finally {
      setIsUploadingAvatar(false);
    }
  };

  const handleLocaleChange = (newLocale) => {
    setLocale(newLocale);
    // Also switch i18n language for supported UI locales
    if (newLocale === 'en-US' || newLocale === 'zh-CN') {
      i18n.changeLanguage(newLocale);
      localStorage.setItem('locale', newLocale);
    }
  };

  const handleUserInfoSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    setSaveSuccess(false);
    try {
      const userData = {};
      if (name.trim()) userData.name = name.trim();
      if (timezone) userData.timezone = timezone;
      if (locale) userData.locale = locale;
      if (Object.keys(userData).length > 0) {
        await updateCurrentUser(userData);
      }
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      setError(err.message || t('settings.failedToUpdateUser'));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleModifyPreferences = () => {
    onClose();
    if (onModifyPreferences) onModifyPreferences();
  };

  const handleStartOnboarding = () => {
    onClose();
    if (onStartOnboarding) onStartOnboarding();
  };

  const handleLogoutConfirm = () => {
    logout();
    setShowLogoutConfirm(false);
    onClose();
  };

  const handleResetConfirm = async () => {
    setIsResetting(true);
    try {
      await clearPreferences();
      setPreferences(null);
      setShowResetConfirm(false);
    } catch {
      setError(t('settings.failedToResetPreferences'));
      setShowResetConfirm(false);
    } finally {
      setIsResetting(false);
    }
  };

  // Prevent Enter key in text inputs from submitting the enclosing <form>.
  // Only the explicit submit button should trigger form submission.
  const preventEnterSubmit = (e) => {
    if (e.key === 'Enter' && e.target.tagName === 'INPUT' && e.target.type !== 'submit') {
      e.preventDefault();
    }
  };

  const handleClose = () => {
    setError(null);
    setSaveSuccess(false);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <>
      <div
        className="fixed inset-0 z-50 flex items-center justify-center"
        style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
        onClick={handleClose}
      >
        <div
          className="relative w-full max-w-2xl rounded-lg p-4 sm:p-6"
          style={{
            backgroundColor: 'var(--color-bg-elevated)',
            border: '1px solid var(--color-border-muted)',
            height: 'min(80vh, 800px)',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <button
            onClick={handleClose}
            className="absolute top-4 right-4 p-1 rounded-full transition-colors"
            style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
            onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
            onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
          >
            <X className="h-5 w-5" />
          </button>

          <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>{t('settings.title')}</h2>
              <div className="flex gap-2 mb-6 border-b" style={{ borderColor: 'var(--color-border-muted)' }}>
                <button
                  type="button"
                  onClick={() => setActiveTab('userInfo')}
                  className="px-4 py-2 text-sm font-medium"
                  style={{
                    color: activeTab === 'userInfo' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    borderBottom: activeTab === 'userInfo' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
                  }}
                >
                  {t('settings.userInfo')}
                </button>
                <button
                  type="button"
                  onClick={() => setActiveTab('preferences')}
                  className="px-4 py-2 text-sm font-medium"
                  style={{
                    color: activeTab === 'preferences' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    borderBottom: activeTab === 'preferences' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
                  }}
                >
                  {t('settings.preferences')}
                </button>
                <button
                  type="button"
                  onClick={() => setActiveTab('model')}
                  className="px-4 py-2 text-sm font-medium"
                  style={{
                    color: activeTab === 'model' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    borderBottom: activeTab === 'model' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
                  }}
                >
                  {t('settings.model')}
                </button>
              </div>

              <div style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
              {isLoading && (
                <div className="flex items-center justify-center py-8">
                  <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>{t('common.loading')}</p>
                </div>
              )}

              {!isLoading && activeTab === 'userInfo' && (
                <form onSubmit={handleUserInfoSubmit} onKeyDown={preventEnterSubmit} className="space-y-5">
                  <div className="flex items-center gap-4 mb-6 pb-6" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
                    <div
                      className="h-16 w-16 rounded-full flex items-center justify-center cursor-pointer overflow-hidden"
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
                      onChange={(e) => setName(e.target.value)}
                      placeholder={t('auth.enterName')}
                      className="w-full"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('settings.timezone')}</label>
                    <select
                      value={timezone}
                      onChange={(e) => setTimezone(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      {timezones.map((item, i) => (
                        item.value !== undefined ? (
                          <option key={i} value={item.value} style={{ backgroundColor: 'var(--color-bg-card)' }}>{item.label}</option>
                        ) : (
                          <optgroup key={i} label={item.group} style={{ backgroundColor: 'var(--color-bg-card)' }}>
                            {item.options.map((opt, j) => (
                              <option key={`${i}-${j}`} value={opt.value} style={{ backgroundColor: 'var(--color-bg-card)' }}>{opt.label}</option>
                            ))}
                          </optgroup>
                        )
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('settings.locale')}</label>
                    <select
                      value={locale}
                      onChange={(e) => handleLocaleChange(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      {locales.map((item, i) => (
                        <option key={i} value={item.value} style={{ backgroundColor: 'var(--color-bg-card)' }}>{item.label}</option>
                      ))}
                    </select>
                  </div>

                  {/* Theme Toggle */}
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{t('settings.theme')}</label>
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

                  {error && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
                    </div>
                  )}

                  <div className="flex gap-3 justify-between pt-4">
                    <button
                      type="button"
                      onClick={() => setShowLogoutConfirm(true)}
                      className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors"
                      style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
                    >
                      <LogOut className="h-4 w-4" /> {t('settings.logout')}
                    </button>
                    <div className="flex items-center gap-3">
                      {saveSuccess && (
                        <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
                      )}
                      <button type="button" onClick={handleClose} disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
                        style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
                        onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
                        onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      >
                        {t('common.close')}
                      </button>
                      <button type="submit" disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {isSubmitting ? t('common.saving') : t('common.save')}
                      </button>
                    </div>
                  </div>
                </form>
              )}

              {!isLoading && activeTab === 'preferences' && (
                <div className="space-y-5">
                  {authUser?.onboarding_completed !== true && onStartOnboarding && (
                    <div
                      className="rounded-lg px-4 py-4 flex items-center justify-between gap-3"
                      style={{
                        backgroundColor: 'rgba(97, 85, 245, 0.08)',
                        border: '1px solid rgba(97, 85, 245, 0.2)',
                      }}
                    >
                      <div>
                        <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                          {t('settings.completeProfile')}
                        </p>
                        <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                          {t('settings.completeProfileDesc')}
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={handleStartOnboarding}
                        className="shrink-0 flex items-center gap-1.5 px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {t('settings.startOnboarding')}
                      </button>
                    </div>
                  )}

                  <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('settings.preferencesDesc')}
                  </p>

                  {preferences && (preferences.risk_preference || preferences.investment_preference || preferences.agent_preference) ? (
                    <div className="space-y-4">
                      {[
                        { label: t('settings.riskTolerance'), data: preferences.risk_preference },
                        { label: t('settings.investmentStyle'), data: preferences.investment_preference },
                        { label: t('settings.agentSettings'), data: preferences.agent_preference },
                      ].filter(({ data }) => data && Object.keys(data).length > 0).map(({ label, data }) => (
                        <div key={label}>
                          <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>{label}</label>
                          <div
                            className="rounded-md px-3 py-2.5 text-sm space-y-1"
                            style={{
                              backgroundColor: 'var(--color-bg-card)',
                              border: '1px solid var(--color-border-muted)',
                            }}
                          >
                            {Object.entries(data).map(([key, value]) => (
                              value != null && value !== '' && (
                                <div key={key} className="flex gap-2">
                                  <span className="shrink-0 font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                                    {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}:
                                  </span>
                                  <span style={{ color: 'var(--color-text-primary)', wordBreak: 'break-word' }}>
                                    {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                                  </span>
                                </div>
                              )
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div
                      className="rounded-md px-4 py-6 text-center"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                      }}
                    >
                      <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('settings.noPreferencesYet')}
                      </p>
                    </div>
                  )}

                  {error && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
                    </div>
                  )}

                  <div className="flex gap-3 justify-between pt-4" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                    <button
                      type="button"
                      onClick={() => setShowResetConfirm(true)}
                      className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors"
                      style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
                    >
                      <Trash2 className="h-4 w-4" /> {t('settings.resetPreferences')}
                    </button>
                    <div className="flex items-center gap-3">
                      <button type="button" onClick={handleClose}
                        className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
                        style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
                        onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
                        onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      >
                        {t('common.close')}
                      </button>
                      <button
                        type="button"
                        onClick={handleModifyPreferences}
                        className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        <MessageSquareText className="h-4 w-4" /> {t('settings.modifyWithAgent')}
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {!isLoading && activeTab === 'model' && (
                <div className="space-y-6">
                  {/* Section 1: Model Preferences */}
                  <div>
                    {[
                      { label: t('settings.defaultModel'), desc: t('settings.defaultModelDesc'), value: preferredModel, setter: setPreferredModel },
                      { label: t('settings.flashModel'), desc: t('settings.flashModelDesc'), value: preferredFlashModel, setter: setPreferredFlashModel },
                    ].map(({ label, desc, value, setter }) => (
                      <div key={label} className="mb-4">
                        <label className="block text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>{label}</label>
                        <p className="text-xs mb-2" style={{ color: 'var(--color-text-tertiary)' }}>{desc}</p>
                        <select
                          value={value}
                          onChange={(e) => setter(e.target.value)}
                          className="w-full rounded-md px-3 py-2 text-sm"
                          style={{
                            backgroundColor: 'var(--color-bg-card)',
                            border: '1px solid var(--color-border-muted)',
                            color: 'var(--color-text-primary)',
                          }}
                          disabled={isSubmitting}
                        >
                          <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>{t('settings.systemDefault')}</option>
                          {Object.entries(availableModels).map(([provider, models]) => (
                            <optgroup key={provider} label={provider.charAt(0).toUpperCase() + provider.slice(1)} style={{ backgroundColor: 'var(--color-bg-card)' }}>
                              {models.map((m) => (
                                <option key={m} value={m} style={{ backgroundColor: 'var(--color-bg-card)' }}>{m}</option>
                              ))}
                            </optgroup>
                          ))}
                        </select>
                      </div>
                    ))}

                  </div>

                  {/* Section 2: Connected Accounts */}
                  <div style={{ borderTop: '1px solid var(--color-border-muted)', paddingTop: '16px' }}>
                    <label className="block text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
                      {t('settings.connectedAccounts', 'Connected Accounts')}
                    </label>
                    <p className="text-xs mb-3" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('settings.connectedAccountsDesc', 'Connect external accounts to use models through your existing subscriptions.')}
                    </p>

                    {/* ChatGPT Codex card */}
                    <div
                      className="rounded-lg px-4 py-3"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: `1px solid ${codexOAuthStatus.connected ? 'var(--color-success-soft)' : 'var(--color-border-muted)'}`,
                      }}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <div
                            className="h-8 w-8 rounded-md flex items-center justify-center"
                            style={{ backgroundColor: codexOAuthStatus.connected ? 'var(--color-success-soft)' : 'var(--color-accent-soft)' }}
                          >
                            <Link2 className="h-4 w-4" style={{ color: codexOAuthStatus.connected ? 'var(--color-success)' : 'var(--color-accent-primary)' }} />
                          </div>
                          <div>
                            <div className="flex items-center gap-2">
                              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>ChatGPT Codex</span>
                              {codexOAuthStatus.connected && codexOAuthStatus.plan_type && (
                                <span
                                  className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium"
                                  style={{ backgroundColor: 'var(--color-success-soft)', color: 'var(--color-success)' }}
                                >
                                  {codexOAuthStatus.plan_type}
                                </span>
                              )}
                            </div>
                            {codexOAuthStatus.connected ? (
                              <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{codexOAuthStatus.email || codexOAuthStatus.account_id}</p>
                            ) : (
                              <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                                {t('settings.codexDesc', 'Use Codex models with your ChatGPT subscription')}
                              </p>
                            )}
                          </div>
                        </div>
                        <div>
                          {codexOAuthStatus.connected ? (
                            <button
                              type="button"
                              onClick={handleCodexDisconnect}
                              disabled={isDisconnectingCodex}
                              className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                              style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
                            >
                              <Unlink className="h-3 w-3" />
                              {isDisconnectingCodex ? t('common.loading', 'Loading...') : t('settings.disconnect', 'Disconnect')}
                            </button>
                          ) : !codexDeviceCode ? (
                            <button
                              type="button"
                              onClick={handleCodexConnectClick}
                              disabled={isConnectingCodex}
                              className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                              style={{
                                backgroundColor: isConnectingCodex ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                                color: 'var(--color-text-on-accent)',
                              }}
                            >
                              <Link2 className="h-3 w-3" />
                              {isConnectingCodex ? t('common.loading', 'Loading...') : t('settings.connect', 'Connect')}
                            </button>
                          ) : null}
                        </div>
                      </div>

                      {/* Device code dialog — shown while waiting for user approval */}
                      {codexDeviceCode && !codexOAuthStatus.connected && (
                        <div className="mt-3 pt-3" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                          <p className="text-xs mb-2" style={{ color: 'var(--color-text-secondary)' }}>
                            {t('settings.codexVisit')} <a href={codexDeviceCode.verification_url} target="_blank" rel="noopener noreferrer" className="underline" style={{ color: 'var(--color-accent-primary)' }}>{codexDeviceCode.verification_url}</a> {t('settings.codexEnterCode')}
                          </p>
                          <div className="flex items-center gap-2 mb-2">
                            <code
                              className="text-lg font-mono font-bold tracking-widest px-3 py-1.5 rounded-md select-all"
                              style={{
                                backgroundColor: 'var(--color-bg-elevated)',
                                border: '1px solid var(--color-border-muted)',
                                color: 'var(--color-text-primary)',
                                letterSpacing: '0.15em',
                              }}
                            >
                              {codexDeviceCode.user_code}
                            </code>
                            <button
                              type="button"
                              onClick={() => navigator.clipboard.writeText(codexDeviceCode.user_code)}
                              className="p-1.5 rounded-md transition-colors hover:opacity-80"
                              style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)' }}
                              title={t('common.copy', 'Copy')}
                            >
                              <ClipboardCopy className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                            </button>
                            {isPollingCodex && (
                              <span className="text-xs animate-pulse" style={{ color: 'var(--color-text-tertiary)' }}>
                                {t('settings.codexWaitingApproval')}
                              </span>
                            )}
                          </div>
                          <button
                            type="button"
                            onClick={handleCodexDeviceCancel}
                            className="px-3 py-1.5 rounded-md text-xs font-medium"
                            style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'transparent' }}
                          >
                            {t('common.cancel', 'Cancel')}
                          </button>
                          {codexDeviceError && (
                            <p className="text-xs mt-1.5" style={{ color: 'var(--color-loss)' }}>{codexDeviceError}</p>
                          )}
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Section 3: BYOK */}
                  <div style={{ borderTop: '1px solid var(--color-border-muted)', paddingTop: '16px' }}>
                    <div className="flex items-center justify-between mb-2">
                      <div>
                        <div className="flex items-center gap-1.5">
                          <label className="block text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.byok')}</label>
                          <div className="relative group">
                            <HelpCircle className="h-3.5 w-3.5 cursor-help" style={{ color: 'var(--color-text-tertiary)' }} />
                            <div
                              className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 rounded-lg text-xs leading-relaxed whitespace-normal hidden group-hover:block z-50"
                              style={{
                                width: '240px',
                                backgroundColor: 'var(--color-bg-elevated)',
                                border: '1px solid var(--color-border-elevated)',
                                color: 'var(--color-text-secondary)',
                                boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                              }}
                            >
                              {t('settings.byokTooltip')}
                            </div>
                          </div>
                        </div>
                        <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                          {t('settings.byokDesc')}
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={handleByokToggle}
                        className="relative inline-flex h-6 w-11 items-center rounded-full transition-colors"
                        style={{
                          backgroundColor: byokEnabled ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
                        }}
                      >
                        <span
                          className="inline-block h-4 w-4 rounded-full bg-white transition-transform"
                          style={{ transform: byokEnabled ? 'translateX(22px)' : 'translateX(4px)' }}
                        />
                      </button>
                    </div>

                    {byokEnabled && (
                      <div className="space-y-3 mt-4">
                        {byokProviders.map((prov) => (
                          <div key={prov.provider} className="flex items-center gap-2">
                            <span className="text-xs font-medium w-24 shrink-0" style={{ color: 'var(--color-text-secondary)' }}>
                              {prov.display_name || prov.provider}
                            </span>
                            <div className="flex-1 relative">
                              <input
                                type={visibleKeys[prov.provider] ? 'text' : 'password'}
                                value={keyInputs[prov.provider] || ''}
                                onChange={(e) => setKeyInputs((prev) => ({ ...prev, [prov.provider]: e.target.value }))}
                                placeholder={prov.has_key ? prov.masked_key : t('settings.enterApiKey')}
                                className="w-full rounded-md px-3 py-1.5 pr-8 text-sm"
                                style={{
                                  backgroundColor: 'var(--color-bg-card)',
                                  border: '1px solid var(--color-border-muted)',
                                  color: 'var(--color-text-primary)',
                                }}
                              />
                              <button
                                type="button"
                                onClick={() => setVisibleKeys((prev) => ({ ...prev, [prov.provider]: !prev[prov.provider] }))}
                                className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5"
                                style={{ color: 'var(--color-text-tertiary)' }}
                              >
                                {visibleKeys[prov.provider] ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
                              </button>
                            </div>
                            {prov.has_key && (
                              <button
                                type="button"
                                onClick={() => handleDeleteProviderKey(prov.provider)}
                                disabled={deletingProvider === prov.provider}
                                className="p-1.5 rounded-md shrink-0 transition-colors"
                                style={{ color: 'var(--color-loss)' }}
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                              </button>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {modelTabError && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{modelTabError}</p>
                    </div>
                  )}

                  <div className="flex items-center gap-3 justify-end pt-4" style={{ borderTop: '1px solid var(--color-border-muted)', marginTop: '8px', paddingTop: '16px' }}>
                    {modelSaveSuccess && (
                      <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
                    )}
                    <button type="button" onClick={handleClose}
                      className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
                      style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
                      onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
                      onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                    >
                      {t('common.close')}
                    </button>
                    <button
                      type="button"
                      onClick={handleModelTabSave}
                      disabled={isSubmitting}
                      className="px-4 py-2 rounded-md text-sm font-medium"
                      style={{
                        backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                        color: 'var(--color-text-on-accent)',
                      }}
                    >
                      {isSubmitting ? t('common.saving') : t('common.save')}
                    </button>
                  </div>
                </div>
              )}
              </div>
        </div>
      </div>

      <ConfirmDialog
        open={showLogoutConfirm}
        title={t('settings.logout')}
        message={t('settings.logoutConfirmMsg')}
        confirmLabel={t('settings.logout')}
        onConfirm={handleLogoutConfirm}
        onOpenChange={setShowLogoutConfirm}
      />

      <ConfirmDialog
        open={showResetConfirm}
        title={t('settings.resetPreferences')}
        message={t('settings.resetConfirmMsg')}
        confirmLabel={isResetting ? t('settings.resetting') : t('settings.resetPreferences')}
        onConfirm={handleResetConfirm}
        onOpenChange={setShowResetConfirm}
      />

      {/* Codex OAuth Disclaimer Dialog */}
      <Dialog open={showCodexDisclaimer} onOpenChange={setShowCodexDisclaimer}>
        <DialogContent
          className="sm:max-w-md border"
          style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
        >
          <DialogHeader>
            <DialogTitle className="title-font flex items-center gap-2" style={{ color: 'var(--color-text-primary)' }}>
              <Link2 className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} />
              {t('settings.codexConnectTitle')}
            </DialogTitle>
          </DialogHeader>

          <div className="space-y-4">
            {/* Steps */}
            <div className="space-y-3">
              <p className="text-xs font-medium uppercase tracking-wide" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.codexHowItWorks')}</p>

              <div className="flex gap-3 items-start">
                <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>1</div>
                <div>
                  <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.codexStep1Title')}</p>
                  <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.codexStep1Desc')}</p>
                </div>
              </div>

              <div className="flex gap-3 items-start">
                <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>2</div>
                <div>
                  <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.codexStep2Title')}</p>
                  <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.codexStep2Desc')}</p>
                </div>
              </div>

              <div className="flex gap-3 items-start">
                <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>3</div>
                <div>
                  <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.codexStep3Title')}</p>
                  <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.codexStep3Desc')}</p>
                </div>
              </div>
            </div>

            {/* Disclaimer */}
            <div className="rounded-lg p-3" style={{ backgroundColor: 'var(--color-bg-sunken, var(--color-bg-card))', border: '1px solid var(--color-border-muted)' }}>
              <div className="flex gap-2 items-start">
                <Shield className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
                <div>
                  <p className="text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>{t('settings.codexSecurityTitle')}</p>
                  <p className="text-[11px] leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('settings.codexSecurityDesc')}
                  </p>
                  <p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('settings.codexDisclaimerDesc')}
                  </p>
                </div>
              </div>
            </div>
          </div>

          <DialogFooter className="gap-2 pt-2">
            <button
              type="button"
              onClick={() => setShowCodexDisclaimer(false)}
              className="px-3 py-1.5 rounded text-sm border"
              style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
              onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
              onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
            >
              {t('common.cancel', 'Cancel')}
            </button>
            <button
              type="button"
              onClick={handleCodexConnect}
              className="px-4 py-1.5 rounded text-sm font-medium hover:opacity-90 flex items-center gap-1.5"
              style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
            >
              <ExternalLink className="h-3.5 w-3.5" />
              {t('settings.codexProceed')}
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

    </>
  );
}

export default UserConfigPanel;
