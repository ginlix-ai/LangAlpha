import React, { useState, useEffect, useRef, useCallback } from 'react';
import { User, LogOut, Trash2, MessageSquareText, Sun, Moon, Monitor, Link2, Unlink, ExternalLink, Shield, ClipboardCopy, Search, Pin, Settings2 } from 'lucide-react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { updateCurrentUser, clearPreferences, uploadAvatar, getUserApiKeys, initiateCodexDevice, pollCodexDevice, getCodexOAuthStatus, disconnectCodexOAuth, initiateClaudeOAuth, submitClaudeCallback, getClaudeOAuthStatus, disconnectClaudeOAuth } from '@/pages/Dashboard/utils/api';
import { useAuth } from '@/contexts/AuthContext';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useTheme } from '@/contexts/ThemeContext';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import { getFlashWorkspace } from '@/pages/ChatAgent/utils/api';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import type { ByokProvider, CustomModelEntry } from '@/components/model/types';
import { useAllModels } from '@/hooks/useAllModels';
import { useDebouncedSave } from '@/hooks/useDebouncedSave';
import './Settings.css';

interface CodexDeviceCode {
  user_code: string;
  verification_url: string;
  interval?: number;
}

interface OAuthStatus {
  connected: boolean;
  account_id?: string | null;
  email?: string | null;
  plan_type?: string | null;
}

// TODO: type properly — depends on backend preferences shape
interface Preferences {
  risk_preference?: Record<string, unknown>;
  investment_preference?: Record<string, unknown>;
  agent_preference?: Record<string, unknown>;
  other_preference?: Record<string, unknown>;
}

interface TimezoneOption {
  value: string;
  label: string;
}

interface TimezoneGroup {
  group: string;
  options: TimezoneOption[];
}

type TimezoneEntry = TimezoneOption | TimezoneGroup;

function Settings() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const { logout } = useAuth();
  const { user: authUser, isLoading: isUserLoading } = useUser();
  const { preferences: prefsData, isLoading: isPrefsLoading } = usePreferences();
  const updatePrefsMutation = useUpdatePreferences();
  const queryClient = useQueryClient();
  const { theme: _theme, preference, setTheme: setThemePref } = useTheme();
  const { models: visibleModels, modelAccessMap, systemDefaults: hookSystemDefaults, validModelNames, isLoading: isModelsLoading } = useAllModels();
  const { t, i18n } = useTranslation();

  const tabParam = searchParams.get('tab') || 'userInfo';
  const [activeTab, setActiveTab] = useState(tabParam);
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');

  const [preferences, setPreferences] = useState<Preferences | null>(null);

  // Model tab state
  const [preferredModel, setPreferredModel] = useState('');
  const [preferredFlashModel, setPreferredFlashModel] = useState('');
  const [starredModels, setStarredModels] = useState<string[]>([]);
  const [byokProviders, setByokProviders] = useState<ByokProvider[]>([]);
  const [modelTabError, setModelTabError] = useState<string | null>(null);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [modelPickerSearch, setModelPickerSearch] = useState('');
  const modelPickerRef = useRef<HTMLDivElement>(null);

  // Other models state
  const [summarizationModel, setSummarizationModel] = useState('');
  const [fetchModel, setFetchModel] = useState('');
  const [fallbackModels, setFallbackModels] = useState<string[]>([]);

  // Custom Models state
  const [customModels, setCustomModels] = useState<CustomModelEntry[]>([]);

  // Connected Accounts (Codex OAuth — Device Code Flow)
  const [codexOAuthStatus, setCodexOAuthStatus] = useState<OAuthStatus>({ connected: false });
  const [showCodexDisclaimer, setShowCodexDisclaimer] = useState(false);
  const [isConnectingCodex, setIsConnectingCodex] = useState(false);
  const [isDisconnectingCodex, setIsDisconnectingCodex] = useState(false);
  const [codexDeviceCode, setCodexDeviceCode] = useState<CodexDeviceCode | null>(null);
  const [codexDeviceError, setCodexDeviceError] = useState<string | null>(null);
  const [isPollingCodex, setIsPollingCodex] = useState(false);
  const codexPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Connected Accounts (Claude OAuth — PKCE Authorization Code Flow)
  const [claudeOAuthStatus, setClaudeOAuthStatus] = useState<OAuthStatus>({ connected: false });
  const [showClaudeDisclaimer, setShowClaudeDisclaimer] = useState(false);
  const [isConnectingClaude, setIsConnectingClaude] = useState(false);
  const [isDisconnectingClaude, setIsDisconnectingClaude] = useState(false);
  const [claudeAuthorizeUrl, setClaudeAuthorizeUrl] = useState<string | null>(null);
  const [claudeCallbackInput, setClaudeCallbackInput] = useState('');
  const [claudeError, setClaudeError] = useState<string | null>(null);
  const [isSubmittingClaudeCallback, setIsSubmittingClaudeCallback] = useState(false);

  const isLoading = isUserLoading || isPrefsLoading;
  const [error, setError] = useState<string | null>(null);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);

  // Close starred-model picker on click outside
  useEffect(() => {
    if (!showModelPicker) return;
    const handler = (e: MouseEvent) => {
      if (modelPickerRef.current && !modelPickerRef.current.contains(e.target as Node)) {
        setShowModelPicker(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showModelPicker]);

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

  // Sync tab with URL search params
  const handleTabChange = (tab: string) => {
    setActiveTab(tab);
    setSearchParams({ tab }, { replace: true });
  };

  // Sync from URL on mount / back-forward navigation
  useEffect(() => {
    const urlTab = searchParams.get('tab');
    if (urlTab && urlTab !== activeTab) {
      setActiveTab(urlTab);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

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

  // Load model tab data lazily when tab is selected and hooks are ready
  useEffect(() => {
    if (activeTab === 'model' && !isModelsLoading) {
      loadModelTabData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, isModelsLoading]);

  // Cleanup device code polling on unmount
  useEffect(() => {
    return () => {
      if (codexPollRef.current) {
        clearInterval(codexPollRef.current);
        codexPollRef.current = null;
      }
    };
  }, []);

  // Sync local preferences state from usePreferences hook
  useEffect(() => {
    if (prefsData) {
      setPreferences(prefsData);
    }
  }, [prefsData]);

  const loadModelTabData = async () => {
    setModelTabError(null);
    try {
      const [keysRes, codexStatus, claudeStatus] = await Promise.all([
        getUserApiKeys(),
        getCodexOAuthStatus(),
        getClaudeOAuthStatus(),
      ]) as [Record<string, unknown>, OAuthStatus, OAuthStatus];
      setByokProviders((keysRes?.providers as ByokProvider[]) || []);
      const otherPref = (prefsData as Preferences | null)?.other_preference as Record<string, unknown> | undefined;
      setPreferredModel((otherPref?.preferred_model as string) || '');
      setPreferredFlashModel((otherPref?.preferred_flash_model as string) || '');
      setStarredModels((otherPref?.starred_models as string[]) || []);
      setCustomModels((otherPref?.custom_models as CustomModelEntry[]) || []);
      setSummarizationModel((otherPref?.summarization_model as string) || '');
      setFetchModel((otherPref?.fetch_model as string) || '');
      setFallbackModels((otherPref?.fallback_models as string[]) || (hookSystemDefaults?.fallback_models as string[]) || []);
      setCodexOAuthStatus(codexStatus || { connected: false });
      setClaudeOAuthStatus(claudeStatus || { connected: false });
    } catch {
      setModelTabError(t('settings.failedToLoadModels'));
    }
  };

  // Refs to hold latest model state for the debounced save callback
  const modelStateRef = useRef({
    preferredModel, preferredFlashModel, starredModels, customModels,
    summarizationModel, fetchModel, fallbackModels, byokProviders,
  });
  modelStateRef.current = {
    preferredModel, preferredFlashModel, starredModels, customModels,
    summarizationModel, fetchModel, fallbackModels, byokProviders,
  };

  const saveModelPrefs = useCallback(async () => {
    const s = modelStateRef.current;
    const customProvidersList = s.byokProviders
      .filter(p => p.is_custom)
      .map(p => {
        const entry: Record<string, unknown> = { name: p.provider, parent_provider: p.parent_provider };
        if (p.use_response_api) entry.use_response_api = true;
        return entry;
      });
    const cleanStarred = s.starredModels.filter(m => validModelNames.has(m));
    const cleanFallback = s.fallbackModels.filter(m => validModelNames.has(m));
    const activeProviderKeys = new Set(s.byokProviders.filter(p => p.has_key).map(p => p.provider));
    const cleanCustomProviders = customProvidersList.filter(cp => activeProviderKeys.has(cp.name as string));
    const cleanCustomModels = s.customModels.filter(cm => activeProviderKeys.has(cm.provider) || validModelNames.has(cm.name));
    const cleanModelRef = (val: string) => validModelNames.has(val) ? val : null;

    await updatePrefsMutation.mutateAsync({
      other_preference: {
        preferred_model: s.preferredModel ? cleanModelRef(s.preferredModel) : null,
        preferred_flash_model: s.preferredFlashModel ? cleanModelRef(s.preferredFlashModel) : null,
        starred_models: cleanStarred.length > 0 ? cleanStarred : null,
        custom_models: cleanCustomModels.length > 0 ? cleanCustomModels : null,
        custom_providers: cleanCustomProviders.length > 0 ? cleanCustomProviders : null,
        summarization_model: s.summarizationModel ? cleanModelRef(s.summarizationModel) : null,
        fetch_model: s.fetchModel ? cleanModelRef(s.fetchModel) : null,
        fallback_models: cleanFallback,
      },
    });
  }, [validModelNames, updatePrefsMutation]);

  const { trigger: triggerModelSave, status: modelSaveStatus } = useDebouncedSave(saveModelPrefs, 500);


  const handleCodexConnectClick = () => {
    setShowCodexDisclaimer(true);
  };

  const handleCodexConnect = async () => {
    setShowCodexDisclaimer(false);
    setIsConnectingCodex(true);
    setModelTabError(null);
    setCodexDeviceError(null);
    try {
      const device = await initiateCodexDevice() as unknown as CodexDeviceCode;
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
          const result = await pollCodexDevice() as Record<string, unknown>;
          if (result.success) {
            handleCodexDeviceCancel(); // stop polling
            setCodexOAuthStatus({
              connected: true,
              account_id: result.account_id as string,
              email: result.email as string,
              plan_type: result.plan_type as string,
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
      queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
      queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
    } catch {
      setModelTabError('Failed to disconnect Codex');
    } finally {
      setIsDisconnectingCodex(false);
    }
  };

  // --- Claude OAuth handlers ---

  const handleClaudeConnectClick = () => {
    setShowClaudeDisclaimer(true);
  };

  const handleClaudeConnect = async () => {
    setShowClaudeDisclaimer(false);
    setIsConnectingClaude(true);
    setModelTabError(null);
    setClaudeError(null);
    try {
      const result = await initiateClaudeOAuth() as Record<string, unknown>;
      setClaudeAuthorizeUrl(result.authorize_url as string);
      // Open authorization page in new tab
      window.open(result.authorize_url as string, '_blank', 'noopener');
    } catch {
      setModelTabError(t('settings.claudeConnectFailed', 'Failed to initiate Claude OAuth'));
    } finally {
      setIsConnectingClaude(false);
    }
  };

  const handleClaudeCallbackSubmit = async () => {
    if (!claudeCallbackInput.trim()) return;
    setIsSubmittingClaudeCallback(true);
    setClaudeError(null);
    try {
      const result = await submitClaudeCallback(claudeCallbackInput.trim()) as Record<string, unknown>;
      if (result.success) {
        setClaudeAuthorizeUrl(null);
        setClaudeCallbackInput('');
        setClaudeOAuthStatus({
          connected: true,
          account_id: (result.account_id as string) || '',
          email: (result.email as string) || null,
          plan_type: (result.plan_type as string) || null,
        });
      }
    } catch (e: unknown) {
      const axiosError = e as { response?: { data?: { detail?: string } } };
      setClaudeError(axiosError.response?.data?.detail || t('settings.claudePasteError', 'Failed to exchange code. Please try again.'));
    } finally {
      setIsSubmittingClaudeCallback(false);
    }
  };

  const handleClaudeCancel = () => {
    setClaudeAuthorizeUrl(null);
    setClaudeCallbackInput('');
    setClaudeError(null);
  };

  const handleClaudeDisconnect = async () => {
    setIsDisconnectingClaude(true);
    setModelTabError(null);
    try {
      await disconnectClaudeOAuth();
      setClaudeOAuthStatus({ connected: false, account_id: null, email: null, plan_type: null });
      queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
      queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
    } catch {
      setModelTabError('Failed to disconnect Claude');
    } finally {
      setIsDisconnectingClaude(false);
    }
  };

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

  const saveUserInfo = useCallback(async () => {
    setError(null);
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
    triggerUserInfoSave();
  };

  const handleTimezoneChange = (value: string) => {
    setTimezone(value);
    userInfoRef.current = { ...userInfoRef.current, timezone: value };
    flushUserInfoSave();
  };

  const handleLocaleChange = (newLocale: string) => {
    setLocale(newLocale);
    if (newLocale === 'en-US' || newLocale === 'zh-CN') {
      i18n.changeLanguage(newLocale);
      localStorage.setItem('locale', newLocale);
    }
    userInfoRef.current = { ...userInfoRef.current, locale: newLocale };
    flushUserInfoSave();
  };

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

  const handleModifyPreferences = async () => {
    try {
      const flashWs = await getFlashWorkspace();
      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId: flashWs.workspace_id,
          isModifyingPreferences: true,
          agentMode: 'flash',
          workspaceStatus: 'flash',
        },
      });
    } catch (err) {
      console.error('Error navigating to modify preferences:', err);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('dashboard.failedPrefUpdate'),
      });
    }
  };

  const handleStartOnboarding = async () => {
    try {
      const flashWs = await getFlashWorkspace();
      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId: flashWs.workspace_id,
          isOnboarding: true,
          agentMode: 'flash',
          workspaceStatus: 'flash',
        },
      });
    } catch (err) {
      console.error('Error setting up onboarding:', err);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('dashboard.failedOnboarding'),
      });
    }
  };

  const handleLogoutConfirm = () => {
    logout();
    setShowLogoutConfirm(false);
  };

  const handleResetConfirm = async () => {
    setIsResetting(true);
    try {
      await clearPreferences();
      setPreferences(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
      setShowResetConfirm(false);
    } catch {
      setError(t('settings.failedToResetPreferences'));
      setShowResetConfirm(false);
    } finally {
      setIsResetting(false);
    }
  };

  // Auto-clean stale starred/fallback models on load
  useEffect(() => {
    if (validModelNames.size === 0) return;
    const cleanS = starredModels.filter(m => validModelNames.has(m));
    if (cleanS.length !== starredModels.length) setStarredModels(cleanS);
    const cleanF = fallbackModels.filter(m => validModelNames.has(m));
    if (cleanF.length !== fallbackModels.length) setFallbackModels(cleanF);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [validModelNames]);

  return (
    <div className="settings-page">
      <div className="settings-container">
        <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>{t('settings.title')}</h2>
        <div className="flex gap-2 mb-6 border-b overflow-x-auto settings-tab-bar" style={{ borderColor: 'var(--color-border-muted)' }}>
          <button
            type="button"
            onClick={() => handleTabChange('userInfo')}
            className="px-4 py-2 text-sm font-medium whitespace-nowrap flex-shrink-0"
            style={{
              color: activeTab === 'userInfo' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              borderBottom: activeTab === 'userInfo' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
            }}
          >
            {t('settings.userInfo')}
          </button>
          <button
            type="button"
            onClick={() => handleTabChange('preferences')}
            className="px-4 py-2 text-sm font-medium whitespace-nowrap flex-shrink-0"
            style={{
              color: activeTab === 'preferences' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              borderBottom: activeTab === 'preferences' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
            }}
          >
            {t('settings.preferences')}
          </button>
          <button
            type="button"
            onClick={() => handleTabChange('model')}
            className="px-4 py-2 text-sm font-medium whitespace-nowrap flex-shrink-0"
            style={{
              color: activeTab === 'model' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              borderBottom: activeTab === 'model' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
            }}
          >
            {t('settings.model')}
          </button>
        </div>

        <div className="settings-content">
          {isLoading && (
            <div className="flex items-center justify-center py-8">
              <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>{t('common.loading')}</p>
            </div>
          )}

          {!isLoading && activeTab === 'userInfo' && (
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
                <button
                  type="button"
                  onClick={handleVoiceInputToggle}
                  className="relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none"
                  style={{
                    backgroundColor: (prefsData as any)?.other_preference?.voice_input_enabled ? 'var(--color-accent-primary)' : 'var(--color-bg-elevated)',
                    borderColor: 'var(--color-border-muted)',
                  }}
                >
                  <span
                    className="pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out"
                    style={{
                      transform: (prefsData as any)?.other_preference?.voice_input_enabled ? 'translateX(16px)' : 'translateX(0)',
                    }}
                  />
                </button>
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
          )}

          {!isLoading && activeTab === 'preferences' && (
            <div className="space-y-5">
              {authUser?.onboarding_completed !== true && (
                <div
                  className="rounded-lg px-4 py-4 flex items-center justify-between gap-3"
                  style={{
                    backgroundColor: 'hsl(var(--primary) / 0.08)',
                    border: '1px solid hsl(var(--primary) / 0.2)',
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
                  ].filter((item): item is { label: string; data: Record<string, unknown> } => !!item.data && Object.keys(item.data).length > 0).map(({ label, data }) => (
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
                {/* Default + Flash model selectors */}
                <ModelTierConfig
                  models={visibleModels}
                  primaryModel={preferredModel}
                  onPrimaryModelChange={(v) => { setPreferredModel(v); triggerModelSave(); }}
                  flashModel={preferredFlashModel}
                  onFlashModelChange={(v) => { setPreferredFlashModel(v); triggerModelSave(); }}
                  showAdvanced
                  advancedModels={{
                    summarizationModel: summarizationModel,
                    fetchModel: fetchModel,
                    fallbackModels: fallbackModels,
                  }}
                  onAdvancedModelsChange={(models) => {
                    if (models.summarizationModel !== undefined) setSummarizationModel(models.summarizationModel);
                    if (models.fetchModel !== undefined) setFetchModel(models.fetchModel);
                    if (models.fallbackModels !== undefined) setFallbackModels(models.fallbackModels);
                    triggerModelSave();
                  }}
                  systemDefaults={hookSystemDefaults ?? undefined}
                  modelAccess={modelAccessMap}
                />

                {/* Quick-access models — compact strip */}
                <div ref={modelPickerRef} style={{ marginTop: '16px' }}>
                <div className="flex flex-col gap-1.5">
                  <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {t('settings.starredModels')}
                  </label>
                  <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('settings.starredModelsDesc')}
                  </p>
                  <div className="flex flex-wrap items-center gap-1.5">
                    {starredModels.filter(m => validModelNames.has(m)).map((key) => (
                      <span
                        key={key}
                        className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs"
                        style={{
                          background: 'var(--color-bg-surface)',
                          border: '1px solid var(--color-border-default)',
                          color: 'var(--color-text-secondary)',
                        }}
                      >
                        {key}
                        <button
                          type="button"
                          onClick={() => { setStarredModels(prev => prev.filter(k => k !== key)); triggerModelSave(); }}
                          className="ml-0.5 hover:opacity-70"
                          style={{ color: 'var(--color-text-tertiary)' }}
                          aria-label={`Remove ${key}`}
                        >
                          &times;
                        </button>
                      </span>
                    ))}
                    <button
                      type="button"
                      onClick={() => { setShowModelPicker(v => !v); setModelPickerSearch(''); }}
                      className="inline-flex items-center px-2 py-1 rounded text-xs font-medium"
                      style={{
                        border: '1px dashed var(--color-border-default)',
                        color: 'var(--color-accent-primary)',
                      }}
                    >
                      + {t('settings.addModels', 'Add')}
                    </button>
                  </div>
                </div>

                {/* Collapsible model picker — hidden by default (inside ref for click-outside) */}
                {showModelPicker && (
                  <div
                    className="mt-3 rounded-lg overflow-hidden"
                    style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-card)' }}
                  >
                    {/* Search */}
                    <div className="px-3 pt-3 pb-2">
                      <div className="relative">
                        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                        <input
                          type="text"
                          value={modelPickerSearch}
                          onChange={(e) => setModelPickerSearch(e.target.value)}
                          placeholder={t('common.search')}
                          className="w-full rounded-md pl-8 pr-3 py-1.5 text-xs"
                          style={{
                            backgroundColor: 'var(--color-bg-elevated)',
                            border: '1px solid var(--color-border-muted)',
                            color: 'var(--color-text-primary)',
                          }}
                          autoFocus
                        />
                      </div>
                    </div>
                    {/* Provider groups */}
                    <div className="px-1 pb-1 max-h-[280px] overflow-y-auto">
                      {Object.entries(visibleModels).map(([provider, providerData]) => {
                        const models: string[] = providerData?.models || [];
                        const query = modelPickerSearch.toLowerCase();
                        const filtered = query
                          ? models.filter(m => m.toLowerCase().includes(query))
                          : models;
                        if (filtered.length === 0) return null;
                        const displayName = providerData?.display_name || provider.charAt(0).toUpperCase() + provider.slice(1);
                        return (
                          <div key={provider} className="mb-1">
                            <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
                              {displayName}
                            </div>
                            {filtered.map((m) => {
                              const isStarred = starredModels.includes(m);
                              return (
                                <button
                                  key={m}
                                  type="button"
                                  onClick={() => { setStarredModels(prev =>
                                    prev.includes(m) ? prev.filter(k => k !== m) : [...prev, m]
                                  ); triggerModelSave(); }}
                                  className="w-full flex items-center justify-between px-2 py-1.5 rounded-md text-xs transition-colors"
                                  style={{
                                    color: isStarred ? 'var(--color-accent-light)' : 'var(--color-text-primary)',
                                    backgroundColor: isStarred ? 'var(--color-accent-soft)' : 'transparent',
                                  }}
                                  onMouseEnter={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'var(--color-bg-elevated)'; }}
                                  onMouseLeave={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'transparent'; }}
                                >
                                  <span>{m}</span>
                                  {isStarred && <Pin className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                                </button>
                              );
                            })}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
                </div>
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

                {/* Claude OAuth card */}
                <div
                  className="rounded-lg px-4 py-3 mt-2"
                  style={{
                    backgroundColor: 'var(--color-bg-card)',
                    border: `1px solid ${claudeOAuthStatus.connected ? 'var(--color-success-soft)' : 'var(--color-border-muted)'}`,
                  }}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div
                        className="h-8 w-8 rounded-md flex items-center justify-center"
                        style={{ backgroundColor: claudeOAuthStatus.connected ? 'var(--color-success-soft)' : 'var(--color-accent-soft)' }}
                      >
                        <Link2 className="h-4 w-4" style={{ color: claudeOAuthStatus.connected ? 'var(--color-success)' : 'var(--color-accent-primary)' }} />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>Claude Code</span>
                          {claudeOAuthStatus.connected && claudeOAuthStatus.plan_type && (
                            <span
                              className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium"
                              style={{ backgroundColor: 'var(--color-success-soft)', color: 'var(--color-success)' }}
                            >
                              {claudeOAuthStatus.plan_type}
                            </span>
                          )}
                        </div>
                        {claudeOAuthStatus.connected ? (
                          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{claudeOAuthStatus.email || claudeOAuthStatus.account_id || t('settings.connected', 'Connected')}</p>
                        ) : (
                          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                            {t('settings.claudeDesc', 'Use Claude models with your Anthropic subscription')}
                          </p>
                        )}
                      </div>
                    </div>
                    <div>
                      {claudeOAuthStatus.connected ? (
                        <button
                          type="button"
                          onClick={handleClaudeDisconnect}
                          disabled={isDisconnectingClaude}
                          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                          style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
                        >
                          <Unlink className="h-3 w-3" />
                          {isDisconnectingClaude ? t('common.loading', 'Loading...') : t('settings.disconnect', 'Disconnect')}
                        </button>
                      ) : !claudeAuthorizeUrl ? (
                        <button
                          type="button"
                          onClick={handleClaudeConnectClick}
                          disabled={isConnectingClaude}
                          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                          style={{
                            backgroundColor: isConnectingClaude ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                            color: 'var(--color-text-on-accent)',
                          }}
                        >
                          <Link2 className="h-3 w-3" />
                          {isConnectingClaude ? t('common.loading', 'Loading...') : t('settings.connect', 'Connect')}
                        </button>
                      ) : null}
                    </div>
                  </div>

                  {/* Paste-back input — shown after user opens authorize URL */}
                  {claudeAuthorizeUrl && !claudeOAuthStatus.connected && (
                    <div className="mt-3 pt-3" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                      <p className="text-xs mb-2" style={{ color: 'var(--color-text-secondary)' }}>
                        {t('settings.claudePastePrompt', 'After authorizing on claude.ai, paste the code shown on the page below:')}
                      </p>
                      <div className="flex items-center gap-2 mb-2">
                        <Input
                          value={claudeCallbackInput}
                          onChange={(e) => setClaudeCallbackInput(e.target.value)}
                          placeholder="code#state"
                          className="flex-1 text-xs font-mono"
                          onKeyDown={(e) => e.key === 'Enter' && handleClaudeCallbackSubmit()}
                        />
                        <button
                          type="button"
                          onClick={handleClaudeCallbackSubmit}
                          disabled={isSubmittingClaudeCallback || !claudeCallbackInput.trim()}
                          className="px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                          style={{
                            backgroundColor: isSubmittingClaudeCallback ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                            color: 'var(--color-text-on-accent)',
                          }}
                        >
                          {isSubmittingClaudeCallback ? t('common.loading', 'Loading...') : t('common.submit', 'Submit')}
                        </button>
                      </div>
                      <div className="flex items-center gap-3">
                        <a
                          href={claudeAuthorizeUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs underline"
                          style={{ color: 'var(--color-accent-primary)' }}
                        >
                          {t('settings.claudeOpenAgain', 'Open authorize page again')}
                        </a>
                        <button
                          type="button"
                          onClick={handleClaudeCancel}
                          className="px-3 py-1.5 rounded-md text-xs font-medium"
                          style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'transparent' }}
                        >
                          {t('common.cancel', 'Cancel')}
                        </button>
                      </div>
                      {claudeError && (
                        <p className="text-xs mt-1.5" style={{ color: 'var(--color-loss)' }}>{claudeError}</p>
                      )}
                    </div>
                  )}
                </div>
              </div>

              {/* Manage providers */}
              <div
                role="button"
                tabIndex={0}
                className="flex items-center justify-between gap-4 p-4 rounded-lg cursor-pointer transition-colors"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  border: '1px solid var(--color-border-default)',
                }}
                onClick={() => navigate('/setup/method')}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); navigate('/setup/method'); } }}
              >
                <div className="flex flex-col gap-1 min-w-0">
                  <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {t('settings.manageProviders', 'Manage providers')}
                  </span>
                  <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    Add or remove API keys, custom providers, and models
                  </span>
                </div>
                <Settings2 className="h-5 w-5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              </div>


              {modelTabError && (
                <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                  <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{modelTabError}</p>
                </div>
              )}

              {modelSaveStatus !== 'idle' && (
                <div className="flex items-center justify-end pt-2">
                  {modelSaveStatus === 'saving' && (
                    <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span>
                  )}
                  {modelSaveStatus === 'saved' && (
                    <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
                  )}
                  {modelSaveStatus === 'error' && (
                    <span className="text-xs" style={{ color: 'var(--color-loss)' }}>{t('settings.failedToSaveSettings')}</span>
                  )}
                </div>
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

        {/* Claude OAuth Disclaimer Dialog */}
        <Dialog open={showClaudeDisclaimer} onOpenChange={setShowClaudeDisclaimer}>
          <DialogContent
            className="sm:max-w-md border"
            style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
          >
            <DialogHeader>
              <DialogTitle className="title-font flex items-center gap-2" style={{ color: 'var(--color-text-primary)' }}>
                <Link2 className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} />
                {t('settings.claudeConnectTitle', 'Connect Claude Account')}
              </DialogTitle>
            </DialogHeader>

            <div className="space-y-4">
              {/* Steps */}
              <div className="space-y-3">
                <p className="text-xs font-medium uppercase tracking-wide" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.claudeHowItWorks', 'How it works')}</p>

                <div className="flex gap-3 items-start">
                  <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>1</div>
                  <div>
                    <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.claudeStep1Title', 'Authorize on claude.ai')}</p>
                    <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.claudeStep1Desc', 'A new tab will open to claude.ai where you sign in and authorize access.')}</p>
                  </div>
                </div>

                <div className="flex gap-3 items-start">
                  <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>2</div>
                  <div>
                    <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.claudeStep2Title', 'Copy the authorization code')}</p>
                    <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.claudeStep2Desc', 'After approval, you\'ll see a code on the page. Copy the entire value.')}</p>
                  </div>
                </div>

                <div className="flex gap-3 items-start">
                  <div className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold" style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}>3</div>
                  <div>
                    <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('settings.claudeStep3Title', 'Paste it back here')}</p>
                    <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.claudeStep3Desc', 'Paste the code into the input field to complete the connection.')}</p>
                  </div>
                </div>
              </div>

              {/* Disclaimer */}
              <div className="rounded-lg p-3" style={{ backgroundColor: 'var(--color-bg-sunken, var(--color-bg-card))', border: '1px solid var(--color-border-muted)' }}>
                <div className="flex gap-2 items-start">
                  <Shield className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
                  <div>
                    <p className="text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>{t('settings.claudeSecurityTitle', 'Security & Privacy')}</p>
                    <p className="text-[11px] leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('settings.claudeSecurityDesc', 'Your tokens are encrypted at rest. We use them only to make API calls on your behalf.')}
                    </p>
                    <p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('settings.claudeDisclaimerDesc', 'Usage will count against your Anthropic subscription. You can disconnect at any time.')}
                    </p>
                  </div>
                </div>
              </div>
            </div>

            <DialogFooter className="gap-2 pt-2">
              <button
                type="button"
                onClick={() => setShowClaudeDisclaimer(false)}
                className="px-3 py-1.5 rounded text-sm border"
                style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
                onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
                onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
              >
                {t('common.cancel', 'Cancel')}
              </button>
              <button
                type="button"
                onClick={handleClaudeConnect}
                className="px-4 py-1.5 rounded text-sm font-medium hover:opacity-90 flex items-center gap-1.5"
                style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
              >
                <ExternalLink className="h-3.5 w-3.5" />
                {t('settings.claudeProceed', 'Open claude.ai')}
              </button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

      </div>
    </div>
  );
}

export default Settings;
