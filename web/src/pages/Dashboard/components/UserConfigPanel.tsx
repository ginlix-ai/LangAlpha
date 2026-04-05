import React, { useState, useEffect, useRef } from 'react';
import { X, User, LogOut, Trash2, HelpCircle, MessageSquareText, Sun, Moon, Monitor, Link2, Unlink, ExternalLink, Shield, ClipboardCopy, Plus, Pencil, Search, Pin } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { Select } from '../../../components/ui/select';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '../../../components/ui/dialog';
import { updateCurrentUser, clearPreferences, uploadAvatar, getUserApiKeys, updateUserApiKeys, deleteUserApiKey, initiateCodexDevice, pollCodexDevice, getCodexOAuthStatus, disconnectCodexOAuth, initiateClaudeOAuth, submitClaudeCallback, getClaudeOAuthStatus, disconnectClaudeOAuth } from '../utils/api';
import { useAuth } from '../../../contexts/AuthContext';
import { useUser } from '../../../hooks/useUser';
import { usePreferences } from '../../../hooks/usePreferences';
import { useUpdatePreferences } from '../../../hooks/useUpdatePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import { useTheme } from '../../../contexts/ThemeContext';
import { useTranslation } from 'react-i18next';
import ConfirmDialog from './ConfirmDialog';
import { ProviderManager } from '@/components/model/ProviderManager';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import type { ByokProvider } from '@/components/model/types';
import { useAllModels } from '@/hooks/useAllModels';

type SettingsTab = 'userInfo' | 'preferences' | 'model';

import type { CustomModelEntry, CustomModelFormState, AddProviderFormState } from '@/components/model/types';

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

// TODO: type properly — preferences have a loosely defined shape from the backend
interface PreferencesData {
  risk_preference?: Record<string, unknown>;
  investment_preference?: Record<string, unknown>;
  agent_preference?: Record<string, unknown>;
  other_preference?: {
    preferred_model?: string;
    preferred_flash_model?: string;
    starred_models?: string[];
    custom_models?: CustomModelEntry[];
    custom_providers?: Array<{ name: string; parent_provider: string; use_response_api?: boolean }>;
  };
  [key: string]: unknown;
}

interface UserConfigPanelProps {
  isOpen: boolean;
  onClose: () => void;
  onModifyPreferences?: () => void;
  onStartOnboarding?: () => void;
  initialTab?: SettingsTab;
}

/**
 * UserConfigPanel Component
 *
 * Modal panel for logged-in users: User info (email read-only), preferences, and logout button.
 */
function UserConfigPanel({ isOpen, onClose, onModifyPreferences, onStartOnboarding, initialTab }: UserConfigPanelProps) {
  const { logout } = useAuth();
  const { user: authUser, isLoading: isUserLoading } = useUser();
  const { preferences: prefsData, isLoading: isPrefsLoading } = usePreferences();
  const updatePrefsMutation = useUpdatePreferences();
  const { models: visibleModels, modelAccessMap, validModelNames } = useAllModels();
  const queryClient = useQueryClient();
  const { theme: _theme, preference, setTheme: setThemePref } = useTheme();
  const { t, i18n } = useTranslation();
  const [activeTab, setActiveTab] = useState<SettingsTab>(initialTab || 'userInfo');
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');

  const [preferences, setPreferences] = useState<PreferencesData | null>(null);

  // Model tab state
  const [preferredModel, setPreferredModel] = useState('');
  const [preferredFlashModel, setPreferredFlashModel] = useState('');
  const [starredModels, setStarredModels] = useState<string[]>([]);
  const [byokEnabled, setByokEnabled] = useState(false);
  const [byokProviders, setByokProviders] = useState<ByokProvider[]>([]);
  const [keyInputs, setKeyInputs] = useState<Record<string, string>>({});
  const [baseUrlInputs, setBaseUrlInputs] = useState<Record<string, string>>({});
  const [selectedByokProvider, setSelectedByokProvider] = useState('');
  const [_deletingProvider, setDeletingProvider] = useState<string | null>(null);

  // Custom (sub-)providers state
  const [showAddProviderForm, setShowAddProviderForm] = useState(false);
  const [addProviderForm, setAddProviderForm] = useState<AddProviderFormState>({ name: '', parent_provider: '' });
  const [addProviderError, setAddProviderError] = useState<string | null>(null);
  const [modelTabError, setModelTabError] = useState<string | null>(null);
  const [modelSaveSuccess, setModelSaveSuccess] = useState(false);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [modelPickerSearch, setModelPickerSearch] = useState('');

  // Custom Models state
  const [customModels, setCustomModels] = useState<CustomModelEntry[]>([]);
  const [showCustomModelForm, setShowCustomModelForm] = useState(false);
  const [editingCustomModelIdx, setEditingCustomModelIdx] = useState<number | null>(null);
  const [customModelForm, setCustomModelForm] = useState<CustomModelFormState>({ name: '', model_id: '', provider: '', parameters: '', extra_body: '' });
  const [customModelError, setCustomModelError] = useState<string | null>(null);

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

  const [isSubmitting, setIsSubmitting] = useState(false);
  const isLoading = isUserLoading || isPrefsLoading;
  const [error, setError] = useState<string | null>(null);
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
      ]);
      const keysData = keysRes as { byok_enabled?: boolean; providers?: ByokProvider[] };
      setByokEnabled(keysData?.byok_enabled || false);
      setByokProviders(keysData?.providers || []);
      const initialBaseUrls: Record<string, string> = {};
      (keysData?.providers || []).forEach((p: ByokProvider) => {
        if (p.base_url) initialBaseUrls[p.provider] = p.base_url;
      });
      setBaseUrlInputs(initialBaseUrls);
      const otherPref = (prefsData as PreferencesData)?.other_preference;
      setPreferredModel(otherPref?.preferred_model || '');
      setPreferredFlashModel(otherPref?.preferred_flash_model || '');
      setStarredModels(otherPref?.starred_models || []);
      setCustomModels(otherPref?.custom_models || []);
      setCodexOAuthStatus((codexStatus as OAuthStatus) || { connected: false });
      setClaudeOAuthStatus((claudeStatus as OAuthStatus) || { connected: false });
    } catch {
      setModelTabError(t('settings.failedToLoadModels'));
    }
  };

  const handleModelTabSave = async () => {
    setModelTabError(null);
    setModelSaveSuccess(false);
    setIsSubmitting(true);
    try {
      // 1. Save model preferences (including custom models + custom providers)
      const customProvidersList = byokProviders
        .filter(p => p.is_custom)
        .map(p => {
          const entry: { name: string; parent_provider: string | undefined; use_response_api?: boolean } = { name: p.provider, parent_provider: p.parent_provider };
          if (p.use_response_api) entry.use_response_api = true;
          return entry;
        });
      await updatePrefsMutation.mutateAsync({
        other_preference: {
          preferred_model: preferredModel || null,
          preferred_flash_model: preferredFlashModel || null,
          starred_models: starredModels.length > 0 ? starredModels : null,
          custom_models: customModels.length > 0 ? customModels : null,
          custom_providers: customProvidersList.length > 0 ? customProvidersList : null,
        },
      });

      // 2. Save any pending API key inputs and base URL changes
      const pendingKeys = Object.entries(keyInputs).filter(([, v]) => v?.trim());
      const pendingBaseUrls: Record<string, string | null> = {};
      for (const [provider, url] of Object.entries(baseUrlInputs)) {
        const original = byokProviders.find(p => p.provider === provider)?.base_url || '';
        if (url !== original) pendingBaseUrls[provider] = url || null;
      }

      if (pendingKeys.length > 0 || Object.keys(pendingBaseUrls).length > 0) {
        const payload: Record<string, unknown> = {};
        if (pendingKeys.length > 0) {
          payload.api_keys = Object.fromEntries(pendingKeys.map(([p, k]) => [p, k.trim()]));
        }
        if (Object.keys(pendingBaseUrls).length > 0) {
          payload.base_urls = pendingBaseUrls;
        }
        const result = await updateUserApiKeys(payload) as { providers: ByokProvider[] };
        setByokProviders(result.providers);
        setKeyInputs({});
        queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() });
        queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
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
      const result = await updateUserApiKeys({ byok_enabled: newValue }) as { byok_enabled: boolean; providers: ByokProvider[] };
      setByokEnabled(result.byok_enabled);
      setByokProviders(result.providers);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() });
    } catch {
      setModelTabError(t('settings.failedToToggleByok'));
    }
  };

  const handleDeleteProviderKey = async (provider: string) => {
    setDeletingProvider(provider);
    setModelTabError(null);
    try {
      const result = await deleteUserApiKey(provider) as { providers: ByokProvider[] };
      setByokProviders(result.providers);
      queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() });
      queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
    } catch {
      setModelTabError(t('settings.failedToDeleteKey', { provider }));
    } finally {
      setDeletingProvider(null);
    }
  };

  const PROVIDER_NAME_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$/;

  const handleAddProviderSave = () => {
    const name = addProviderForm.name.trim();
    const parent = addProviderForm.parent_provider;
    const apiKey = (addProviderForm.api_key || '').trim();
    const baseUrl = (addProviderForm.base_url || '').trim();
    if (!name) { setAddProviderError(t('settings.providerNameRequired')); return; }
    if (!PROVIDER_NAME_RE.test(name)) { setAddProviderError(t('settings.providerNameInvalid')); return; }
    if (byokProviders.some(p => p.provider === name)) { setAddProviderError(t('settings.providerNameDuplicate')); return; }
    if (!parent) { setAddProviderError(t('settings.parentProviderRequired')); return; }
    // Add to providers list (will be persisted on Save)
    setByokProviders(prev => [...prev, {
      provider: name,
      display_name: name,
      parent_provider: parent,
      has_key: false,
      masked_key: null,
      base_url: baseUrl || null,
      is_custom: true,
      use_response_api: addProviderForm.use_response_api || false,
    }]);
    // Seed key/url inputs so they get saved with the main Save button
    if (apiKey) setKeyInputs(prev => ({ ...prev, [name]: apiKey }));
    if (baseUrl) setBaseUrlInputs(prev => ({ ...prev, [name]: baseUrl }));
    setSelectedByokProvider(name);
    setShowAddProviderForm(false);
    setAddProviderForm({ name: '', parent_provider: '', api_key: '', base_url: '', use_response_api: false });
    setAddProviderError(null);
  };

  const _handleDeleteCustomProvider = (providerName: string) => {
    setByokProviders(prev => prev.filter(p => p.provider !== providerName));
    // Also clean up any pending key/url inputs
    setKeyInputs(prev => { const next = { ...prev }; delete next[providerName]; return next; });
    setBaseUrlInputs(prev => { const next = { ...prev }; delete next[providerName]; return next; });
    if (selectedByokProvider === providerName) setSelectedByokProvider('');
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
          const result = await pollCodexDevice() as { success?: boolean; pending?: boolean; account_id?: string; email?: string; plan_type?: string };
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

  // Custom Models helpers
  const CUSTOM_MODEL_NAME_RE = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;

  const validateCustomModelForm = (form: CustomModelFormState, existingModels: CustomModelEntry[], editIdx: number | null): string | null => {
    if (!form.name?.trim()) return t('settings.customModelNameRequired');
    if (!CUSTOM_MODEL_NAME_RE.test(form.name.trim())) return t('settings.customModelNameInvalid');
    if (!form.model_id?.trim()) return t('settings.customModelIdRequired');
    if (!form.provider?.trim()) return t('settings.customModelProviderRequired');
    // Check collision with system models
    if (validModelNames.has(form.name.trim())) return t('settings.customModelNameConflict');
    // Check duplicate in custom models
    const dup = existingModels.findIndex((cm, i) => i !== editIdx && cm.name === form.name.trim());
    if (dup >= 0) return t('settings.customModelNameDuplicate');
    // Check that the selected provider has a BYOK key configured
    const providerName = form.provider.trim();
    const prov = byokProviders.find(p => p.provider === providerName);
    if (!prov || !prov.has_key) return t('settings.customModelProviderNoKey', { provider: providerName });
    // Validate JSON fields
    for (const field of ['parameters', 'extra_body'] as const) {
      const val = form[field]?.trim();
      if (val) {
        try { JSON.parse(val); } catch { return `${field}: ${t('settings.customModelInvalidJson')}`; }
      }
    }
    return null;
  };

  const handleCustomModelSave = () => {
    const err = validateCustomModelForm(customModelForm, customModels, editingCustomModelIdx);
    if (err) { setCustomModelError(err); return; }
    const existing = editingCustomModelIdx != null ? customModels[editingCustomModelIdx] : undefined;
    const entry: CustomModelEntry = {
      ...(existing ?? {}),
      name: customModelForm.name.trim(),
      model_id: customModelForm.model_id.trim(),
      provider: customModelForm.provider.trim(),
    };
    if (customModelForm.parameters?.trim()) entry.parameters = JSON.parse(customModelForm.parameters.trim()) as Record<string, unknown>;
    else delete entry.parameters;
    if (customModelForm.extra_body?.trim()) entry.extra_body = JSON.parse(customModelForm.extra_body.trim()) as Record<string, unknown>;
    else delete entry.extra_body;
    setCustomModels(prev => {
      const next = [...prev];
      if (editingCustomModelIdx != null) next[editingCustomModelIdx] = entry;
      else next.push(entry);
      return next;
    });
    setShowCustomModelForm(false);
    setEditingCustomModelIdx(null);
    setCustomModelForm({ name: '', model_id: '', provider: '', parameters: '', extra_body: '' });
    setCustomModelError(null);
  };

  const handleCustomModelEdit = (idx: number) => {
    const cm = customModels[idx];
    const isKnown = byokProviders.some(p => p.provider === cm.provider);
    setCustomModelForm({
      name: cm.name,
      model_id: cm.model_id,
      provider: cm.provider,
      parameters: cm.parameters ? JSON.stringify(cm.parameters, null, 2) : '',
      extra_body: cm.extra_body ? JSON.stringify(cm.extra_body, null, 2) : '',
      _customProvider: !isKnown,
    });
    setEditingCustomModelIdx(idx);
    setShowCustomModelForm(true);
    setCustomModelError(null);
  };

  const handleCustomModelDelete = (idx: number) => {
    setCustomModels(prev => prev.filter((_, i) => i !== idx));
  };

  const handleCustomModelCancel = () => {
    setShowCustomModelForm(false);
    setEditingCustomModelIdx(null);
    setCustomModelForm({ name: '', model_id: '', provider: '', parameters: '', extra_body: '' });
    setCustomModelError(null);
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
      const result = await initiateClaudeOAuth() as { authorize_url: string };
      setClaudeAuthorizeUrl(result.authorize_url);
      // Open authorization page in new tab
      window.open(result.authorize_url, '_blank', 'noopener');
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
      const result = await submitClaudeCallback(claudeCallbackInput.trim()) as { success?: boolean; account_id?: string; email?: string; plan_type?: string };
      if (result.success) {
        setClaudeAuthorizeUrl(null);
        setClaudeCallbackInput('');
        setClaudeOAuthStatus({
          connected: true,
          account_id: result.account_id || '',
          email: result.email || null,
          plan_type: result.plan_type || null,
        });
      }
    } catch (e: unknown) {
      const axiosErr = e as { response?: { data?: { detail?: string } } };
      setClaudeError(axiosErr.response?.data?.detail || t('settings.claudePasteError', 'Failed to exchange code. Please try again.'));
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

  const handleLocaleChange = (newLocale: string) => {
    setLocale(newLocale);
    // Also switch i18n language for supported UI locales
    if (newLocale === 'en-US' || newLocale === 'zh-CN') {
      i18n.changeLanguage(newLocale);
      localStorage.setItem('locale', newLocale);
    }
  };

  const handleUserInfoSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    setSaveSuccess(false);
    try {
      const userData: Record<string, string> = {};
      if (name.trim()) userData.name = name.trim();
      if (timezone) userData.timezone = timezone;
      if (locale) userData.locale = locale;
      if (Object.keys(userData).length > 0) {
        await updateCurrentUser(userData);
        queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
      }
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      setError((err as Error).message || t('settings.failedToUpdateUser'));
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
      queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
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
  const preventEnterSubmit = (e: React.KeyboardEvent<HTMLFormElement>) => {
    const target = e.target as HTMLInputElement;
    if (e.key === 'Enter' && target.tagName === 'INPUT' && target.type !== 'submit') {
      e.preventDefault();
    }
  };

  const handleClose = () => {
    setError(null);
    setSaveSuccess(false);
    onClose();
  };

  // Build provider manifest for ProviderManager from byokProviders list.
  const providerManifest = React.useMemo(() => {
    return byokProviders.filter(p => !p.is_custom).map(p => ({
      provider: p.provider,
      display_name: p.display_name || p.provider,
    }));
  }, [byokProviders]);

  if (!isOpen) return null;

  return (
    <>
      <div
        className="fixed inset-0 z-[1010] flex items-center justify-center"
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
                    <Select
                      value={timezone}
                      onChange={(e) => setTimezone(e.target.value)}
                      disabled={isSubmitting}
                    >
                      {timezones.map((item, i) => (
                        item.value !== undefined ? (
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
                      disabled={isSubmitting}
                    >
                      {locales.map((item, i) => (
                        <option key={i} value={item.value}>{item.label}</option>
                      ))}
                    </Select>
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
                    {/* Default + Flash model selectors */}
                    <ModelTierConfig
                      models={visibleModels}
                      primaryModel={preferredModel}
                      onPrimaryModelChange={setPreferredModel}
                      flashModel={preferredFlashModel}
                      onFlashModelChange={setPreferredFlashModel}
                      modelAccess={modelAccessMap}
                    />

                    {/* Quick-access models — compact strip */}
                    <div style={{ marginTop: '16px' }}>
                      <label className="block text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
                        {t('settings.starredModels')}
                      </label>
                      <div className="flex flex-wrap items-center gap-1.5">
                        {starredModels.map((key) => (
                          <button
                            key={key}
                            type="button"
                            onClick={() => setStarredModels(prev => prev.filter(k => k !== key))}
                            className="inline-flex items-center gap-1 pl-2 pr-1 py-0.5 rounded-md text-xs transition-colors group"
                            style={{
                              border: '1px solid var(--color-accent-primary)',
                              background: 'var(--color-accent-soft)',
                              color: 'var(--color-accent-light)',
                            }}
                            title={key}
                          >
                            <span>{key}</span>
                            <X className="h-3 w-3 opacity-40 group-hover:opacity-100 transition-opacity" />
                          </button>
                        ))}
                        {starredModels.length === 0 && (
                          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                            {t('settings.starredModelsDesc')}
                          </span>
                        )}
                        <button
                          type="button"
                          onClick={() => { setShowModelPicker(v => !v); setModelPickerSearch(''); }}
                          className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs transition-colors"
                          style={{
                            border: '1px dashed var(--color-border-muted)',
                            background: showModelPicker ? 'var(--color-accent-soft)' : 'transparent',
                            color: showModelPicker ? 'var(--color-accent-light)' : 'var(--color-text-tertiary)',
                          }}
                        >
                          <Plus className="h-3 w-3" />
                          <span>{t('settings.addModels', 'Add')}</span>
                        </button>
                      </div>
                    </div>

                    {/* Collapsible model picker — hidden by default */}
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
                              ? models.filter((m: string) => m.toLowerCase().includes(query))
                              : models;
                            if (filtered.length === 0) return null;
                            const displayName = providerData?.display_name || provider.charAt(0).toUpperCase() + provider.slice(1);
                            return (
                              <div key={provider} className="mb-1">
                                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
                                  {displayName}
                                </div>
                                {filtered.map((m: string) => {
                                  const key = m;
                                  const isStarred = starredModels.includes(key);
                                  return (
                                    <button
                                      key={key}
                                      type="button"
                                      onClick={() => setStarredModels(prev =>
                                        prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]
                                      )}
                                      className="w-full flex items-center justify-between px-2 py-1.5 rounded-md text-xs transition-colors"
                                      style={{
                                        color: isStarred ? 'var(--color-accent-light)' : 'var(--color-text-primary)',
                                        backgroundColor: isStarred ? 'var(--color-accent-soft)' : 'transparent',
                                      }}
                                      onMouseEnter={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'var(--color-bg-elevated)'; }}
                                      onMouseLeave={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'transparent'; }}
                                    >
                                      <span>{m}</span>
                                      <Pin className="h-3 w-3 flex-shrink-0" style={{ color: isStarred ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)', opacity: isStarred ? 1 : 0.4 }} />
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

                    {byokEnabled && (<>
                      <div className="space-y-3 mt-4">
                        <ProviderManager
                          providers={providerManifest}
                          configuredProviders={byokProviders.filter(p => p.has_key)}
                          selectedProvider={selectedByokProvider || null}
                          onSelectProvider={(p) => setSelectedByokProvider(p || '')}
                          keyInputs={keyInputs}
                          onKeyChange={(provider, value) => setKeyInputs(prev => ({ ...prev, [provider]: value }))}
                          baseUrlInputs={baseUrlInputs}
                          onBaseUrlChange={(provider, value) => setBaseUrlInputs(prev => ({ ...prev, [provider]: value }))}
                          providerNeedsBaseUrl={(provider) => !!byokProviders.find(p => p.provider === provider)?.is_custom}
                          onDeleteProvider={handleDeleteProviderKey}
                        />

                        {/* Add custom sub-provider button */}
                        <button
                          type="button"
                          onClick={() => { setShowAddProviderForm(true); setAddProviderForm({ name: '', parent_provider: '', api_key: '', base_url: '', use_response_api: false }); setAddProviderError(null); }}
                          className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                          style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                        >
                          <Plus className="h-3.5 w-3.5" />
                          {t('settings.addProvider')}
                        </button>

                        {/* Add Provider inline form */}
                        {showAddProviderForm && (
                          <div
                            className="rounded-lg p-3 space-y-3"
                            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
                          >
                            <div className="grid grid-cols-2 gap-3">
                              <div>
                                <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                  {t('settings.addProviderName')} *
                                </label>
                                <input
                                  type="text"
                                  value={addProviderForm.name}
                                  onChange={(e) => setAddProviderForm(f => ({ ...f, name: e.target.value }))}
                                  placeholder={t('settings.addProviderNamePlaceholder')}
                                  className="w-full rounded-md px-2.5 py-1.5 text-sm"
                                  style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                                  autoFocus
                                />
                              </div>
                              <div>
                                <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                  {t('settings.parentProvider')} *
                                </label>
                                <Select
                                  value={addProviderForm.parent_provider}
                                  onChange={(e) => setAddProviderForm(f => ({ ...f, parent_provider: e.target.value }))}
                                  style={{ backgroundColor: 'var(--color-bg-elevated)' }}
                                >
                                  <option value="">{t('settings.selectProvider')}</option>
                                  {byokProviders.filter(p => !p.is_custom).map(p => (
                                    <option key={p.provider} value={p.provider}>{p.display_name || p.provider}</option>
                                  ))}
                                </Select>
                              </div>
                            </div>
                            <div>
                              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                {t('settings.enterApiKey')}
                              </label>
                              <input
                                type="password"
                                value={addProviderForm.api_key || ''}
                                onChange={(e) => setAddProviderForm(f => ({ ...f, api_key: e.target.value }))}
                                placeholder="sk-..."
                                className="w-full rounded-md px-2.5 py-1.5 text-sm"
                                style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                              />
                            </div>
                            <div>
                              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                {t('settings.baseUrlPlaceholder')}
                              </label>
                              <input
                                type="text"
                                value={addProviderForm.base_url || ''}
                                onChange={(e) => setAddProviderForm(f => ({ ...f, base_url: e.target.value }))}
                                placeholder="https://api.example.com/v1"
                                className="w-full rounded-md px-2.5 py-1.5 text-sm"
                                style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
                              />
                            </div>
                            {addProviderForm.parent_provider === 'openai' && (
                              <div className="flex items-center justify-between">
                                <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{t('settings.useResponseApi')}</span>
                                <button
                                  type="button"
                                  onClick={() => setAddProviderForm(f => ({ ...f, use_response_api: !f.use_response_api }))}
                                  className="relative inline-flex h-5 w-9 items-center rounded-full transition-colors"
                                  style={{ backgroundColor: addProviderForm.use_response_api ? 'var(--color-accent-primary)' : 'var(--color-border-muted)' }}
                                >
                                  <span className="inline-block h-3.5 w-3.5 rounded-full bg-white transition-transform" style={{ transform: addProviderForm.use_response_api ? 'translateX(17px)' : 'translateX(3px)' }} />
                                </button>
                              </div>
                            )}
                            {addProviderError && (
                              <p className="text-xs" style={{ color: 'var(--color-loss)' }}>{addProviderError}</p>
                            )}
                            <div className="flex items-center gap-2 justify-end">
                              <button
                                type="button"
                                onClick={() => { setShowAddProviderForm(false); setAddProviderError(null); }}
                                className="px-3 py-1.5 rounded-md text-xs font-medium"
                                style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
                              >
                                {t('common.cancel')}
                              </button>
                              <button
                                type="button"
                                onClick={handleAddProviderSave}
                                className="px-3 py-1.5 rounded-md text-xs font-medium"
                                style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
                              >
                                {t('settings.addProvider')}
                              </button>
                            </div>
                          </div>
                        )}
                      </div>

                      {/* Custom Models — inside BYOK */}
                      <div className="mt-5 pt-4" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                        <div className="flex items-center justify-between mb-2">
                          <div>
                            <label className="block text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                              {t('settings.customModels')}
                            </label>
                            <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                              {t('settings.customModelsDesc')}
                            </p>
                          </div>
                          {!showCustomModelForm && (
                            <button
                              type="button"
                              onClick={() => { setShowCustomModelForm(true); setEditingCustomModelIdx(null); setCustomModelForm({ name: '', model_id: '', provider: '', parameters: '', extra_body: '' }); setCustomModelError(null); }}
                              className="flex items-center gap-1 px-2.5 py-1.5 rounded-md text-xs font-medium transition-colors"
                              style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                            >
                              <Plus className="h-3 w-3" /> {t('settings.addCustomModel')}
                            </button>
                          )}
                        </div>

                        {/* Existing custom models list */}
                        {customModels.length > 0 && !showCustomModelForm && (
                          <div className="space-y-2 mb-3">
                            {customModels.map((cm, idx) => (
                              <div
                                key={cm.name}
                                className="flex items-center justify-between rounded-md px-3 py-2"
                                style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
                              >
                                <div className="flex items-center gap-2 min-w-0">
                                  <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>{cm.name}</span>
                                  <span
                                    className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0"
                                    style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                                  >
                                    {cm.provider}
                                  </span>
                                  <span className="text-xs truncate" style={{ color: 'var(--color-text-tertiary)' }}>{cm.model_id}</span>
                                </div>
                                <div className="flex items-center gap-1 shrink-0">
                                  <button
                                    type="button"
                                    onClick={() => handleCustomModelEdit(idx)}
                                    className="p-1 rounded transition-colors"
                                    style={{ color: 'var(--color-text-tertiary)' }}
                                  >
                                    <Pencil className="h-3.5 w-3.5" />
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => handleCustomModelDelete(idx)}
                                    className="p-1 rounded transition-colors"
                                    style={{ color: 'var(--color-loss)' }}
                                  >
                                    <Trash2 className="h-3.5 w-3.5" />
                                  </button>
                                </div>
                              </div>
                            ))}
                          </div>
                        )}

                        {/* Inline form for add/edit */}
                        {showCustomModelForm && (
                          <div
                            className="rounded-lg p-3 space-y-3 mt-2"
                            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
                          >
                            <div className="grid grid-cols-2 gap-3">
                              <div>
                                <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                  {t('settings.customModelName')} *
                                </label>
                                <input
                                  type="text"
                                  value={customModelForm.name}
                                  onChange={(e) => setCustomModelForm(f => ({ ...f, name: e.target.value }))}
                                  placeholder="my-random-model"
                                  className="w-full rounded-md px-2.5 py-1.5 text-sm"
                                  style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                                />
                              </div>
                              <div>
                                <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                  {t('settings.customModelId')} *
                                </label>
                                <input
                                  type="text"
                                  value={customModelForm.model_id}
                                  onChange={(e) => setCustomModelForm(f => ({ ...f, model_id: e.target.value }))}
                                  placeholder="gpt-5.2"
                                  className="w-full rounded-md px-2.5 py-1.5 text-sm"
                                  style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                                />
                              </div>
                            </div>
                            <div>
                              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                {t('settings.customModelProvider')} *
                              </label>
                              {(() => {
                                const isKnown = byokProviders.some(p => p.provider === customModelForm.provider);
                                const isCustomMode = customModelForm.provider !== '' && !isKnown;
                                const selectValue = isKnown ? customModelForm.provider : (isCustomMode || customModelForm._customProvider ? '__custom__' : '');
                                return (
                                  <>
                                    <Select
                                      value={selectValue}
                                      onChange={(e) => {
                                        const val = e.target.value;
                                        if (val === '__custom__') {
                                          setCustomModelForm(f => ({ ...f, provider: '', _customProvider: true }));
                                        } else {
                                          setCustomModelForm(f => ({ ...f, provider: val, _customProvider: false }));
                                        }
                                      }}
                                      style={{ backgroundColor: 'var(--color-bg-elevated)' }}
                                    >
                                      <option value="">{t('settings.selectProvider')}</option>
                                      {byokProviders.filter(p => !p.is_custom).map(p => (
                                        <option key={p.provider} value={p.provider}>{p.display_name || p.provider}</option>
                                      ))}
                                      {byokProviders.some(p => p.is_custom) && (
                                        <optgroup label="─────────">
                                          {byokProviders.filter(p => p.is_custom).map(p => (
                                            <option key={p.provider} value={p.provider}>{p.display_name} ({p.parent_provider})</option>
                                          ))}
                                        </optgroup>
                                      )}
                                      <option value="__custom__">{t('settings.customProvider')}</option>
                                    </Select>
                                    {(isCustomMode || customModelForm._customProvider) && (
                                      <input
                                        type="text"
                                        value={customModelForm.provider}
                                        onChange={(e) => setCustomModelForm(f => ({ ...f, provider: e.target.value, _customProvider: true }))}
                                        placeholder={t('settings.customProviderPlaceholder')}
                                        className="w-full rounded-md px-2.5 py-1.5 text-sm mt-2"
                                        style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                                        autoFocus
                                      />
                                    )}
                                  </>
                                );
                              })()}
                            </div>
                            <div>
                              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                {t('settings.customModelParameters')}
                              </label>
                              <textarea
                                value={customModelForm.parameters}
                                onChange={(e) => setCustomModelForm(f => ({ ...f, parameters: e.target.value }))}
                                placeholder='{"reasoning": {"effort": "medium", "summary": "auto"}}'
                                rows={2}
                                className="w-full rounded-md px-2.5 py-1.5 text-xs font-mono resize-none"
                                style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                              />
                            </div>
                            <div>
                              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                                {t('settings.customModelExtraBody')}
                              </label>
                              <textarea
                                value={customModelForm.extra_body}
                                onChange={(e) => setCustomModelForm(f => ({ ...f, extra_body: e.target.value }))}
                                placeholder='{"thinking": {"type": "enabled"}}'
                                rows={2}
                                className="w-full rounded-md px-2.5 py-1.5 text-xs font-mono resize-none"
                                style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
                              />
                            </div>
                            {customModelError && (
                              <p className="text-xs" style={{ color: 'var(--color-loss)' }}>{customModelError}</p>
                            )}
                            <div className="flex items-center gap-2 justify-end">
                              <button
                                type="button"
                                onClick={handleCustomModelCancel}
                                className="px-3 py-1.5 rounded-md text-xs font-medium"
                                style={{ color: 'var(--color-text-primary)', backgroundColor: 'transparent' }}
                              >
                                {t('common.cancel')}
                              </button>
                              <button
                                type="button"
                                onClick={handleCustomModelSave}
                                className="px-3 py-1.5 rounded-md text-xs font-medium"
                                style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
                              >
                                {t('common.save')}
                              </button>
                            </div>
                          </div>
                        )}
                      </div>
                    </>)}
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

    </>
  );
}

export default UserConfigPanel;
