import React, { useState, useEffect, useRef } from 'react';
import { X, User, LogOut, Eye, EyeOff, Trash2, HelpCircle } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { updateCurrentUser, getCurrentUser, updatePreferences, getPreferences, clearPreferences, uploadAvatar, redeemCode, getUsageStatus, getAvailableModels, getUserApiKeys, updateUserApiKeys, deleteUserApiKey } from '../utils/api';
import { useAuth } from '../../../contexts/AuthContext';
import ConfirmDialog from './ConfirmDialog';

/**
 * UserConfigPanel Component
 *
 * Modal panel for logged-in users: User info (email read-only), preferences, and logout button.
 *
 * @param {boolean} isOpen - Whether the panel is open
 * @param {Function} onClose - Callback to close the panel
 */
function UserConfigPanel({ isOpen, onClose, onResetOnboarding }) {
  const { user: authUser, logout, refreshUser } = useAuth();
  const [activeTab, setActiveTab] = useState('userInfo');
  const [avatarUrl, setAvatarUrl] = useState(null);
  const fileInputRef = useRef(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');

  const [riskTolerance, setRiskTolerance] = useState('');
  const [companyInterest, setCompanyInterest] = useState('');
  const [holdingPeriod, setHoldingPeriod] = useState('');
  const [analysisFocus, setAnalysisFocus] = useState('');
  const [outputStyle, setOutputStyle] = useState('');

  const [plan, setPlan] = useState({ id: 1, name: 'free', display_name: 'Free', rank: 0 });
  const [redeemInput, setRedeemInput] = useState('');
  const [isRedeeming, setIsRedeeming] = useState(false);
  const [redeemError, setRedeemError] = useState(null);
  const [redeemSuccess, setRedeemSuccess] = useState(null);

  const [usage, setUsage] = useState(null);

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

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  const [isResetting, setIsResetting] = useState(false);

  const timezones = [
    { value: '', label: 'Select timezone...' },
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
    { value: '', label: 'Select locale...' },
    { value: 'en-US', label: 'English (United States)' },
    { value: 'en-GB', label: 'English (United Kingdom)' },
    { value: 'zh-CN', label: 'Chinese (Simplified, China)' },
    { value: 'zh-TW', label: 'Chinese (Traditional, Taiwan)' },
    { value: 'ja-JP', label: 'Japanese (Japan)' },
    { value: 'ko-KR', label: 'Korean (Korea)' },
  ];

  useEffect(() => {
    if (isOpen) {
      setIsLoading(true);
      Promise.all([loadUserData(), loadPreferencesData(), loadUsageData()])
        .finally(() => setIsLoading(false));
    }
  }, [isOpen]);

  // Load model tab data lazily when tab is selected
  useEffect(() => {
    if (isOpen && activeTab === 'model') {
      loadModelTabData();
    }
  }, [isOpen, activeTab]);

  const loadUserData = async () => {
    try {
      const userData = await getCurrentUser();
      if (userData?.user) {
        setName(userData.user.name || '');
        setTimezone(userData.user.timezone || '');
        setLocale(userData.user.locale || '');
        setPlan(userData.user.plan || { id: 1, name: 'free', display_name: 'Free', rank: 0 });
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
      if (preferencesData) {
        setRiskTolerance(preferencesData.risk_preference?.risk_tolerance || '');
        setCompanyInterest(preferencesData.investment_preference?.company_interest || '');
        setHoldingPeriod(preferencesData.investment_preference?.holding_period || '');
        setAnalysisFocus(preferencesData.investment_preference?.analysis_focus || '');
        setOutputStyle(preferencesData.agent_preference?.output_style || '');
      }
    } catch {}
  };

  const loadUsageData = async () => {
    try {
      const data = await getUsageStatus();
      setUsage(data);
    } catch {
      // Usage data load failed - keep null
    }
  };

  const loadModelTabData = async () => {
    setModelTabError(null);
    try {
      const [modelsRes, keysRes, prefsRes] = await Promise.all([
        getAvailableModels(),
        getUserApiKeys(),
        getPreferences(),
      ]);
      setAvailableModels(modelsRes?.models || {});
      setByokEnabled(keysRes?.byok_enabled || false);
      setByokProviders(keysRes?.providers || []);
      setPreferredModel(prefsRes?.other_preference?.preferred_model || '');
      setPreferredFlashModel(prefsRes?.other_preference?.preferred_flash_model || '');
    } catch {
      setModelTabError('Failed to load model settings');
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
      setModelTabError('Failed to save settings');
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
      setModelTabError('Failed to toggle BYOK');
    }
  };

  const handleDeleteProviderKey = async (provider) => {
    setDeletingProvider(provider);
    setModelTabError(null);
    try {
      const result = await deleteUserApiKey(provider);
      setByokProviders(result.providers);
    } catch {
      setModelTabError(`Failed to delete ${provider} key`);
    } finally {
      setDeletingProvider(null);
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
      setError('Failed to upload avatar');
    } finally {
      setIsUploadingAvatar(false);
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
      setError(err.message || 'Failed to update user information');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handlePreferencesSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    setSaveSuccess(false);
    try {
      const preferences = {};
      if (riskTolerance) preferences.risk_preference = { risk_tolerance: riskTolerance };
      const investmentPrefs = {};
      if (companyInterest) investmentPrefs.company_interest = companyInterest;
      if (holdingPeriod) investmentPrefs.holding_period = holdingPeriod;
      if (analysisFocus) investmentPrefs.analysis_focus = analysisFocus;
      if (Object.keys(investmentPrefs).length > 0) preferences.investment_preference = investmentPrefs;
      if (outputStyle) preferences.agent_preference = { output_style: outputStyle };
      if (Object.keys(preferences).length > 0) {
        await updatePreferences(preferences);
        await updateCurrentUser({ onboarding_completed: true });
      }
      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 3000);
    } catch (err) {
      setError(err.message || 'Failed to update preferences');
    } finally {
      setIsSubmitting(false);
    }
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
      setShowResetConfirm(false);
      onClose();
      if (onResetOnboarding) onResetOnboarding();
    } catch {
      setError('Failed to reset preferences');
      setShowResetConfirm(false);
    } finally {
      setIsResetting(false);
    }
  };

  const handleRedeemCode = async () => {
    if (!redeemInput.trim()) return;
    setIsRedeeming(true);
    setRedeemError(null);
    setRedeemSuccess(null);
    try {
      const result = await redeemCode(redeemInput.trim());
      setRedeemSuccess(result.message);
      setRedeemInput('');
      refreshUser();
      await Promise.all([loadUserData(), loadUsageData()]);
    } catch (err) {
      const detail = err.response?.data?.detail || err.message || 'Failed to redeem code';
      setRedeemError(typeof detail === 'string' ? detail : detail.message || 'Failed to redeem code');
    } finally {
      setIsRedeeming(false);
    }
  };

  const PLAN_BADGE_COLORS = [
    { backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' },
    { backgroundColor: 'rgba(59, 130, 246, 0.15)', color: '#3b82f6', border: '1px solid rgba(59, 130, 246, 0.3)' },
    { backgroundColor: 'rgba(234, 179, 8, 0.15)', color: '#eab308', border: '1px solid rgba(234, 179, 8, 0.3)' },
  ];
  const getPlanBadgeStyle = (rank) => PLAN_BADGE_COLORS[Math.min(rank, PLAN_BADGE_COLORS.length - 1)];

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
    setRedeemError(null);
    setRedeemSuccess(null);
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
          className="relative w-full max-w-2xl rounded-lg p-6"
          style={{
            backgroundColor: 'var(--color-bg-elevated)',
            border: '1px solid var(--color-border-muted)',
            maxHeight: '90vh',
            overflowY: 'auto',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <button
            onClick={handleClose}
            className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-white/10"
            style={{ color: 'var(--color-text-primary)' }}
          >
            <X className="h-5 w-5" />
          </button>

          <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>User Settings</h2>
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
                  User Info
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
                  Preferences
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
                  Model
                </button>
              </div>

              {isLoading && (
                <div className="flex items-center justify-center py-8">
                  <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>Loading...</p>
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
                        {isUploadingAvatar ? 'Uploading...' : 'Change Avatar'}
                      </button>
                    </div>
                    <input type="file" ref={fileInputRef} onChange={handleAvatarChange} accept="image/png,image/jpeg,image/gif,image/webp" style={{ display: 'none' }} />
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Email</label>
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
                    <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>Email cannot be changed</p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Name</label>
                    <Input
                      type="text"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                      placeholder="Enter your name"
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
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Timezone</label>
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
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Locale</label>
                    <select
                      value={locale}
                      onChange={(e) => setLocale(e.target.value)}
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

                  <div style={{ borderTop: '1px solid var(--color-border-muted)', paddingTop: '16px' }}>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Plan</label>
                    <div className="flex items-center gap-3 mb-3">
                      <span
                        className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold uppercase"
                        style={getPlanBadgeStyle(plan.rank ?? 0)}
                      >
                        {plan.display_name || plan.name || 'Free'}
                      </span>
                    </div>

                    {usage && (
                      <div className="mb-4 space-y-3">
                        {/* Credits */}
                        <div>
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                              Daily Credits
                              {usage.byok_enabled && (
                                <span
                                  className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium"
                                  style={{ backgroundColor: 'rgba(34, 197, 94, 0.15)', color: '#22c55e', border: '1px solid rgba(34, 197, 94, 0.3)' }}
                                >
                                  BYOK
                                </span>
                              )}
                            </span>
                            <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                              {usage.credits.limit === -1
                                ? 'Unlimited'
                                : `${usage.credits.used} / ${usage.credits.limit}`}
                            </span>
                          </div>
                          {usage.credits.limit !== -1 && (
                            <div className="w-full h-1.5 rounded-full overflow-hidden" style={{ backgroundColor: 'var(--color-border-muted)' }}>
                              <div
                                className="h-full rounded-full transition-all"
                                style={{
                                  width: `${Math.min(100, (usage.credits.used / usage.credits.limit) * 100)}%`,
                                  backgroundColor: usage.credits.used / usage.credits.limit > 0.9
                                    ? 'var(--color-loss)'
                                    : 'var(--color-accent-primary)',
                                }}
                              />
                            </div>
                          )}
                        </div>

                        {/* Workspaces */}
                        <div>
                          <div className="flex items-center justify-between">
                            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>Active Workspaces</span>
                            <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                              {usage.workspaces.limit === -1
                                ? 'Unlimited'
                                : `${usage.workspaces.active} / ${usage.workspaces.limit}`}
                            </span>
                          </div>
                        </div>
                      </div>
                    )}

                    <div className="flex gap-2">
                      <input
                        type="text"
                        value={redeemInput}
                        onChange={(e) => { setRedeemInput(e.target.value); setRedeemError(null); setRedeemSuccess(null); }}
                        placeholder="Enter redemption code"
                        className="flex-1 rounded-md px-3 py-1.5 text-sm"
                        style={{
                          backgroundColor: 'var(--color-bg-card)',
                          border: '1px solid var(--color-border-muted)',
                          color: 'var(--color-text-primary)',
                        }}
                        disabled={isRedeeming}
                        onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); handleRedeemCode(); } }}
                      />
                      <button
                        type="button"
                        onClick={handleRedeemCode}
                        disabled={isRedeeming || !redeemInput.trim()}
                        className="px-3 py-1.5 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: isRedeeming || !redeemInput.trim() ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {isRedeeming ? 'Redeeming...' : 'Redeem'}
                      </button>
                    </div>
                    {redeemError && (
                      <p className="text-xs mt-1.5" style={{ color: 'var(--color-loss)' }}>{redeemError}</p>
                    )}
                    {redeemSuccess && (
                      <p className="text-xs mt-1.5" style={{ color: 'var(--color-gain)' }}>{redeemSuccess}</p>
                    )}
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
                      <LogOut className="h-4 w-4" /> Logout
                    </button>
                    <div className="flex items-center gap-3">
                      {saveSuccess && (
                        <span className="text-xs" style={{ color: 'var(--color-success, #22c55e)' }}>Saved</span>
                      )}
                      <button type="button" onClick={handleClose} disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                        Close
                      </button>
                      <button type="submit" disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {isSubmitting ? 'Saving...' : 'Save'}
                      </button>
                    </div>
                  </div>
                </form>
              )}

              {!isLoading && activeTab === 'preferences' && (
                <form onSubmit={handlePreferencesSubmit} onKeyDown={preventEnterSubmit} className="space-y-5">
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Risk Tolerance</label>
                    <select
                      value={riskTolerance}
                      onChange={(e) => setRiskTolerance(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select...</option>
                      <option value="low" style={{ backgroundColor: 'var(--color-bg-card)' }}>Low</option>
                      <option value="medium" style={{ backgroundColor: 'var(--color-bg-card)' }}>Medium</option>
                      <option value="high" style={{ backgroundColor: 'var(--color-bg-card)' }}>High</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Company Interest</label>
                    <select
                      value={companyInterest}
                      onChange={(e) => setCompanyInterest(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select...</option>
                      <option value="growth" style={{ backgroundColor: 'var(--color-bg-card)' }}>Growth</option>
                      <option value="stable" style={{ backgroundColor: 'var(--color-bg-card)' }}>Stable</option>
                      <option value="value" style={{ backgroundColor: 'var(--color-bg-card)' }}>Value</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Holding Period</label>
                    <select
                      value={holdingPeriod}
                      onChange={(e) => setHoldingPeriod(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select...</option>
                      <option value="short_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Short-term</option>
                      <option value="mid_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Mid-term</option>
                      <option value="long_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Long-term</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Analysis Focus</label>
                    <select
                      value={analysisFocus}
                      onChange={(e) => setAnalysisFocus(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select...</option>
                      <option value="growth" style={{ backgroundColor: 'var(--color-bg-card)' }}>Growth</option>
                      <option value="valuation" style={{ backgroundColor: 'var(--color-bg-card)' }}>Valuation</option>
                      <option value="moat" style={{ backgroundColor: 'var(--color-bg-card)' }}>Moat</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Output Style</label>
                    <select
                      value={outputStyle}
                      onChange={(e) => setOutputStyle(e.target.value)}
                      className="w-full rounded-md px-3 py-2 text-sm"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                    >
                      <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select...</option>
                      <option value="summary" style={{ backgroundColor: 'var(--color-bg-card)' }}>Summary</option>
                      <option value="data" style={{ backgroundColor: 'var(--color-bg-card)' }}>Data</option>
                      <option value="deep_dive" style={{ backgroundColor: 'var(--color-bg-card)' }}>Deep Dive</option>
                    </select>
                  </div>

                  {error && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
                    </div>
                  )}

                  <div className="flex gap-3 justify-between pt-4">
                    <button
                      type="button"
                      onClick={() => setShowResetConfirm(true)}
                      className="flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors"
                      style={{ color: 'var(--color-loss)', backgroundColor: 'transparent', border: '1px solid var(--color-loss)' }}
                    >
                      <Trash2 className="h-4 w-4" /> Reset & Re-onboard
                    </button>
                    <div className="flex items-center gap-3">
                      {saveSuccess && (
                        <span className="text-xs" style={{ color: 'var(--color-success, #22c55e)' }}>Saved</span>
                      )}
                      <button type="button" onClick={handleClose} disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                        Close
                      </button>
                      <button type="submit" disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {isSubmitting ? 'Saving...' : 'Save'}
                      </button>
                    </div>
                  </div>
                </form>
              )}

              {!isLoading && activeTab === 'model' && (
                <div className="space-y-6">
                  {/* Section 1: Model Preferences */}
                  <div>
                    {[
                      { label: 'Default Model', desc: 'Used for workspace conversations (code execution, sandbox).', value: preferredModel, setter: setPreferredModel },
                      { label: 'Flash Model', desc: 'Used for quick queries without sandbox (web search, market data).', value: preferredFlashModel, setter: setPreferredFlashModel },
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
                          <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>System default</option>
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

                  {/* Section 2: BYOK */}
                  <div style={{ borderTop: '1px solid var(--color-border-muted)', paddingTop: '16px' }}>
                    <div className="flex items-center justify-between mb-2">
                      <div>
                        <div className="flex items-center gap-1.5">
                          <label className="block text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>Bring Your Own Key (BYOK)</label>
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
                              Your API keys are stored using AES encryption and are never visible in plaintext. If you choose to delete a key, it is permanently removed from our records.
                            </div>
                          </div>
                        </div>
                        <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                          Provide your own API keys to bypass credit limits.
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
                                placeholder={prov.has_key ? prov.masked_key : 'Enter API key...'}
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
                                className="p-1.5 rounded-md shrink-0 transition-colors hover:bg-white/10"
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
                      <span className="text-xs" style={{ color: 'var(--color-success, #22c55e)' }}>Saved</span>
                    )}
                    <button type="button" onClick={handleClose}
                      className="px-4 py-2 rounded-md text-sm font-medium hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                      Close
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
                      {isSubmitting ? 'Saving...' : 'Save'}
                    </button>
                  </div>
                </div>
              )}
        </div>
      </div>

      <ConfirmDialog
        open={showLogoutConfirm}
        title="Logout"
        message="Are you sure you want to logout?"
        confirmLabel="Logout"
        onConfirm={handleLogoutConfirm}
        onOpenChange={setShowLogoutConfirm}
      />

      <ConfirmDialog
        open={showResetConfirm}
        title="Reset Preferences"
        message="This will clear all your preferences (risk tolerance, investment style, agent settings) and restart the onboarding process. Are you sure?"
        confirmLabel={isResetting ? 'Resetting...' : 'Reset & Re-onboard'}
        onConfirm={handleResetConfirm}
        onOpenChange={setShowResetConfirm}
      />
    </>
  );
}

export default UserConfigPanel;
