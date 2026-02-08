import React, { useState, useEffect, useRef } from 'react';
import { X, User, LogOut } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { updateCurrentUser, getCurrentUser, updatePreferences, getPreferences, uploadAvatar } from '../utils/api';
import { useAuth } from '../../../contexts/AuthContext';
import ConfirmDialog from './ConfirmDialog';

/**
 * UserConfigPanel Component
 *
 * Modal panel for:
 * - When not logged in (requireLogin): Login by email or Sign up (email + name). Cannot be closed.
 * - When logged in: User info (email read-only), preferences, and logout button.
 *
 * @param {boolean} isOpen - Whether the panel is open
 * @param {Function} onClose - Callback to close the panel
 * @param {boolean} requireLogin - If true and not logged in, show login/signup and prevent closing
 */
function UserConfigPanel({ isOpen, onClose, requireLogin = false }) {
  const { userId, user: authUser, login, signup, logout, refreshUser } = useAuth();
  const [mode, setMode] = useState('login'); // 'login' | 'signup'
  const [activeTab, setActiveTab] = useState('userInfo');
  const [avatarUrl, setAvatarUrl] = useState(null);
  const fileInputRef = useRef(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  const [loginEmail, setLoginEmail] = useState('');
  const [signupEmail, setSignupEmail] = useState('');
  const [signupName, setSignupName] = useState('');

  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');

  const [riskTolerance, setRiskTolerance] = useState('');
  const [companyInterest, setCompanyInterest] = useState('');
  const [holdingPeriod, setHoldingPeriod] = useState('');
  const [analysisFocus, setAnalysisFocus] = useState('');
  const [outputStyle, setOutputStyle] = useState('');

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);

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

  const isLoggedIn = !!userId;
  const canClose = !requireLogin || isLoggedIn;

  useEffect(() => {
    if (isOpen && isLoggedIn) {
      setIsLoading(true);
      Promise.all([loadUserData(), loadPreferencesData()])
        .finally(() => setIsLoading(false));
    }
  }, [isOpen, isLoggedIn, userId]);

  const loadUserData = async () => {
    try {
      const userData = await getCurrentUser(userId);
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
      const preferencesData = await getPreferences(userId);
      if (preferencesData) {
        setRiskTolerance(preferencesData.risk_preference?.risk_tolerance || '');
        setCompanyInterest(preferencesData.investment_preference?.company_interest || '');
        setHoldingPeriod(preferencesData.investment_preference?.holding_period || '');
        setAnalysisFocus(preferencesData.investment_preference?.analysis_focus || '');
        setOutputStyle(preferencesData.agent_preference?.output_style || '');
      }
    } catch {}
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      await login(loginEmail);
      setLoginEmail('');
      if (!requireLogin) onClose();
    } catch (err) {
      const status = err?.response?.status;
      if (status === 404) {
        setError('No account found with this email. Please sign up.');
      } else {
        setError(err?.response?.data?.detail || err.message || 'Login failed');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSignup = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      await signup(signupEmail, signupName);
      setSignupEmail('');
      setSignupName('');
      if (!requireLogin) onClose();
    } catch (err) {
      const status = err?.response?.status;
      if (status === 409) {
        setError('An account with this email already exists. Please login.');
      } else {
        setError(err?.response?.data?.detail || err.message || 'Sign up failed');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleAvatarChange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setIsUploadingAvatar(true);
    try {
      const { avatar_url } = await uploadAvatar(file, userId);
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
    try {
      const userData = {};
      if (name.trim()) userData.name = name.trim();
      if (timezone) userData.timezone = timezone;
      if (locale) userData.locale = locale;
      if (Object.keys(userData).length > 0) {
        await updateCurrentUser(userData, userId);
        refreshUser();
      }
      onClose();
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
        await updatePreferences(preferences, userId);
        await updateCurrentUser({ onboarding_completed: true }, userId);
        refreshUser();
      }
      onClose();
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

  const handleClose = () => {
    if (!canClose) return;
    setError(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <>
      <div
        className="fixed inset-0 z-50 flex items-center justify-center"
        style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
        onClick={canClose ? handleClose : undefined}
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
          {canClose && (
            <button
              onClick={handleClose}
              className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-white/10"
              style={{ color: 'var(--color-text-primary)' }}
            >
              <X className="h-5 w-5" />
            </button>
          )}

          {/* ----- Not logged in: Login / Sign up ----- */}
          {!isLoggedIn && (
            <>
              <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>
                Welcome
              </h2>
              <div className="flex gap-2 mb-6 border-b" style={{ borderColor: 'var(--color-border-muted)' }}>
                <button
                  type="button"
                  onClick={() => { setMode('login'); setError(null); }}
                  className="px-4 py-2 text-sm font-medium"
                  style={{
                    color: mode === 'login' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    borderBottom: mode === 'login' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
                  }}
                >
                  Login
                </button>
                <button
                  type="button"
                  onClick={() => { setMode('signup'); setError(null); }}
                  className="px-4 py-2 text-sm font-medium"
                  style={{
                    color: mode === 'signup' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                    borderBottom: mode === 'signup' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
                  }}
                >
                  Sign up
                </button>
              </div>

              {mode === 'login' && (
                <form onSubmit={handleLogin} className="space-y-5">
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Email</label>
                    <Input
                      type="email"
                      value={loginEmail}
                      onChange={(e) => setLoginEmail(e.target.value)}
                      placeholder="Enter your email"
                      className="w-full"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                      required
                    />
                  </div>
                  {error && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
                    </div>
                  )}
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full px-4 py-2 rounded-md text-sm font-medium"
                    style={{
                      backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                      color: 'var(--color-text-on-accent)',
                    }}
                  >
                    {isSubmitting ? 'Logging in...' : 'Login'}
                  </button>
                </form>
              )}

              {mode === 'signup' && (
                <form onSubmit={handleSignup} className="space-y-5">
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Email</label>
                    <Input
                      type="email"
                      value={signupEmail}
                      onChange={(e) => setSignupEmail(e.target.value)}
                      placeholder="Enter your email"
                      className="w-full"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                      required
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>Name</label>
                    <Input
                      type="text"
                      value={signupName}
                      onChange={(e) => setSignupName(e.target.value)}
                      placeholder="Enter your name"
                      className="w-full"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        border: '1px solid var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                      disabled={isSubmitting}
                      required
                    />
                  </div>
                  {error && (
                    <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                      <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
                    </div>
                  )}
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full px-4 py-2 rounded-md text-sm font-medium"
                    style={{
                      backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                      color: 'var(--color-text-on-accent)',
                    }}
                  >
                    {isSubmitting ? 'Creating account...' : 'Sign up'}
                  </button>
                </form>
              )}
            </>
          )}

          {/* ----- Logged in: User info + Preferences + Logout ----- */}
          {isLoggedIn && (
            <>
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
              </div>

              {isLoading && (
                <div className="flex items-center justify-center py-8">
                  <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>Loading...</p>
                </div>
              )}

              {!isLoading && activeTab === 'userInfo' && (
                <form onSubmit={handleUserInfoSubmit} className="space-y-5">
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
                      value={userId}
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
                    <div className="flex gap-3">
                      <button type="button" onClick={handleClose} disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                        Cancel
                      </button>
                      <button type="submit" disabled={isSubmitting}
                        className="px-4 py-2 rounded-md text-sm font-medium"
                        style={{
                          backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                          color: 'var(--color-text-on-accent)',
                        }}
                      >
                        {isSubmitting ? 'Updating...' : 'Update'}
                      </button>
                    </div>
                  </div>
                </form>
              )}

              {!isLoading && activeTab === 'preferences' && (
                <form onSubmit={handlePreferencesSubmit} className="space-y-5">
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

                  <div className="flex gap-3 justify-end pt-4">
                    <button type="button" onClick={handleClose} disabled={isSubmitting}
                      className="px-4 py-2 rounded-md text-sm font-medium hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                      Cancel
                    </button>
                    <button type="submit" disabled={isSubmitting}
                      className="px-4 py-2 rounded-md text-sm font-medium"
                      style={{
                        backgroundColor: isSubmitting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                        color: 'var(--color-text-on-accent)',
                      }}
                    >
                      {isSubmitting ? 'Updating...' : 'Update'}
                    </button>
                  </div>
                </form>
              )}
            </>
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
    </>
  );
}

export default UserConfigPanel;
