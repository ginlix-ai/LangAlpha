import React, { useState, useEffect, useRef } from 'react';
import { X, User } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { updateCurrentUser, getCurrentUser, updatePreferences, getPreferences,uploadAvatar } from '../utils/api';

/**
 * UserConfigPanel Component
 * 
 * Modal panel for configuring user information and preferences.
 * Two-page panel:
 * 1. User Info: Email, Name, Timezone, Locale
 * 2. Preferences: Risk tolerance, Investment preferences, Agent preferences
 * 
 * @param {boolean} isOpen - Whether the panel is open
 * @param {Function} onClose - Callback to close the panel
 */
function UserConfigPanel({ isOpen, onClose }) {
  const [activeTab, setActiveTab] = useState('userInfo'); // 'userInfo' or 'preferences'
  // user avatar upload state
  const [avatarUrl, setAvatarUrl] = useState(null);
  const fileInputRef = useRef(null);
  const [isUploadingAvatar, setIsUploadingAvatar] = useState(false);

  // User info state
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [timezone, setTimezone] = useState('');
  const [locale, setLocale] = useState('');
  
  // Preferences state
  const [riskTolerance, setRiskTolerance] = useState('');
  const [companyInterest, setCompanyInterest] = useState('');
  const [holdingPeriod, setHoldingPeriod] = useState('');
  const [analysisFocus, setAnalysisFocus] = useState('');
  const [outputStyle, setOutputStyle] = useState('');
  
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  // Common timezones organized by region
  const timezones = [
    { value: '', label: 'Select timezone...' },
    { group: 'Americas', options: [
      { value: 'America/New_York', label: 'Eastern Time (America/New_York)' },
      { value: 'America/Chicago', label: 'Central Time (America/Chicago)' },
      { value: 'America/Denver', label: 'Mountain Time (America/Denver)' },
      { value: 'America/Los_Angeles', label: 'Pacific Time (America/Los_Angeles)' },
      { value: 'America/Toronto', label: 'Eastern Time - Canada (America/Toronto)' },
      { value: 'America/Vancouver', label: 'Pacific Time - Canada (America/Vancouver)' },
      { value: 'America/Mexico_City', label: 'Central Time - Mexico (America/Mexico_City)' },
      { value: 'America/Sao_Paulo', label: 'Brasília Time (America/Sao_Paulo)' },
      { value: 'America/Buenos_Aires', label: 'Argentina Time (America/Buenos_Aires)' },
    ]},
    { group: 'Europe', options: [
      { value: 'Europe/London', label: 'Greenwich Mean Time (Europe/London)' },
      { value: 'Europe/Paris', label: 'Central European Time (Europe/Paris)' },
      { value: 'Europe/Berlin', label: 'Central European Time (Europe/Berlin)' },
      { value: 'Europe/Moscow', label: 'Moscow Time (Europe/Moscow)' },
      { value: 'Europe/Istanbul', label: 'Turkey Time (Europe/Istanbul)' },
    ]},
    { group: 'Asia', options: [
      { value: 'Asia/Shanghai', label: 'China Standard Time (Asia/Shanghai)' },
      { value: 'Asia/Tokyo', label: 'Japan Standard Time (Asia/Tokyo)' },
      { value: 'Asia/Hong_Kong', label: 'Hong Kong Time (Asia/Hong_Kong)' },
      { value: 'Asia/Singapore', label: 'Singapore Time (Asia/Singapore)' },
      { value: 'Asia/Dubai', label: 'Gulf Standard Time (Asia/Dubai)' },
      { value: 'Asia/Kolkata', label: 'India Standard Time (Asia/Kolkata)' },
      { value: 'Asia/Seoul', label: 'Korea Standard Time (Asia/Seoul)' },
    ]},
    { group: 'Oceania', options: [
      { value: 'Australia/Sydney', label: 'Australian Eastern Time (Australia/Sydney)' },
      { value: 'Australia/Melbourne', label: 'Australian Eastern Time (Australia/Melbourne)' },
      { value: 'Australia/Perth', label: 'Australian Western Time (Australia/Perth)' },
      { value: 'Pacific/Auckland', label: 'New Zealand Time (Pacific/Auckland)' },
    ]},
    { group: 'Other', options: [
      { value: 'UTC', label: 'Coordinated Universal Time (UTC)' },
      { value: 'GMT', label: 'Greenwich Mean Time (GMT)' },
    ]},
  ];

  // Common locales
  const locales = [
    { value: '', label: 'Select locale...' },
    { value: 'en-US', label: 'English (United States)' },
    { value: 'en-GB', label: 'English (United Kingdom)' },
    { value: 'en-CA', label: 'English (Canada)' },
    { value: 'en-AU', label: 'English (Australia)' },
    { value: 'zh-CN', label: 'Chinese (Simplified, China)' },
    { value: 'zh-TW', label: 'Chinese (Traditional, Taiwan)' },
    { value: 'zh-HK', label: 'Chinese (Traditional, Hong Kong)' },
    { value: 'fr-FR', label: 'French (France)' },
    { value: 'de-DE', label: 'German (Germany)' },
    { value: 'ja-JP', label: 'Japanese (Japan)' },
    { value: 'ko-KR', label: 'Korean (Korea)' },
    { value: 'es-ES', label: 'Spanish (Spain)' },
    { value: 'es-MX', label: 'Spanish (Mexico)' },
    { value: 'pt-BR', label: 'Portuguese (Brazil)' },
    { value: 'ru-RU', label: 'Russian (Russia)' },
    { value: 'ar-SA', label: 'Arabic (Saudi Arabia)' },
    { value: 'hi-IN', label: 'Hindi (India)' },
  ];

  // Load current user data and preferences when panel opens
  useEffect(() => {
    if (isOpen) {
      setIsLoading(true);
      Promise.all([loadUserData(), loadPreferencesData()])
        .finally(() => setIsLoading(false));
    }
  }, [isOpen]);

  /**
   * Loads current user data to populate the form
   */
  const loadUserData = async () => {
    try {
      const userData = await getCurrentUser();
      if (userData?.user) {
        setEmail(userData.user.email || '');
        setName(userData.user.name || '');
        setTimezone(userData.user.timezone || '');
        setLocale(userData.user.locale || '');
        const url = userData.user.avatar_url;
        const version = userData.user.updated_at;
        setAvatarUrl(url ? `${url}?v=${version}` : null);
      }
    } catch (err) {
      console.error('Error loading user data:', err);
      // Don't show error on load - user might not exist yet
    }
  };

  /**
   * Loads current preferences data to populate the form
   * API returns UserPreferencesResponse directly (not nested under 'preferences')
   */
  const loadPreferencesData = async () => {
    try {
      const preferencesData = await getPreferences();
      // API returns UserPreferencesResponse directly
      if (preferencesData) {
        setRiskTolerance(preferencesData.risk_preference?.risk_tolerance || '');
        setCompanyInterest(preferencesData.investment_preference?.company_interest || '');
        setHoldingPeriod(preferencesData.investment_preference?.holding_period || '');
        setAnalysisFocus(preferencesData.investment_preference?.analysis_focus || '');
        setOutputStyle(preferencesData.agent_preference?.output_style || '');
      }
    } catch (err) {
      console.error('Error loading preferences data:', err);
      // Don't show error on load - preferences might not exist yet (404 is expected for new users)
    }
  };

  /**
   * Handles avatar file selection and upload
   */
  const handleAvatarChange = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    
    setIsUploadingAvatar(true);
    try {
      const { avatar_url } = await uploadAvatar(file);
      setAvatarUrl(`${avatar_url}?t=${Date.now()}`);
    } catch (err) {
      console.error('Failed to upload avatar:', err);
      setError('Failed to upload avatar');
    } finally {
      setIsUploadingAvatar(false);
    }
  };


  /**
   * Handles user info form submission
   */
  const handleUserInfoSubmit = async (e) => {
    e.preventDefault();
    
    setIsSubmitting(true);
    setError(null);

    try {
      // Build request body with only non-empty fields
      const userData = {};
      if (email.trim()) userData.email = email.trim();
      if (name.trim()) userData.name = name.trim();
      if (timezone) userData.timezone = timezone;
      if (locale) userData.locale = locale;

      // Only submit if at least one field is provided
      if (Object.keys(userData).length === 0) {
        setError('Please fill in at least one field');
        setIsSubmitting(false);
        return;
      }

      await updateCurrentUser(userData);
      onClose();
    } catch (err) {
      setError(err.message || 'Failed to update user information');
    } finally {
      setIsSubmitting(false);
    }
  };

  /**
   * Handles preferences form submission
   */
  const handlePreferencesSubmit = async (e) => {
    e.preventDefault();
    
    setIsSubmitting(true);
    setError(null);

    try {
      // Build request body with only non-empty fields
      const preferences = {};
      
      // Risk preference
      if (riskTolerance) {
        preferences.risk_preference = {
          risk_tolerance: riskTolerance
        };
      }
      
      // Investment preference
      const investmentPrefs = {};
      if (companyInterest) investmentPrefs.company_interest = companyInterest;
      if (holdingPeriod) investmentPrefs.holding_period = holdingPeriod;
      if (analysisFocus) investmentPrefs.analysis_focus = analysisFocus;
      if (Object.keys(investmentPrefs).length > 0) {
        preferences.investment_preference = investmentPrefs;
      }
      
      // Agent preference
      if (outputStyle) {
        preferences.agent_preference = {
          output_style: outputStyle
        };
      }

      // Only submit if at least one field is provided
      if (Object.keys(preferences).length === 0) {
        setError('Please fill in at least one field');
        setIsSubmitting(false);
        return;
      }

      await updatePreferences(preferences);

      // Mark onboarding as completed after saving preferences
      await updateCurrentUser({ onboarding_completed: true });

      onClose();
    } catch (err) {
      setError(err.message || 'Failed to update preferences');
    } finally {
      setIsSubmitting(false);
    }
  };

  /**
   * Handles panel close
   */
  const handleClose = () => {
    setError(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
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
        {/* Close button */}
        <button
          onClick={handleClose}
          className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-white/10"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <X className="h-5 w-5" />
        </button>

        {/* Header */}
        <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>
          User Settings
        </h2>

        {/* Tab Navigation */}
        <div className="flex gap-2 mb-6 border-b" style={{ borderColor: 'var(--color-border-muted)' }}>
          <button
            type="button"
            onClick={() => setActiveTab('userInfo')}
            className="px-4 py-2 text-sm font-medium transition-colors relative"
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
            className="px-4 py-2 text-sm font-medium transition-colors relative"
            style={{
              color: activeTab === 'preferences' ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              borderBottom: activeTab === 'preferences' ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
            }}
          >
            Preferences
          </button>
        </div>

        {/* Loading state */}
        {isLoading && (
          <div className="flex items-center justify-center py-8">
            <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>
              Loading...
            </p>
          </div>
        )}

        {/* User Info Form */}
        {!isLoading && activeTab === 'userInfo' && (
          <form onSubmit={handleUserInfoSubmit} className="space-y-5">
            {/* Avatar Upload */}
            <div className="flex items-center gap-4 mb-6 pb-6" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
              <div 
                className="h-16 w-16 rounded-full flex items-center justify-center cursor-pointer transition-colors overflow-hidden"
                style={{ backgroundColor: 'var(--color-accent-soft)' }}
                onClick={() => fileInputRef.current?.click()}
              >
                {avatarUrl ? (
                  <img src={avatarUrl} alt="avatar" className="h-full w-full object-cover" />
                ) : (
                  <User className="h-8 w-8" style={{ color: 'var(--color-accent-primary)' }} />
                )}
              </div>
              <div>
                <button
                  type="button"
                  onClick={() => fileInputRef.current?.click()}
                  disabled={isUploadingAvatar}
                  className="px-3 py-1.5 rounded-md text-sm font-medium transition-colors"
                  style={{
                    backgroundColor: 'var(--color-accent-soft)',
                    color: 'var(--color-accent-primary)',
                  }}
                >
                  {isUploadingAvatar ? 'Uploading...' : 'Change Avatar'}
                </button>
                <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
                  JPG, PNG, GIF or WebP. Max 10MB.
                </p>
              </div>
              <input
                type="file"
                ref={fileInputRef}
                onChange={handleAvatarChange}
                accept="image/png,image/jpeg,image/gif,image/webp"
                style={{ display: 'none' }}
              />
            </div>

            {/* Email input */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Email <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
              <Input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Enter your email"
                className="w-full"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  border: '1px solid var(--color-border-muted)',
                  color: 'var(--color-text-primary)',
                }}
                disabled={isSubmitting}
              />
            </div>

            {/* Name input */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Name <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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

            {/* Timezone select */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Timezone <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                {timezones.map((item, index) => {
                  if (item.value !== undefined) {
                    // Regular option
                    return (
                      <option key={index} value={item.value} style={{ backgroundColor: 'var(--color-bg-card)' }}>
                        {item.label}
                      </option>
                    );
                  } else {
                    // Group with options
                    return (
                      <optgroup key={index} label={item.group} style={{ backgroundColor: 'var(--color-bg-card)' }}>
                        {item.options.map((opt, optIndex) => (
                          <option
                            key={`${index}-${optIndex}`}
                            value={opt.value}
                            style={{ backgroundColor: 'var(--color-bg-card)' }}
                          >
                            {opt.label}
                          </option>
                        ))}
                      </optgroup>
                    );
                  }
                })}
              </select>
            </div>

            {/* Locale select */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Locale <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                {locales.map((item, index) => (
                  <option key={index} value={item.value} style={{ backgroundColor: 'var(--color-bg-card)' }}>
                    {item.label}
                  </option>
                ))}
              </select>
            </div>

            {/* Error message */}
            {error && (
              <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
                  {error}
                </p>
              </div>
            )}

            {/* Action buttons */}
            <div className="flex gap-3 justify-end pt-4">
              <button
                type="button"
                onClick={handleClose}
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed"
                style={{ color: 'var(--color-text-primary)' }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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

        {/* Preferences Form */}
        {!isLoading && activeTab === 'preferences' && (
          <form onSubmit={handlePreferencesSubmit} className="space-y-5">
            {/* Risk Tolerance */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Risk Tolerance <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select risk tolerance...</option>
                <option value="low" style={{ backgroundColor: 'var(--color-bg-card)' }}>Low</option>
                <option value="medium" style={{ backgroundColor: 'var(--color-bg-card)' }}>Medium</option>
                <option value="high" style={{ backgroundColor: 'var(--color-bg-card)' }}>High</option>
                <option value="long_term_focus" style={{ backgroundColor: 'var(--color-bg-card)' }}>Long-term Focus</option>
              </select>
            </div>

            {/* Company Interest */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Company Interest <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select company interest...</option>
                <option value="growth" style={{ backgroundColor: 'var(--color-bg-card)' }}>Growth</option>
                <option value="stable" style={{ backgroundColor: 'var(--color-bg-card)' }}>Stable</option>
                <option value="value" style={{ backgroundColor: 'var(--color-bg-card)' }}>Value</option>
                <option value="esg" style={{ backgroundColor: 'var(--color-bg-card)' }}>ESG</option>
              </select>
            </div>

            {/* Holding Period */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Holding Period <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select holding period...</option>
                <option value="short_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Short-term</option>
                <option value="mid_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Mid-term</option>
                <option value="long_term" style={{ backgroundColor: 'var(--color-bg-card)' }}>Long-term</option>
                <option value="flexible" style={{ backgroundColor: 'var(--color-bg-card)' }}>Flexible</option>
              </select>
            </div>

            {/* Analysis Focus */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Analysis Focus <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select analysis focus...</option>
                <option value="growth" style={{ backgroundColor: 'var(--color-bg-card)' }}>Growth</option>
                <option value="valuation" style={{ backgroundColor: 'var(--color-bg-card)' }}>Valuation</option>
                <option value="moat" style={{ backgroundColor: 'var(--color-bg-card)' }}>Moat</option>
                <option value="risk" style={{ backgroundColor: 'var(--color-bg-card)' }}>Risk</option>
              </select>
            </div>

            {/* Output Style */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
                Output Style <span style={{ color: 'var(--color-text-secondary)', fontSize: '12px' }}>(Optional)</span>
              </label>
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
                <option value="" style={{ backgroundColor: 'var(--color-bg-card)' }}>Select output style...</option>
                <option value="summary" style={{ backgroundColor: 'var(--color-bg-card)' }}>Summary</option>
                <option value="data" style={{ backgroundColor: 'var(--color-bg-card)' }}>Data</option>
                <option value="deep_dive" style={{ backgroundColor: 'var(--color-bg-card)' }}>Deep Dive</option>
                <option value="quick" style={{ backgroundColor: 'var(--color-bg-card)' }}>Quick</option>
              </select>
            </div>

            {/* Error message */}
            {error && (
              <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
                <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
                  {error}
                </p>
              </div>
            )}

            {/* Action buttons */}
            <div className="flex gap-3 justify-end pt-4">
              <button
                type="button"
                onClick={handleClose}
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed"
                style={{ color: 'var(--color-text-primary)' }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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
      </div>
    </div>
  );
}

export default UserConfigPanel;
