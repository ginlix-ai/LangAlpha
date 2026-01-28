import React, { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { updateCurrentUser, getCurrentUser, updatePreferences, getPreferences } from '../utils/api';

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
      }
    } catch (err) {
      console.error('Error loading user data:', err);
      // Don't show error on load - user might not exist yet
    }
  };

  /**
   * Loads current preferences data to populate the form
   */
  const loadPreferencesData = async () => {
    try {
      const preferencesData = await getPreferences();
      if (preferencesData?.preferences) {
        const prefs = preferencesData.preferences;
        setRiskTolerance(prefs.risk_preference?.risk_tolerance || '');
        setCompanyInterest(prefs.investment_preference?.company_interest || '');
        setHoldingPeriod(prefs.investment_preference?.holding_period || '');
        setAnalysisFocus(prefs.investment_preference?.analysis_focus || '');
        setOutputStyle(prefs.agent_preference?.output_style || '');
      }
    } catch (err) {
      console.error('Error loading preferences data:', err);
      // Don't show error on load - preferences might not exist yet
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
      style={{ backgroundColor: 'rgba(0, 0, 0, 0.7)' }}
      onClick={handleClose}
    >
      <div
        className="relative w-full max-w-2xl rounded-lg p-6"
        style={{
          backgroundColor: '#1B1D25',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          maxHeight: '90vh',
          overflowY: 'auto',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Close button */}
        <button
          onClick={handleClose}
          className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-white/10"
          style={{ color: '#FFFFFF' }}
        >
          <X className="h-5 w-5" />
        </button>

        {/* Header */}
        <h2 className="text-xl font-semibold mb-6" style={{ color: '#FFFFFF' }}>
          User Settings
        </h2>

        {/* Tab Navigation */}
        <div className="flex gap-2 mb-6 border-b" style={{ borderColor: 'rgba(255, 255, 255, 0.1)' }}>
          <button
            type="button"
            onClick={() => setActiveTab('userInfo')}
            className="px-4 py-2 text-sm font-medium transition-colors relative"
            style={{
              color: activeTab === 'userInfo' ? '#FFFFFF' : 'rgba(255, 255, 255, 0.6)',
              borderBottom: activeTab === 'userInfo' ? '2px solid #6155F5' : '2px solid transparent',
            }}
          >
            User Info
          </button>
          <button
            type="button"
            onClick={() => setActiveTab('preferences')}
            className="px-4 py-2 text-sm font-medium transition-colors relative"
            style={{
              color: activeTab === 'preferences' ? '#FFFFFF' : 'rgba(255, 255, 255, 0.6)',
              borderBottom: activeTab === 'preferences' ? '2px solid #6155F5' : '2px solid transparent',
            }}
          >
            Preferences
          </button>
        </div>

        {/* Loading state */}
        {isLoading && (
          <div className="flex items-center justify-center py-8">
            <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.7 }}>
              Loading...
            </p>
          </div>
        )}

        {/* User Info Form */}
        {!isLoading && activeTab === 'userInfo' && (
          <form onSubmit={handleUserInfoSubmit} className="space-y-5">
            {/* Email input */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Email <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <Input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Enter your email"
                className="w-full"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              />
            </div>

            {/* Name input */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Name <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <Input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Enter your name"
                className="w-full"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              />
            </div>

            {/* Timezone select */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Timezone <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={timezone}
                onChange={(e) => setTimezone(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                {timezones.map((item, index) => {
                  if (item.value !== undefined) {
                    // Regular option
                    return (
                      <option key={index} value={item.value} style={{ backgroundColor: '#0A0A0A' }}>
                        {item.label}
                      </option>
                    );
                  } else {
                    // Group with options
                    return (
                      <optgroup key={index} label={item.group} style={{ backgroundColor: '#0A0A0A' }}>
                        {item.options.map((opt, optIndex) => (
                          <option
                            key={`${index}-${optIndex}`}
                            value={opt.value}
                            style={{ backgroundColor: '#0A0A0A' }}
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
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Locale <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={locale}
                onChange={(e) => setLocale(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                {locales.map((item, index) => (
                  <option key={index} value={item.value} style={{ backgroundColor: '#0A0A0A' }}>
                    {item.label}
                  </option>
                ))}
              </select>
            </div>

            {/* Error message */}
            {error && (
              <div className="p-3 rounded-md" style={{ backgroundColor: 'rgba(255, 56, 60, 0.1)', border: '1px solid rgba(255, 56, 60, 0.3)' }}>
                <p className="text-sm" style={{ color: '#FF383C' }}>
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
                style={{ color: '#FFFFFF' }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                style={{
                  backgroundColor: isSubmitting ? 'rgba(97, 85, 245, 0.5)' : '#6155F5',
                  color: '#FFFFFF',
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
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Risk Tolerance <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={riskTolerance}
                onChange={(e) => setRiskTolerance(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                <option value="" style={{ backgroundColor: '#0A0A0A' }}>Select risk tolerance...</option>
                <option value="low" style={{ backgroundColor: '#0A0A0A' }}>Low</option>
                <option value="medium" style={{ backgroundColor: '#0A0A0A' }}>Medium</option>
                <option value="high" style={{ backgroundColor: '#0A0A0A' }}>High</option>
                <option value="long_term_focus" style={{ backgroundColor: '#0A0A0A' }}>Long-term Focus</option>
              </select>
            </div>

            {/* Company Interest */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Company Interest <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={companyInterest}
                onChange={(e) => setCompanyInterest(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                <option value="" style={{ backgroundColor: '#0A0A0A' }}>Select company interest...</option>
                <option value="growth" style={{ backgroundColor: '#0A0A0A' }}>Growth</option>
                <option value="stable" style={{ backgroundColor: '#0A0A0A' }}>Stable</option>
                <option value="value" style={{ backgroundColor: '#0A0A0A' }}>Value</option>
                <option value="esg" style={{ backgroundColor: '#0A0A0A' }}>ESG</option>
              </select>
            </div>

            {/* Holding Period */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Holding Period <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={holdingPeriod}
                onChange={(e) => setHoldingPeriod(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                <option value="" style={{ backgroundColor: '#0A0A0A' }}>Select holding period...</option>
                <option value="short_term" style={{ backgroundColor: '#0A0A0A' }}>Short-term</option>
                <option value="mid_term" style={{ backgroundColor: '#0A0A0A' }}>Mid-term</option>
                <option value="long_term" style={{ backgroundColor: '#0A0A0A' }}>Long-term</option>
                <option value="flexible" style={{ backgroundColor: '#0A0A0A' }}>Flexible</option>
              </select>
            </div>

            {/* Analysis Focus */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Analysis Focus <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={analysisFocus}
                onChange={(e) => setAnalysisFocus(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                <option value="" style={{ backgroundColor: '#0A0A0A' }}>Select analysis focus...</option>
                <option value="growth" style={{ backgroundColor: '#0A0A0A' }}>Growth</option>
                <option value="valuation" style={{ backgroundColor: '#0A0A0A' }}>Valuation</option>
                <option value="moat" style={{ backgroundColor: '#0A0A0A' }}>Moat</option>
                <option value="risk" style={{ backgroundColor: '#0A0A0A' }}>Risk</option>
              </select>
            </div>

            {/* Output Style */}
            <div>
              <label className="block text-sm font-medium mb-2" style={{ color: '#FFFFFF' }}>
                Output Style <span style={{ color: '#999999', fontSize: '12px' }}>(Optional)</span>
              </label>
              <select
                value={outputStyle}
                onChange={(e) => setOutputStyle(e.target.value)}
                className="w-full rounded-md px-3 py-2 text-sm"
                style={{
                  backgroundColor: '#0A0A0A',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  color: '#FFFFFF',
                }}
                disabled={isSubmitting}
              >
                <option value="" style={{ backgroundColor: '#0A0A0A' }}>Select output style...</option>
                <option value="summary" style={{ backgroundColor: '#0A0A0A' }}>Summary</option>
                <option value="data" style={{ backgroundColor: '#0A0A0A' }}>Data</option>
                <option value="deep_dive" style={{ backgroundColor: '#0A0A0A' }}>Deep Dive</option>
                <option value="quick" style={{ backgroundColor: '#0A0A0A' }}>Quick</option>
              </select>
            </div>

            {/* Error message */}
            {error && (
              <div className="p-3 rounded-md" style={{ backgroundColor: 'rgba(255, 56, 60, 0.1)', border: '1px solid rgba(255, 56, 60, 0.3)' }}>
                <p className="text-sm" style={{ color: '#FF383C' }}>
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
                style={{ color: '#FFFFFF' }}
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isSubmitting}
                className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                style={{
                  backgroundColor: isSubmitting ? 'rgba(97, 85, 245, 0.5)' : '#6155F5',
                  color: '#FFFFFF',
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
