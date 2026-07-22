import { useEffect, useRef, useState } from 'react';
import { Link2, Unlink, ExternalLink, Shield, ClipboardCopy } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { initiateCodexDevice, pollCodexDevice, getCodexOAuthStatus, disconnectCodexOAuth, initiateClaudeOAuth, submitClaudeCallback, getClaudeOAuthStatus, disconnectClaudeOAuth } from '@/pages/Dashboard/utils/api';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useTranslation } from 'react-i18next';

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

/** Connected Accounts section of the model tab: the Codex device-code flow and
 * the Claude PKCE paste-back flow, each with its disclaimer dialog. Owns its
 * OAuth state end-to-end — fetches connection status when the section mounts
 * (i.e. when the model tab is first shown). */
export function ConnectedAccounts() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

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

  const [accountsError, setAccountsError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const [codexStatus, claudeStatus] = await Promise.all([
          getCodexOAuthStatus(),
          getClaudeOAuthStatus(),
        ]) as [OAuthStatus, OAuthStatus];
        if (cancelled) return;
        setCodexOAuthStatus(codexStatus || { connected: false });
        setClaudeOAuthStatus(claudeStatus || { connected: false });
      } catch {
        // Status stays disconnected; the connect flows still work.
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Cleanup device code polling on unmount
  useEffect(() => {
    return () => {
      if (codexPollRef.current) {
        clearInterval(codexPollRef.current);
        codexPollRef.current = null;
      }
    };
  }, []);

  const handleCodexConnectClick = () => {
    setShowCodexDisclaimer(true);
  };

  const handleCodexConnect = async () => {
    setShowCodexDisclaimer(false);
    setIsConnectingCodex(true);
    setAccountsError(null);
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
            queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
            queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
          }
          // result.pending → keep polling
        } catch {
          handleCodexDeviceCancel();
          setCodexDeviceError(t('settings.codexPollFailed'));
        }
      }, interval);
    } catch {
      setAccountsError(t('settings.codexFlowFailed'));
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
    setAccountsError(null);
    try {
      await disconnectCodexOAuth();
      setCodexOAuthStatus({ connected: false, account_id: null, email: null, plan_type: null });
      queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
      queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
    } catch {
      setAccountsError('Failed to disconnect Codex');
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
    setAccountsError(null);
    setClaudeError(null);
    try {
      const result = await initiateClaudeOAuth() as Record<string, unknown>;
      setClaudeAuthorizeUrl(result.authorize_url as string);
      // Open authorization page in new tab
      window.open(result.authorize_url as string, '_blank', 'noopener');
    } catch {
      setAccountsError(t('settings.claudeConnectFailed', 'Failed to initiate Claude OAuth'));
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
        queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
        queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
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
    setAccountsError(null);
    try {
      await disconnectClaudeOAuth();
      setClaudeOAuthStatus({ connected: false, account_id: null, email: null, plan_type: null });
      queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
      queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
    } catch {
      setAccountsError('Failed to disconnect Claude');
    } finally {
      setIsDisconnectingClaude(false);
    }
  };

  return (
    <>
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

        {accountsError && (
          <div className="p-3 rounded-md mt-2" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
            <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{accountsError}</p>
          </div>
        )}
      </div>

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
