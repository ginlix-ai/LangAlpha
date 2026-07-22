import { useState, useCallback, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Loader2, ExternalLink, AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { queryKeys } from '@/lib/queryKeys';
import {
  initiateCodexDevice,
  pollCodexDevice,
  initiateClaudeOAuth,
  submitClaudeCallback,
} from '@/pages/Dashboard/utils/api';
import { useTranslation } from 'react-i18next';
import { ProcessStep, DisclaimerBox, CopyButton, type LocationState } from './shared';

export function OAuthConnect({ state }: { state: LocationState }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { t } = useTranslation();

  const method = state.method ?? 'api_key';
  const provider = state.provider ?? '';
  const displayName = state.displayName ?? provider;
  const brandKey = state.brandKey ?? provider;

  // OAuth shared state
  const [oauthPhase, setOauthPhase] = useState<'disclaimer' | 'connecting' | 'active'>('disclaimer');
  const [oauthError, setOauthError] = useState<string | null>(null);
  const [agreed, setAgreed] = useState(false);

  // Codex device flow state
  const [codexUserCode, setCodexUserCode] = useState<string | null>(null);
  const [codexVerifyUrl, setCodexVerifyUrl] = useState<string | null>(null);
  const [codexPolling, setCodexPolling] = useState(false);

  // Claude OAuth state
  const [claudeAuthorizeUrl, setClaudeAuthorizeUrl] = useState<string | null>(null);
  const [claudeCallbackInput, setClaudeCallbackInput] = useState('');
  const [claudeSubmitting, setClaudeSubmitting] = useState(false);

  const isCodex = provider === 'codex-oauth';
  const isClaude = provider === 'claude-oauth';

  // Abort polling on unmount
  const cancelledRef = useRef(false);
  useEffect(() => {
    return () => { cancelledRef.current = true; };
  }, []);

  // ---------------------------------------------------------------------------
  // Codex OAuth handler
  // ---------------------------------------------------------------------------

  const handleCodexStart = useCallback(async () => {
    setOauthPhase('connecting');
    setOauthError(null);
    try {
      const result = await initiateCodexDevice();
      const userCode = result.user_code as string;
      const verifyUrl = result.verification_url as string;
      const interval = (result.interval as number) || 5;

      setCodexUserCode(userCode);
      setCodexVerifyUrl(verifyUrl);
      setOauthPhase('active');

      // Open verification URL in new tab
      window.open(verifyUrl, '_blank', 'noopener');

      // Start polling
      cancelledRef.current = false;
      setCodexPolling(true);
      const maxAttempts = 60;
      for (let i = 0; i < maxAttempts; i++) {
        await new Promise((r) => setTimeout(r, interval * 1000));
        if (cancelledRef.current) return;
        try {
          const pollResult = await pollCodexDevice();
          if ('success' in pollResult && pollResult.success) {
            await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
            await queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
            await queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
            navigate('/setup/models', {
              state: { method, provider, displayName, brandKey },
            });
            return;
          }
        } catch {
          // Continue polling
        }
      }
      setOauthError(t('setup.errorAuthTimeout'));
      setCodexPolling(false);
    } catch {
      setOauthError(t('setup.errorDeviceAuth'));
      setOauthPhase('disclaimer');
    }
  }, [queryClient, navigate, method, provider, displayName, brandKey, t]);

  // ---------------------------------------------------------------------------
  // Claude OAuth handlers
  // ---------------------------------------------------------------------------

  const handleClaudeStart = useCallback(async () => {
    setOauthPhase('connecting');
    setOauthError(null);
    try {
      const result = await initiateClaudeOAuth();
      const authorizeUrl = result.authorize_url as string;
      setClaudeAuthorizeUrl(authorizeUrl);
      setOauthPhase('active');
      // Open in new tab so user can paste code back here
      window.open(authorizeUrl, '_blank', 'noopener');
    } catch {
      setOauthError(t('setup.errorClaudeOAuth'));
      setOauthPhase('disclaimer');
    }
  }, [t]);

  const handleClaudeSubmit = useCallback(async () => {
    if (!claudeCallbackInput.trim()) return;
    setClaudeSubmitting(true);
    setOauthError(null);
    try {
      const result = await submitClaudeCallback(claudeCallbackInput.trim());
      if (result.success) {
        await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
        await queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
        await queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
        navigate('/setup/models', {
          state: { method, provider, displayName, brandKey },
        });
      } else {
        setOauthError((result as Record<string, unknown>).error as string ?? 'Authorization failed. Please try again.');
      }
    } catch {
      setOauthError(t('setup.errorInvalidCode'));
    } finally {
      setClaudeSubmitting(false);
    }
  }, [claudeCallbackInput, queryClient, navigate, method, provider, displayName, brandKey, t]);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

    return (
      <div className="flex flex-col gap-4 sm:gap-6">
        <div className="flex flex-col gap-1">
          <h2
            className="font-semibold"
            style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
          >
            {t('setup.connectTitle', { provider: displayName })}
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {isCodex
              ? t('setup.oauthCodexDesc')
              : t('setup.oauthClaudeDesc')}
          </p>
        </div>

        {/* Phase 1: Disclaimer + process flow */}
        {oauthPhase === 'disclaimer' && (
          <>
            {/* Process flow */}
            <div
              className="rounded-lg p-4"
              style={{
                background: 'var(--color-bg-surface)',
                border: '1px solid var(--color-border-default)',
              }}
            >
              <p
                className="text-xs font-medium uppercase tracking-wide mb-3"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('setup.howItWorks')}
              </p>
              <div className="space-y-3">
                {isCodex ? (
                  <>
                    <ProcessStep
                      number={1}
                      title={t('setup.codexStep1Title')}
                      description={t('setup.codexStep1Desc')}
                    />
                    <ProcessStep
                      number={2}
                      title={t('setup.codexStep2Title')}
                      description={t('setup.codexStep2Desc')}
                    />
                    <ProcessStep
                      number={3}
                      title={t('setup.codexStep3Title')}
                      description={t('setup.codexStep3Desc')}
                    />
                  </>
                ) : (
                  <>
                    <ProcessStep
                      number={1}
                      title={t('setup.claudeStep1Title')}
                      description={t('setup.claudeStep1Desc')}
                    />
                    <ProcessStep
                      number={2}
                      title={t('setup.claudeStep2Title')}
                      description={t('setup.claudeStep2Desc')}
                    />
                    <ProcessStep
                      number={3}
                      title={t('setup.claudeStep3Title')}
                      description={t('setup.claudeStep3Desc')}
                    />
                  </>
                )}
              </div>
            </div>

            {/* Disclaimer */}
            <DisclaimerBox provider={provider} />

            {/* Agreement checkbox */}
            <label className="flex items-start gap-3 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={agreed}
                onChange={(e) => setAgreed(e.target.checked)}
                className="mt-0.5 h-4 w-4 rounded border accent-[var(--color-accent-primary)]"
              />
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t(isClaude ? 'setup.oauthAgreeAnthropic' : 'setup.oauthAgreeOpenai')}
              </span>
            </label>

            {/* Action button */}
            <Button
              variant="default"
              disabled={!agreed}
              onClick={isCodex ? handleCodexStart : handleClaudeStart}
              className="w-full h-11"
            >
              <ExternalLink className="h-4 w-4 mr-1.5" />
              {isCodex ? t('setup.openVerification') : t('setup.openClaude')}
            </Button>
          </>
        )}

        {/* Phase 2: Connecting spinner */}
        {oauthPhase === 'connecting' && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
            <span className="ml-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('setup.connecting')}
            </span>
          </div>
        )}

        {/* Phase 3: Active — Codex device code display */}
        {oauthPhase === 'active' && isCodex && codexUserCode && (
          <div
            className="flex flex-col items-center gap-4 rounded-lg p-6"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" style={{ color: 'var(--color-warning, #f59e0b)' }} />
              <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('setup.enterCodeOnOpenai')}
              </p>
            </div>

            <div className="flex items-center gap-3">
              <code
                className="text-2xl font-mono font-bold tracking-widest px-4 py-2 rounded"
                style={{
                  background: 'var(--color-bg-page)',
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-default)',
                }}
              >
                {codexUserCode}
              </code>
              <CopyButton text={codexUserCode} />
            </div>

            {codexPolling && (
              <p
                className="text-xs flex items-center gap-1.5"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                <Loader2 className="h-3 w-3 animate-spin" />
                {t('setup.waitingApproval')}
              </p>
            )}

            <a
              href={codexVerifyUrl ?? '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-sm font-medium"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('setup.openVerificationAgain')}
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          </div>
        )}

        {/* Phase 3: Active — Claude paste-back input */}
        {oauthPhase === 'active' && isClaude && (
          <div
            className="flex flex-col gap-4 rounded-lg p-5"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <div className="flex flex-col gap-1">
              <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('setup.pasteAuthCode')}
              </p>
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('setup.pasteAuthCodeHint')}
              </p>
            </div>

            <div className="flex gap-2">
              <Input
                value={claudeCallbackInput}
                onChange={(e) => {
                  setClaudeCallbackInput(e.target.value);
                  setOauthError(null);
                }}
                placeholder={t('setup.pasteCodePlaceholder')}
                className="flex-1 font-mono text-sm"
                autoComplete="off"
                spellCheck={false}
              />
              <Button
                variant="default"
                disabled={claudeSubmitting || !claudeCallbackInput.trim()}
                onClick={handleClaudeSubmit}
                className="shrink-0"
              >
                {claudeSubmitting ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
                    {t('setup.submitting')}
                  </>
                ) : (
                  t('setup.submit')
                )}
              </Button>
            </div>

            <a
              href={claudeAuthorizeUrl ?? '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-xs self-start"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('setup.openClaudeAgain')}
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        )}

        {/* Error */}
        {oauthError && (
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {oauthError}
          </p>
        )}

        {/* Back button */}
        <div className="flex items-center justify-between pt-2">
          <Button variant="outline" onClick={handleBack}>
            {t('setup.back')}
          </Button>
        </div>
      </div>
    );
}
