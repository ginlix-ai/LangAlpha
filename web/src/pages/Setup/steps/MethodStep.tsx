import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Loader2, Ticket, Link2, Code2, Key, Monitor, CheckCircle2, ChevronDown, ChevronUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { api } from '@/api/client';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useConfiguredProviders, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { deleteUserApiKey, disconnectCodexOAuth, disconnectClaudeOAuth, getCurrentUser } from '@/pages/Dashboard/utils/api';
import type { AccessType } from '@/components/model/types';
import { isPlatformMode } from '@/config/hostMode';
import { useAllModels } from '@/hooks/useAllModels';

// ---------------------------------------------------------------------------
// Method card data
// ---------------------------------------------------------------------------

const ALL_METHODS: Array<{
  id: AccessType;
  icon: typeof Link2;
  titleKey: string;
  descKey: string;
  platformOnly?: boolean;
  ossOnly?: boolean;
}> = [
  {
    id: 'oauth',
    icon: Link2,
    titleKey: 'setup.methodOAuth',
    descKey: 'setup.methodOAuthDesc',
  },
  {
    id: 'coding_plan',
    icon: Code2,
    titleKey: 'setup.methodCodingPlan',
    descKey: 'setup.methodCodingPlanDesc',
  },
  {
    id: 'api_key',
    icon: Key,
    titleKey: 'setup.methodApiKey',
    descKey: 'setup.methodApiKeyDesc',
  },
  {
    id: 'local',
    icon: Monitor,
    titleKey: 'setup.methodLocal',
    descKey: 'setup.methodLocalDesc',
    ossOnly: true,
  },
];

const METHODS = ALL_METHODS.filter((m) => {
  if (m.ossOnly && isPlatformMode) return false;
  if (m.platformOnly && !isPlatformMode) return false;
  return true;
});

// ---------------------------------------------------------------------------
// Configured providers banner
// ---------------------------------------------------------------------------

function ConfiguredBanner({
  providers,
  onSkip,
  onRemove,
}: {
  providers: ConfiguredProvider[];
  onSkip: () => void;
  onRemove: (provider: ConfiguredProvider) => void;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);

  if (providers.length === 0) return null;

  return (
    <div
      className="rounded-lg overflow-hidden"
      style={{
        background: 'var(--color-bg-surface)',
        border: '1px solid var(--color-border-default)',
      }}
    >
      {/* Header row */}
      <div className="flex items-center justify-between p-4">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-1.5 text-left"
        >
          <CheckCircle2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-success)' }} />
          <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.providersConnected', { count: providers.length })}
          </span>
          {expanded
            ? <ChevronUp className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
            : <ChevronDown className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
          }
        </button>
        <Button variant="outline" size="sm" onClick={onSkip} className="shrink-0 ml-3">
          {t('setup.nextStep')}
        </Button>
      </div>

      {/* Expandable provider list */}
      {expanded && (
        <div
          className="flex flex-col gap-0"
          style={{ borderTop: '1px solid var(--color-border-default)' }}
        >
          {providers.map((p) => (
            <div
              key={p.provider}
              className="flex items-center justify-between px-4 py-2.5"
              style={{ borderBottom: '1px solid var(--color-border-default)' }}
            >
              <div className="flex items-center gap-2 min-w-0">
                <span className="text-sm truncate" style={{ color: 'var(--color-text-primary)' }}>
                  {p.display_name}
                </span>
                <span
                  className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0"
                  style={{
                    background: p.access_type === 'oauth' ? 'var(--color-accent-soft)' : 'var(--color-bg-page)',
                    color: p.access_type === 'oauth' ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
                    border: p.access_type === 'oauth' ? 'none' : '1px solid var(--color-border-default)',
                  }}
                >
                  {p.access_type === 'oauth' ? t('setup.oauthBadge')
                    : p.access_type === 'local' ? t('setup.localBadge')
                    : p.access_type === 'coding_plan' ? t('setup.codingPlanBadge')
                    : t('setup.apiKeyBadge')}
                </span>
              </div>
              <button
                type="button"
                onClick={() => onRemove(p)}
                className="text-xs shrink-0 px-2 py-1 rounded transition-colors hover:opacity-80"
                style={{ color: 'var(--color-loss)' }}
              >
                {p.access_type === 'oauth' ? t('setup.disconnectProvider') : t('setup.removeProvider')}
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// MethodStep
// ---------------------------------------------------------------------------

export default function MethodStep() {
  const navigate = useNavigate();
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { providers: configuredProviders, hasAny: hasConfigured } = useConfiguredProviders();
  const { user, isLoading: userLoading } = useUser();
  const [localRedeemed, setLocalRedeemed] = useState(false);
  const hasPlatformAccess = (user?.access_tier ?? -1) >= 0 || localRedeemed;
  // Wait for user data before using hasPlatformAccess to avoid flicker
  const canSkip = hasConfigured || (!userLoading && hasPlatformAccess);
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { metadata } = useAllModels();

  const [selected, setSelected] = useState<AccessType | null>(null);

  // Invitation code state
  const [showInvitation, setShowInvitation] = useState(false);
  const [invitationCode, setInvitationCode] = useState('');
  const [invitationError, setInvitationError] = useState<string | null>(null);
  const [redeemingInvitation, setRedeemingInvitation] = useState(false);

  const handleRemoveProvider = useCallback(async (provider: ConfiguredProvider) => {
    try {
      if (provider.access_type === 'oauth') {
        if (provider.provider === 'codex-oauth') {
          await disconnectCodexOAuth();
          queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
        } else if (provider.provider === 'claude-oauth') {
          await disconnectClaudeOAuth();
          queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
        }
      } else {
        // Remove API key
        await deleteUserApiKey(provider.provider).catch(() => {});
        // Clean custom_providers, custom_models, and any model preference
        // fields that reference models from the removed provider.
        const prefs = preferences as Record<string, unknown> | null;
        const otherPref = (prefs?.other_preference ?? {}) as Record<string, unknown>;
        const existingProviders = (otherPref.custom_providers as Array<{ name: string }>) ?? [];
        const existingModels = (otherPref.custom_models as Array<{ provider: string; name: string }>) ?? [];
        const isCustom = existingProviders.some(cp => cp.name === provider.provider);
        const remainingProviders = isCustom ? existingProviders.filter(cp => cp.name !== provider.provider) : existingProviders;
        const remainingModels = existingModels.filter(cm => cm.provider !== provider.provider);
        // Collect ALL model names belonging to this provider (custom + built-in)
        const removedModelNames = new Set([
          ...existingModels.filter(cm => cm.provider === provider.provider).map(cm => cm.name),
          ...Object.entries(metadata)
            .filter(([, meta]) => meta?.provider === provider.provider)
            .map(([name]) => name),
        ]);
        const cleanModelPref = (val: unknown) =>
          typeof val === 'string' && removedModelNames.has(val) ? null : undefined;
        const prefUpdate: Record<string, unknown> = {
          custom_providers: remainingProviders.length > 0 ? remainingProviders : null,
          custom_models: remainingModels.length > 0 ? remainingModels : null,
        };
        // Clear subsidiary/preferred model fields if they reference a removed model
        if (cleanModelPref(otherPref.compaction_model) === null) prefUpdate.compaction_model = null;
        if (cleanModelPref(otherPref.summarization_model) === null) prefUpdate.summarization_model = null;
        if (cleanModelPref(otherPref.fetch_model) === null) prefUpdate.fetch_model = null;
        if (cleanModelPref(otherPref.preferred_model) === null) prefUpdate.preferred_model = null;
        if (cleanModelPref(otherPref.preferred_flash_model) === null) prefUpdate.preferred_flash_model = null;
        // Filter starred and fallback model lists
        const starred = (otherPref.starred_models as string[]) ?? [];
        const cleanStarred = starred.filter(m => !removedModelNames.has(m));
        if (cleanStarred.length !== starred.length) prefUpdate.starred_models = cleanStarred.length > 0 ? cleanStarred : null;
        const fallback = (otherPref.fallback_models as string[]) ?? [];
        const cleanFallback = fallback.filter(m => !removedModelNames.has(m));
        if (cleanFallback.length !== fallback.length) prefUpdate.fallback_models = cleanFallback.length > 0 ? cleanFallback : null;

        await updatePreferences.mutateAsync({ other_preference: prefUpdate });
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
      queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() });
    } catch {
      // Silently fail — user can retry
    }
  }, [preferences, updatePreferences, queryClient, metadata]);

  const handleNext = useCallback(() => {
    if (!selected) return;
    navigate('/setup/provider', { state: { method: selected } });
  }, [selected, navigate]);

  const handleSkipToDefaults = useCallback(() => {
    navigate('/setup/defaults');
  }, [navigate]);

  const handleRedeemInvitation = useCallback(async () => {
    if (!invitationCode.trim()) {
      setInvitationError(t('setup.invitationErrorEmpty'));
      return;
    }

    setRedeemingInvitation(true);
    setInvitationError(null);

    try {
      await api.post('/api/auth/invitations/redeem', { code: invitationCode.trim() });
      setLocalRedeemed(true);
      // Bust stale platform tier cache, then refresh user + model access queries
      await getCurrentUser({ refresh_tier: true }).catch(() => {});
      await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
      await queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
      navigate('/setup/defaults');
    } catch (e: unknown) {
      const err = e as {
        response?: { status?: number; data?: { detail?: string | { message?: string; type?: string } } };
        message?: string;
      };
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail;

      if (status === 404) {
        setInvitationError(t('setup.invitationErrorInvalid'));
      } else if (status === 410) {
        setInvitationError(t('setup.invitationErrorExpired'));
      } else if (status === 409) {
        // 409 = this user already redeemed the code — they have access.
        // Treat same as successful redemption: grant local access + navigate.
        setLocalRedeemed(true);
        await getCurrentUser({ refresh_tier: true }).catch(() => {});
        await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
        await queryClient.invalidateQueries({ queryKey: queryKeys.platform.models() });
        navigate('/setup/defaults');
        return;
      } else if (typeof detail === 'string') {
        setInvitationError(detail);
      } else if (detail && typeof detail === 'object' && 'message' in detail) {
        setInvitationError(detail.message || t('setup.invitationErrorGeneric'));
      } else {
        setInvitationError(t('setup.invitationErrorGeneric'));
      }
    } finally {
      setRedeemingInvitation(false);
    }
  }, [invitationCode, queryClient, navigate, t]);

  return (
    <div className="flex flex-col gap-4 sm:gap-6">
      {/* Configured providers banner (visible when looping back) */}
      {hasConfigured && (
        <ConfiguredBanner
          providers={configuredProviders}
          onSkip={handleSkipToDefaults}
          onRemove={handleRemoveProvider}
        />
      )}

      {/* Invitation-redeemed skip banner (when no other providers configured) */}
      {!userLoading && hasPlatformAccess && !hasConfigured && (
        <div
          className="flex items-center justify-between gap-3 rounded-lg p-4"
          style={{
            background: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-default)',
          }}
        >
          <div className="flex items-center gap-2">
            <CheckCircle2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-gain)' }} />
            <span className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
              {t('setup.invitationRedeemed')}
            </span>
          </div>
          <Button variant="outline" size="sm" onClick={handleSkipToDefaults} className="shrink-0 ml-3">
            {t('setup.nextStep')}
          </Button>
        </div>
      )}

      {/* Section heading */}
      <div className="flex flex-col gap-1">
        <h2
          className="font-semibold"
          style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
        >
          {canSkip ? t('setup.addAnotherProviderHeading') : t('setup.methodTitle')}
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {canSkip
            ? t('setup.addMoreProviders')
            : t('setup.methodSubtitle')}
        </p>
      </div>

      {/* Method cards */}
      <div className="flex flex-col gap-3" role="radiogroup" aria-label="Connection method">
        {METHODS.map((method) => {
          const Icon = method.icon;
          const isSelected = selected === method.id;
          return (
            <button
              key={method.id}
              type="button"
              role="radio"
              aria-checked={isSelected}
              onClick={() => setSelected(method.id)}
              className="flex items-start gap-4 rounded-lg p-4 text-left transition-colors cursor-pointer"
              style={{
                border: isSelected
                  ? '2px solid var(--color-accent-primary)'
                  : '1px solid var(--color-border-default)',
                background: isSelected ? 'var(--color-accent-soft)' : undefined,
                padding: isSelected ? 15 : 16,
              }}
            >
              <div
                className="flex items-center justify-center w-10 h-10 rounded-lg shrink-0 mt-0.5"
                style={{
                  background: isSelected
                    ? 'var(--color-accent-primary)'
                    : 'var(--color-bg-surface)',
                  color: isSelected ? '#fff' : 'var(--color-text-secondary)',
                }}
              >
                <Icon className="h-5 w-5" />
              </div>
              <div className="flex flex-col gap-0.5">
                <span
                  className="text-sm font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {t(method.titleKey)}
                </span>
                <span
                  className="text-xs"
                  style={{ color: 'var(--color-text-secondary)' }}
                >
                  {t(method.descKey)}
                </span>
              </div>
            </button>
          );
        })}
      </div>

      {/* Invitation code section — hide only when user already has platform access */}
      {(userLoading || !hasPlatformAccess) && (
        <>
          {!showInvitation ? (
            <button
              type="button"
              onClick={() => setShowInvitation(true)}
              className="inline-flex items-center gap-1.5 text-sm font-medium transition-colors self-start"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              <Ticket className="h-4 w-4" />
              {t('setup.invitationCodeLink')}
            </button>
          ) : (
            <div
              className="flex flex-col gap-3 rounded-lg p-4"
              style={{
                background: 'var(--color-bg-surface)',
                border: '1px solid var(--color-border-default)',
              }}
            >
              <label
                className="text-sm font-medium"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('setup.invitationCodeLabel')}
              </label>
              <div className="flex gap-2">
                <Input
                  value={invitationCode}
                  onChange={(e) => {
                    setInvitationCode(e.target.value);
                    setInvitationError(null);
                  }}
                  placeholder={t('setup.invitationCodePlaceholder')}
                  className="flex-1"
                  autoComplete="off"
                  spellCheck={false}
                />
                <Button
                  variant="default"
                  disabled={redeemingInvitation || !invitationCode.trim()}
                  onClick={handleRedeemInvitation}
                  className="shrink-0"
                >
                  {redeemingInvitation ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
                      {t('setup.redeeming')}
                    </>
                  ) : (
                    t('setup.redeem')
                  )}
                </Button>
              </div>
              {invitationError && (
                <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
                  {invitationError}
                </p>
              )}
              <button
                type="button"
                onClick={() => {
                  setShowInvitation(false);
                  setInvitationError(null);
                }}
                className="text-xs self-start"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('setup.chooseMethodInstead')}
              </button>
            </div>
          )}
        </>
      )}

      {/* Next button */}
      <div className="flex justify-end pt-2">
        <Button
          variant="default"
          disabled={!selected}
          onClick={handleNext}
          className="min-w-[120px]"
        >
          {t('setup.next')}
        </Button>
      </div>
    </div>
  );
}
