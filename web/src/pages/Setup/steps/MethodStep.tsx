import { useState, useCallback, useId } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Ticket, Key, Monitor, ChevronDown, ChevronRight, ChevronUp, CheckCircle2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { WizardStepHeader } from '@/components/ui/wizard';
import { SetupFooter } from '../SetupFooter';
import { Loader } from '@/components/ui/loader';
import { Input } from '@/components/ui/input';
import { redeemInvitation } from '@/api/invitations';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { SERVER_KEY, useConfiguredProviders, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useUser } from '@/hooks/useUser';
import { getCurrentUser } from '@/pages/Dashboard/utils/api';
import { RemoveAccessDialog } from '@/components/modelAccess/RemoveAccessDialog';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { llmProviderArt } from '@/lib/brandArt';
import { SUBSCRIPTIONS } from '@/api/oauth';
import type { AccessType } from '@/components/model/types';
import { isPlatformMode } from '@/config/hostMode';

// ---------------------------------------------------------------------------
// Ways to connect
// ---------------------------------------------------------------------------

type Method = Exclude<AccessType, 'coding_plan'>;

/** A provider's mark, by its own key or the brand it belongs to. */
const markOf = (p: { provider: string; brand_key?: string | null }) =>
  llmProviderArt(p.provider) ?? (p.brand_key ? llmProviderArt(p.brand_key) : undefined);

/**
 * A coding plan is a subscription bought with a key, listed with the
 * subscriptions, so it is not a way of its own here.
 */
const METHODS: Array<{ id: Method; titleKey: string; descKey: string }> = [
  { id: 'oauth', titleKey: 'setup.methodOAuth', descKey: 'setup.methodOAuthDesc' },
  { id: 'api_key', titleKey: 'setup.methodApiKey', descKey: 'setup.methodApiKeyDesc' },
  ...(isPlatformMode ? [] : [{ id: 'local' as const, titleKey: 'setup.methodLocal', descKey: 'setup.methodLocalDesc' }]),
];

function MethodMark({ method }: { method: Method }) {
  if (method === 'oauth') {
    return (
      <span className="flex shrink-0 gap-1">
        {SUBSCRIPTIONS.map((s) => <BrandMark key={s.provider} name={s.provider} art={llmProviderArt(s.provider)} size="xs" />)}
      </span>
    );
  }
  const Icon = method === 'local' ? Monitor : Key;
  return <Icon aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />;
}

// ---------------------------------------------------------------------------
// What is already connected
// ---------------------------------------------------------------------------

/**
 * One line however many providers are connected, so the choices below never
 * move down the page; the list opens under it to remove one.
 */
function ConnectedSummary({
  providers,
  onRemove,
}: {
  providers: ConfiguredProvider[];
  onRemove: (provider: ConfiguredProvider) => void;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const listId = useId();

  if (providers.length === 0) return null;

  // One mark per brand: a ChatGPT sign-in and an OpenAI key are both OpenAI's.
  const marks = [...new Map(providers.map((p) => [markOf(p)?.src ?? p.provider, p])).values()].slice(0, 6);

  return (
    <div
      className="rounded-lg overflow-hidden"
      style={{
        background: 'var(--color-bg-card)',
        border: '1px solid var(--color-border-muted)',
      }}
    >
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        aria-expanded={expanded}
        aria-controls={listId}
        className="flex w-full items-center gap-3 px-4 py-3 text-left"
      >
        <span className="flex shrink-0 gap-1">
          {marks.map((p) => (
            <BrandMark key={p.provider} name={p.display_name} art={markOf(p)} size="xs" />
          ))}
        </span>
        <span className="flex-1 text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('setup.providersConnected', { count: providers.length })}
        </span>
        <span className="inline-flex items-center gap-1 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {t('modelAccess.manage')}
          {expanded
            ? <ChevronUp aria-hidden="true" className="h-3.5 w-3.5" />
            : <ChevronDown aria-hidden="true" className="h-3.5 w-3.5" />}
        </span>
      </button>

      {expanded && (
        <ul id={listId} style={{ borderTop: '1px solid var(--color-border-muted)' }}>
          {providers.map((p) => (
            <li
              key={p.provider}
              className="flex items-center gap-3 px-4 py-2.5"
              style={{ borderBottom: '1px solid var(--color-border-muted)' }}
            >
              <BrandMark name={p.display_name} art={markOf(p)} size="sm" className="rounded-md" />
              <span className="min-w-0 flex-1 truncate text-sm" style={{ color: 'var(--color-text-primary)' }}>
                {p.display_name}
              </span>
              <span
                className="inline-flex items-center px-1.5 py-0.5 rounded text-[0.625rem] font-medium shrink-0"
                style={{
                  background: 'var(--color-bg-tag)',
                  color: 'var(--color-text-tertiary)',
                }}
              >
                {p.access_type === 'oauth' ? t('setup.oauthBadge')
                  : p.access_type === 'local' ? t('setup.localBadge')
                  : p.access_type === 'coding_plan' ? t('setup.codingPlanBadge')
                  : t('setup.apiKeyBadge')}
              </span>
              {/* The deployment's own key or server: nothing of the user's to remove. */}
              {p.masked_key !== SERVER_KEY && (
                <button
                  type="button"
                  onClick={() => onRemove(p)}
                  className="text-xs shrink-0 px-2 py-1 rounded transition-colors hover:opacity-80"
                  style={{ color: 'var(--color-loss)' }}
                >
                  {p.access_type === 'oauth' ? t('setup.disconnectProvider') : t('setup.removeProvider')}
                </button>
              )}
            </li>
          ))}
        </ul>
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
  const [removing, setRemoving] = useState<ConfiguredProvider | null>(null);

  const headingId = useId();

  // Invitation code state
  const [showInvitation, setShowInvitation] = useState(false);
  const [invitationCode, setInvitationCode] = useState('');
  const [invitationError, setInvitationError] = useState<string | null>(null);
  const [redeemingInvitation, setRedeemingInvitation] = useState(false);

  const choose = useCallback((method: Method) => {
    navigate('/setup/provider', { state: { list: method } });
  }, [navigate]);

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
      await redeemInvitation(invitationCode.trim());
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
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={canSkip ? t('setup.addAnotherProviderHeading') : t('setup.methodTitle')}
        helper={canSkip ? t('setup.addMoreProviders') : t('setup.methodSubtitle')}
      />
      <div className="flex flex-col gap-4 sm:gap-6">
      {/* Configured providers banner (visible when looping back) */}
      {hasConfigured && (
        <ConnectedSummary
          providers={configuredProviders}
          onRemove={setRemoving}
        />
      )}
      {removing && (
        <RemoveAccessDialog provider={removing} name={removing.display_name} onClose={() => setRemoving(null)} />
      )}

      {/* Invitation-redeemed note (when no other providers configured); the
          way on from it is the footer's skip */}
      {!userLoading && hasPlatformAccess && !hasConfigured && (
        <div
          className="flex items-center gap-2 rounded-lg p-4"
          style={{
            background: 'var(--color-bg-card)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          <CheckCircle2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-success)' }} />
          <span className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.invitationRedeemed')}
          </span>
        </div>
      )}

      {/* Each way goes straight on, as the rows in Settings do. */}
      <ul aria-labelledby={headingId} className="flex flex-col gap-2">
        {METHODS.map((method) => (
          <li key={method.id}>
            <button
              type="button"
              onClick={() => choose(method.id)}
              className="flex w-full items-center gap-3 rounded-lg px-4 py-3.5 text-left transition-colors hover:bg-(--color-bg-hover)"
              style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
            >
              <span className="flex w-12 shrink-0 justify-center"><MethodMark method={method.id} /></span>
              <span className="flex min-w-0 flex-1 flex-col gap-0.5">
                <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t(method.titleKey)}</span>
                <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t(method.descKey)}</span>
              </span>
              <ChevronRight aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            </button>
          </li>
        ))}
      </ul>

      {/* Invitation code section — hide only when user already has platform access */}
      {(userLoading || !hasPlatformAccess) && (
        <>
          {!showInvitation ? (
            <button
              type="button"
              onClick={() => setShowInvitation(true)}
              className="inline-flex items-center gap-1.5 text-sm font-medium transition-colors self-start text-(--color-text-secondary) hover:text-(--color-text-primary)"
            >
              <Ticket className="h-4 w-4" />
              {t('setup.invitationCodeLink')}
            </button>
          ) : (
            <div
              className="flex flex-col gap-3 rounded-lg p-4"
              style={{
                background: 'var(--color-bg-card)',
                border: '1px solid var(--color-border-muted)',
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
                      <span aria-hidden="true" className="mr-1.5 shrink-0">
                        <Loader size={16} className="text-current" />
                      </span>
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

      </div>

      <SetupFooter
        skip={canSkip ? { label: t('setup.skipToModels'), onClick: handleSkipToDefaults } : null}
        primary={null}
      />
    </div>
  );
}
