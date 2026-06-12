import { useState, useEffect, useRef, useCallback } from 'react';
import { Link2, Unlink, ExternalLink, Shield, CheckCircle2, Radio } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { useTranslation } from 'react-i18next';
import { queryKeys } from '@/lib/queryKeys';
import { BROKERAGE_BROADCAST_CHANNEL, type BrokerageOAuthMessage } from '@/lib/oauthPopup';
import {
  initiateRobinhoodOAuth,
  getRobinhoodOAuthStatus,
  disconnectRobinhoodOAuth,
  getPreferences,
  updatePreferences,
} from '@/pages/Dashboard/utils/api';
import robinhoodLogo from '@/assets/providers/robinhood.png';
import ibkrLogo from '@/assets/providers/ibkr.png';

interface OAuthStatus {
  connected: boolean;
  account_id?: string | null;
  email?: string | null;
  plan_type?: string | null;
}

// ---------------------------------------------------------------------------
// Broker registry — add new brokers here; UI is fully data-driven
// ---------------------------------------------------------------------------

interface BrokerConfig {
  id: string;
  name: string;
  description: string;
  accentColor: string;
  logoUrl: string;
  comingSoon?: boolean;
}

const BROKERS: BrokerConfig[] = [
  {
    id: 'robinhood',
    name: 'Robinhood',
    description: 'Connected via MCP',
    accentColor: '#00c805',
    logoUrl: robinhoodLogo,
  },
  {
    id: 'ibkr',
    name: 'Interactive Brokers',
    description: 'Connected via API',
    accentColor: '#e30613',
    logoUrl: ibkrLogo,
    comingSoon: true,
  },
  // Future brokers:
  // { id: 'schwab', name: 'Charles Schwab', description: '...', accentColor: '...', logoUrl: schwabLogo },
];

// ---------------------------------------------------------------------------
// Single broker card
// ---------------------------------------------------------------------------

interface BrokerCardProps {
  broker: BrokerConfig;
  status: OAuthStatus;
  isActive: boolean;
  onConnect: () => void;
  onDisconnect: () => void;
  onSetActive: () => void;
  onDeactivate: () => void;
  isConnecting: boolean;
  isDisconnecting: boolean;
}

function BrokerCard({
  broker,
  status,
  isActive,
  onConnect,
  onDisconnect,
  onSetActive,
  onDeactivate,
  isConnecting,
  isDisconnecting,
}: BrokerCardProps) {
  const { t } = useTranslation();
  const { logoUrl, name, description, accentColor, comingSoon } = broker;

  return (
    <div
      className="rounded-lg px-4 py-3"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        border: `1px solid ${isActive ? accentColor + '55' : status.connected ? 'var(--color-success-soft)' : 'var(--color-border-muted)'}`,
        opacity: comingSoon ? 0.6 : 1,
      }}
    >
      <div className="flex items-center justify-between gap-3">
        {/* Logo + info */}
        <div className="flex items-center gap-3 min-w-0">
          <div
            className="h-8 w-8 rounded-md flex items-center justify-center flex-shrink-0 overflow-hidden"
            style={{
              backgroundColor: isActive
                ? accentColor + '22'
                : status.connected
                  ? 'var(--color-success-soft)'
                  : `${accentColor}18`,
            }}
          >
            <img src={logoUrl} alt={name} className="h-5 w-5 object-contain" />
          </div>

          <div className="min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {name}
              </span>

              {comingSoon && (
                <span
                  className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium"
                  style={{ backgroundColor: 'var(--color-bg-sunken, var(--color-bg-card))', color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
                >
                  {t('brokerage.comingSoon', 'Coming Soon')}
                </span>
              )}

              {!comingSoon && isActive && (
                <span
                  className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium"
                  style={{ backgroundColor: accentColor + '18', color: accentColor }}
                >
                  <CheckCircle2 className="h-2.5 w-2.5" />
                  {t('brokerage.active', 'Active')}
                </span>
              )}

              {!comingSoon && !isActive && status.connected && (
                <span
                  className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium"
                  style={{ backgroundColor: 'var(--color-success-soft)', color: 'var(--color-success)' }}
                >
                  {t('brokerage.connected', 'Connected')}
                </span>
              )}
            </div>

            <p className="text-xs mt-0.5 truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {!comingSoon && status.connected
                ? (status.email || t('brokerage.accountLinked', 'Account linked'))
                : description}
            </p>
          </div>
        </div>

        {/* Actions */}
        {!comingSoon && (
          <div className="flex items-center gap-2 flex-shrink-0">
            {status.connected ? (
              <>
                {isActive ? (
                  <button
                    type="button"
                    onClick={onDeactivate}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                    style={{
                      color: 'var(--color-text-tertiary)',
                      backgroundColor: 'transparent',
                      border: '1px solid var(--color-border-muted)',
                    }}
                  >
                    <Radio className="h-3 w-3" />
                    {t('brokerage.deactivate', 'Deactivate')}
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={onSetActive}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                    style={{
                      color: accentColor,
                      backgroundColor: 'transparent',
                      border: `1px solid ${accentColor}55`,
                    }}
                  >
                    <CheckCircle2 className="h-3 w-3" />
                    {t('brokerage.setActive', 'Set as Active')}
                  </button>
                )}

                <button
                  type="button"
                  onClick={onDisconnect}
                  disabled={isDisconnecting}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                  style={{
                    color: 'var(--color-loss)',
                    backgroundColor: 'transparent',
                    border: '1px solid var(--color-loss)',
                  }}
                >
                  <Unlink className="h-3 w-3" />
                  {isDisconnecting
                    ? t('common.loading', 'Loading...')
                    : t('brokerage.disconnect', 'Disconnect')}
                </button>
              </>
            ) : (
              <button
                type="button"
                onClick={onConnect}
                disabled={isConnecting}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors"
                style={{
                  backgroundColor: isConnecting ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                  color: 'var(--color-text-on-accent)',
                }}
              >
                <Link2 className="h-3 w-3" />
                {isConnecting
                  ? t('common.loading', 'Loading...')
                  : t('brokerage.connect', 'Connect')}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Robinhood connect disclaimer dialog
// ---------------------------------------------------------------------------

interface RobinhoodDisclaimerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onProceed: () => void;
}

function RobinhoodDisclaimer({ open, onOpenChange, onProceed }: RobinhoodDisclaimerProps) {
  const { t } = useTranslation();

  const steps = [
    {
      title: t('brokerage.robinhoodStep1Title', 'Authorize on Robinhood'),
      desc: t('brokerage.robinhoodStep1Desc', 'A popup will open where you sign in to your Robinhood account and grant access.'),
    },
    {
      title: t('brokerage.robinhoodStep2Title', 'Approve permissions'),
      desc: t('brokerage.robinhoodStep2Desc', 'Review and approve the requested permissions.'),
    },
    {
      title: t('brokerage.robinhoodStep3Title', 'Return here automatically'),
      desc: t('brokerage.robinhoodStep3Desc', 'Once authorized, the popup closes and your account is linked instantly.'),
    },
  ];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="sm:max-w-md border"
        style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
      >
        <DialogHeader>
          <DialogTitle className="title-font flex items-center gap-2" style={{ color: 'var(--color-text-primary)' }}>
            <Link2 className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} />
            {t('brokerage.robinhoodConnectTitle', 'Connect Robinhood')}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-3">
            <p className="text-xs font-medium uppercase tracking-wide" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('brokerage.howItWorks', 'How it works')}
            </p>
            {steps.map((step, i) => (
              <div key={i} className="flex gap-3 items-start">
                <div
                  className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold"
                  style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                >
                  {i + 1}
                </div>
                <div>
                  <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{step.title}</p>
                  <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{step.desc}</p>
                </div>
              </div>
            ))}
          </div>

          <div
            className="rounded-lg p-3"
            style={{ backgroundColor: 'var(--color-bg-sunken, var(--color-bg-card))', border: '1px solid var(--color-border-muted)' }}
          >
            <div className="flex gap-2 items-start">
              <Shield className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
              <div>
                <p className="text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                  {t('brokerage.securityTitle', 'Security & Privacy')}
                </p>
                <p className="text-[11px] leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('brokerage.securityDesc', 'Your access tokens are encrypted at rest and used only to make API calls on your behalf.')}
                </p>
                <p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('brokerage.tradingDisclaimer', 'Trade execution requires explicit confirmation. You can disconnect at any time.')}
                </p>
              </div>
            </div>
          </div>
        </div>

        <DialogFooter className="gap-2 pt-2">
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="px-3 py-1.5 rounded text-sm border"
            style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
            onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
            onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
          >
            {t('common.cancel', 'Cancel')}
          </button>
          <button
            type="button"
            onClick={onProceed}
            className="px-4 py-1.5 rounded text-sm font-medium hover:opacity-90 flex items-center gap-1.5"
            style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
          >
            <ExternalLink className="h-3.5 w-3.5" />
            {t('brokerage.openRobinhood', 'Open Robinhood')}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ---------------------------------------------------------------------------
// BrokerageTab — top-level component used by Settings
// ---------------------------------------------------------------------------

export default function BrokerageTab() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  const [robinhoodStatus, setRobinhoodStatus] = useState<OAuthStatus>({ connected: false });
  const [activeBrokerage, setActiveBrokerage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isConnecting, setIsConnecting] = useState(false);
  const [isDisconnecting, setIsDisconnecting] = useState(false);
  const [showDisclaimer, setShowDisclaimer] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const popupRef = useRef<Window | null>(null);

  useEffect(() => {
    Promise.all([
      getRobinhoodOAuthStatus(),
      getPreferences(),
    ]).then(([rhStatus, prefs]) => {
      setRobinhoodStatus(rhStatus);
      const otherPref = (prefs as Record<string, unknown> | null)?.other_preference as Record<string, unknown> | undefined;
      setActiveBrokerage((otherPref?.active_brokerage as string) ?? null);
    }).finally(() => setIsLoading(false));
  }, []);

  const saveActiveBrokerage = useCallback(async (brokerId: string | null) => {
    await updatePreferences({ other_preference: { active_brokerage: brokerId } });
    setActiveBrokerage(brokerId);
  }, []);

  const onRobinhoodConnected = useCallback(async () => {
    const status = await getRobinhoodOAuthStatus();
    setRobinhoodStatus(status);
    queryClient.invalidateQueries({ queryKey: queryKeys.oauth.robinhood() });
    if (status.connected) {
      const prefs = await getPreferences();
      const otherPref = (prefs as Record<string, unknown> | null)?.other_preference as Record<string, unknown> | undefined;
      if (!otherPref?.active_brokerage) await saveActiveBrokerage('robinhood');
    }
  }, [queryClient, saveActiveBrokerage]);

  useEffect(() => {
    let channel: BroadcastChannel;
    try {
      channel = new BroadcastChannel(BROKERAGE_BROADCAST_CHANNEL);
      channel.onmessage = (event: MessageEvent<BrokerageOAuthMessage>) => {
        const msg = event.data;
        if (msg.type !== 'brokerage-oauth-complete' || msg.provider !== 'robinhood') return;
        popupRef.current = null;
        setIsConnecting(false);
        if (msg.status === 'success') {
          onRobinhoodConnected();
        } else {
          setError(msg.error || t('brokerage.connectFailed', 'Authorization failed. Please try again.'));
        }
      };
    } catch {
      // BroadcastChannel unsupported — polling fallback handles cleanup
    }
    return () => { try { channel?.close(); } catch {} };
  }, [onRobinhoodConnected, t]);

  useEffect(() => {
    if (!isConnecting) return;
    const timer = setInterval(() => {
      if (popupRef.current?.closed) {
        popupRef.current = null;
        setIsConnecting(false);
        clearInterval(timer);
        onRobinhoodConnected();
      }
    }, 500);
    return () => clearInterval(timer);
  }, [isConnecting, onRobinhoodConnected]);

  const handleConnect = useCallback(async () => {
    setShowDisclaimer(false);
    setIsConnecting(true);
    setError(null);
    try {
      const result = await initiateRobinhoodOAuth();
      const authorizeUrl = result.authorize_url as string;
      const popup = window.open(
        authorizeUrl,
        'robinhood_oauth',
        'width=520,height=680,scrollbars=yes,resizable=yes',
      );
      if (!popup) {
        setIsConnecting(false);
        setError(t('brokerage.popupBlocked', 'Popup was blocked. Please allow popups for this site and try again.'));
        return;
      }
      popupRef.current = popup;
    } catch {
      setIsConnecting(false);
      setError(t('brokerage.connectFailed', 'Failed to initiate Robinhood connection. Please try again.'));
    }
  }, [t]);

  const handleDisconnect = useCallback(async () => {
    setIsDisconnecting(true);
    setError(null);
    try {
      await disconnectRobinhoodOAuth();
      setRobinhoodStatus({ connected: false });
      queryClient.invalidateQueries({ queryKey: queryKeys.oauth.robinhood() });
      // Clear active if this broker was active
      if (activeBrokerage === 'robinhood') {
        await saveActiveBrokerage(null);
      }
    } catch {
      setError(t('brokerage.disconnectFailed', 'Failed to disconnect. Please try again.'));
    } finally {
      setIsDisconnecting(false);
    }
  }, [queryClient, t, activeBrokerage, saveActiveBrokerage]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <p className="text-sm" style={{ color: 'var(--color-text-primary)', opacity: 0.7 }}>
          {t('common.loading')}
        </p>
      </div>
    );
  }

  const statusMap: Record<string, OAuthStatus> = {
    robinhood: robinhoodStatus,
  };

  const connectHandlers: Record<string, () => void> = {
    robinhood: () => setShowDisclaimer(true),
  };

  const disconnectHandlers: Record<string, () => void> = {
    robinhood: handleDisconnect,
  };

  return (
    <div className="space-y-4">
      <div>
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('brokerage.desc', 'Connect your brokerage accounts to enable portfolio tracking and AI-assisted trading.')}
        </p>
        <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)', opacity: 0.75 }}>
          {t('brokerage.activeHint', 'Only one brokerage can be active at a time.')}
        </p>
      </div>

      <div className="space-y-2">
        {BROKERS.map((broker) => (
          <BrokerCard
            key={broker.id}
            broker={broker}
            status={statusMap[broker.id] ?? { connected: false }}
            isActive={activeBrokerage === broker.id}
            onConnect={connectHandlers[broker.id] ?? (() => {})}
            onDisconnect={disconnectHandlers[broker.id] ?? (() => {})}
            onSetActive={() => saveActiveBrokerage(broker.id)}
            onDeactivate={() => saveActiveBrokerage(null)}
            isConnecting={broker.id === 'robinhood' ? isConnecting : false}
            isDisconnecting={broker.id === 'robinhood' ? isDisconnecting : false}
          />
        ))}
      </div>

      {error && (
        <div
          className="p-3 rounded-md"
          style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}
        >
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{error}</p>
        </div>
      )}

      <RobinhoodDisclaimer
        open={showDisclaimer}
        onOpenChange={setShowDisclaimer}
        onProceed={handleConnect}
      />
    </div>
  );
}
