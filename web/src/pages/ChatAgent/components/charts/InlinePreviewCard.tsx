import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { relativeTime } from '@/lib/format';
import { Globe, ExternalLink } from 'lucide-react';
import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { checkPreviewHealth } from '../../utils/api';
import { CARD_BG, CARD_BORDER } from './inlineCardsShared';

// Module-level deduplication: share a single in-flight health check per (workspaceId, port)
const inflightChecks = new Map<string, Promise<{ reachable: boolean; checked_at: number }>>();

function deduplicatedHealthCheck(workspaceId: string, port: number) {
  const key = `${workspaceId}:${port}`;
  const existing = inflightChecks.get(key);
  if (existing) return existing;
  const promise = checkPreviewHealth(workspaceId, port).finally(() => {
    inflightChecks.delete(key);
  });
  inflightChecks.set(key, promise);
  return promise;
}

const TEXT_COLOR = 'var(--color-text-tertiary)';
const ACCENT = 'var(--color-accent-primary)';

interface InlinePreviewCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}


export function InlinePreviewCard({ artifact, onClick }: InlinePreviewCardProps): React.ReactElement | null {
  useTranslation(); // relativeTime is locale-keyed — re-render on locale switch
  const workspaceId = useWorkspaceId();
  const [health, setHealth] = useState<{ reachable: boolean; checkedAt: number; sandboxStopped?: boolean } | null>(null);
  const [, setTick] = useState(0); // force re-render for "X min ago" updates
  const checkingRef = useRef(false);

  const doHealthCheck = useCallback(async () => {
    if (!workspaceId || !artifact?.port || checkingRef.current) return;
    checkingRef.current = true;
    try {
      const result = await deduplicatedHealthCheck(workspaceId, artifact.port as number);
      setHealth({ reachable: result.reachable, checkedAt: result.checked_at });
    } catch (err: unknown) {
      const status = (err as { response?: { status?: number } })?.response?.status;
      if (status === 503) {
        setHealth({ reachable: false, checkedAt: Math.floor(Date.now() / 1000), sandboxStopped: true });
      } else {
        setHealth({ reachable: false, checkedAt: Math.floor(Date.now() / 1000) });
      }
    } finally {
      checkingRef.current = false;
    }
  }, [workspaceId, artifact?.port]);

  // Health check on mount + every 2 minutes
  useEffect(() => {
    doHealthCheck();
    const interval = setInterval(doHealthCheck, 2 * 60 * 1000);
    return () => clearInterval(interval);
  }, [doHealthCheck]);

  // Tick every 30s to update "X min ago" display
  useEffect(() => {
    const interval = setInterval(() => setTick(t => t + 1), 30_000);
    return () => clearInterval(interval);
  }, []);

  if (!artifact) return null;

  const port = artifact.port as number | undefined;
  const title = (artifact.title as string) || (port ? `Port ${port}` : 'Preview');

  // Status indicator
  let dotColor: string;
  let subtitle: string;
  if (!health) {
    dotColor = '#9ca3af';
    subtitle = 'Checking...';
  } else if (health.reachable) {
    dotColor = '#22c55e';
    subtitle = `Live \u00b7 checked ${relativeTime(health.checkedAt * 1000)}`;
  } else if (health.sandboxStopped) {
    dotColor = '#f59e0b';
    subtitle = `Sandbox stopped \u00b7 checked ${relativeTime(health.checkedAt * 1000)}`;
  } else {
    dotColor = '#9ca3af';
    subtitle = `Offline \u00b7 checked ${relativeTime(health.checkedAt * 1000)}`;
  }

  return (
    <div
      style={{
        background: CARD_BG,
        border: `1px solid ${CARD_BORDER}`,
        borderRadius: 8,
        padding: '10px 14px',
        cursor: 'pointer',
        transition: 'border-color 0.15s',
        outline: 'none',
        WebkitTapHighlightColor: 'transparent',
        userSelect: 'none',
        display: 'flex',
        alignItems: 'center',
        gap: 10,
      }}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = ACCENT)}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      <Globe size={16} style={{ color: ACCENT, flexShrink: 0 }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: '0.8125rem' }}>
            {title}
          </span>
          {port && (
            <span
              style={{
                fontSize: '0.625rem',
                fontFamily: 'var(--font-mono)',
                padding: '1px 6px',
                borderRadius: 10,
                backgroundColor: 'var(--color-bg-surface)',
                color: TEXT_COLOR,
              }}
            >
              :{port}
            </span>
          )}
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              backgroundColor: dotColor,
              flexShrink: 0,
              animation: !health ? 'pulse 1.5s ease-in-out infinite' : undefined,
            }}
          />
        </div>
        <div style={{ fontSize: '0.6875rem', color: TEXT_COLOR, marginTop: 1 }}>
          {subtitle}
        </div>
      </div>
      <ExternalLink size={14} style={{ color: TEXT_COLOR, flexShrink: 0 }} />
    </div>
  );
}
