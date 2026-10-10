import React from 'react';
import { useTranslation } from 'react-i18next';
import { Paperclip } from 'lucide-react';
import { FaviconImg, googleFaviconUrl } from '../charts/InlineArtifactCards';
import type { InlineCardProps } from '../charts/inlineCardsShared';
import { DeliveryStatusPill } from './deliveryStatusUi';
import {
  deliveredFileCount,
  messagingAppDomain,
  messagingAppName,
  readMessageDelivery,
} from './messageDelivery';

/**
 * A message the agent sent to a chat app, as a pill: the app, how many files
 * went with it, and whether it arrived. A click opens the detail panel, which
 * carries the whole message and each file's outcome.
 */
export function MessageDeliveryCard({ artifact, toolArgs, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const delivery = readMessageDelivery(artifact, toolArgs);
  if (!delivery) return null;

  const label = messagingAppName(delivery.platform) ?? t('toolArtifact.messageDelivery.message');
  const domain = messagingAppDomain(delivery.platform) ?? '';
  const total = delivery.files.length;
  const delivered = deliveredFileCount(delivery.files);
  // A file that did not go turns the count into "ok/N"; an unknown outcome stays "N".
  const anyMissed = delivery.files.some((f) => f.status === 'failed' || f.status === 'not_sent');
  const count = anyMissed ? `${delivered}/${total}` : `${total}`;
  const statusLabel = t(`toolArtifact.messageDelivery.status.${delivery.status}`);
  const summary = [label, total > 0 ? t('toolArtifact.messageDelivery.fileCount', { count: total }) : null, statusLabel]
    .filter(Boolean)
    .join(' · ');

  return (
    <button
      type="button"
      data-testid="message-delivery-card"
      title={summary}
      aria-label={summary}
      onClick={onClick}
      className="inline-flex items-center min-w-0 max-w-full rounded-full border border-(--color-border-elevated) bg-(--color-bg-card) hover:bg-(--color-bg-surface)"
      style={{
        gap: 7,
        padding: '5px 11px 5px 8px',
        fontSize: 13,
        cursor: 'pointer',
        color: 'var(--color-text-primary)',
      }}
    >
      <FaviconImg src={googleFaviconUrl(domain)} domain={label} />
      <span className="truncate" style={{ fontWeight: 560, whiteSpace: 'nowrap' }}>{label}</span>
      {total > 0 && (
        <span
          data-testid="message-delivery-files"
          className="inline-flex items-center shrink-0"
          style={{
            gap: 4,
            color: 'var(--color-text-secondary)',
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          <span aria-hidden style={{ color: 'var(--color-text-tertiary)' }}>·</span>
          <Paperclip className="shrink-0" style={{ width: 13, height: 13 }} aria-hidden />
          {count}
        </span>
      )}
      <span className="shrink-0">
        <DeliveryStatusPill status={delivery.status} />
      </span>
    </button>
  );
}
