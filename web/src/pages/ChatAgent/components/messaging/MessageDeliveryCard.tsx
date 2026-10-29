import React from 'react';
import { useTranslation } from 'react-i18next';
import { Paperclip } from 'lucide-react';
import type { InlineCardProps } from '../charts/cardReaders';
import { FaviconImg } from '../charts/FaviconImg';
import '../charts/termCard.css';
import '../charts/termCardStyles.css';
import { DeliveryStatusWord } from './deliveryStatusUi';
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
 *
 * A chip keeps a chip's shape; its type and its verdict, `[SENT]`, are the
 * terminal ones the data cards around it use.
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
      // Unfilled, held by the same edge as the cards around it, which is also
      // what answers a hover.
      className="term-pill inline-flex items-center min-w-0 max-w-full rounded-full border border-(--color-term-edge) hover:border-(--color-term-edge-hover) transition-colors"
    >
      <FaviconImg domain={domain} name={label} />
      <span className="truncate font-medium">{label}</span>
      {total > 0 && (
        <span data-testid="message-delivery-files" className="term-pill-files inline-flex items-center shrink-0">
          <span aria-hidden className="term-t3">·</span>
          <Paperclip className="shrink-0" aria-hidden />
          {count}
        </span>
      )}
      <span className="shrink-0">
        <DeliveryStatusWord status={delivery.status} />
      </span>
    </button>
  );
}
