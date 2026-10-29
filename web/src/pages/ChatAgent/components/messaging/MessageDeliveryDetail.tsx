import React from 'react';
import { useTranslation } from 'react-i18next';
import { Paperclip } from 'lucide-react';
import Markdown from '../Markdown';
import { TermCard, TermTag } from '../charts/TermCard';
import { TermPanelHead } from '../charts/TermPanel';
import { DeliveryFileStatusWord, DeliveryStatusWord } from './deliveryStatusUi';
import type { PanelProps } from '../charts/cardReaders';
import {
  deliveredFileCount,
  isDmAddress,
  messagingAppName,
  readMessageDelivery,
  type MessageDelivery,
} from './messageDelivery';

/**
 * A sent message in the tool detail panel, its card a size up: where it went
 * and whether it arrived on the first line, the message in full, and what
 * happened to each file. The app's favicon is on the panel's tab just above,
 * so the heading is the name alone.
 */
export function MessageDeliveryDetail({ delivery, onOpenFile }: {
  delivery: MessageDelivery;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const app = messagingAppName(delivery.platform);
  const label = app ?? t('toolArtifact.messageDelivery.message');
  // A DM address ("telegram", "telegram:@me", "slack:T0123") only repeats the app's name above it.
  const address = delivery.address && !isDmAddress(delivery.address) ? delivery.address : null;
  const total = delivery.files.length;
  // The card's count: "ok/N" once a file did not go.
  const anyMissed = delivery.files.some((f) => f.status === 'failed' || f.status === 'not_sent');
  const count = anyMissed ? `${deliveredFileCount(delivery.files)}/${total}` : `${total}`;

  return (
    // Its parts are the panel's sections, spaced by the panel around them.
    <div className="contents" data-testid="message-delivery-detail">
      <div>
        <TermPanelHead
          title={label}
          sub={
            address || delivery.current ? (
              <>
                {/* Matched against the app's own ids, so it keeps its case. */}
                {address && <span className="term-delivery-address" title={address}>{address}</span>}
                {address && delivery.current && <span aria-hidden="true"> · </span>}
                {delivery.current && <span>{t('toolArtifact.messageDelivery.thisConversation')}</span>}
              </>
            ) : undefined
          }
          aside={
            <>
              {delivery.duplicate && <TermTag>{t('toolArtifact.messageDelivery.sentEarlier')}</TermTag>}
              <DeliveryStatusWord status={delivery.status} />
            </>
          }
        />
        {delivery.outcome && <p className="term-prose term-delivery-outcome">{delivery.outcome}</p>}
      </div>

      {delivery.text.trim() && (
        <TermCard title={t('toolArtifact.messageDelivery.message')}>
          <div className="term-prose min-w-0">
            <Markdown variant="panel" content={delivery.text} className="text-sm" />
          </div>
        </TermCard>
      )}

      {total > 0 && (
        <TermCard
          title={t('toolArtifact.messageDelivery.files')}
          meta={
            <span className="term-delivery-count">
              <Paperclip className="term-delivery-clip" aria-hidden="true" />
              {count}
            </span>
          }
        >
          <ul className="term-delivery-files">
            {delivery.files.map((file, i) => (
              <li key={`${file.path}-${i}`} data-testid="message-delivery-detail-file">
                <div className="term-delivery-file">
                  {onOpenFile ? (
                    <button
                      type="button"
                      className="term-delivery-path"
                      onClick={() => onOpenFile(file.path, delivery.workspaceId ?? undefined)}
                    >
                      {file.path}
                    </button>
                  ) : (
                    <span className="term-delivery-path">{file.path}</span>
                  )}
                  {file.status && (
                    <>
                      <span className="term-kv-leader" aria-hidden="true" />
                      <DeliveryFileStatusWord status={file.status} />
                    </>
                  )}
                </div>
                {file.reason && <div className="term-delivery-reason">{file.reason}</div>}
              </li>
            ))}
          </ul>
        </TermCard>
      )}
    </div>
  );
}

/** The detail panel's entry: the outcome rides the artifact, the message itself is the call's text. */
export function MessageDeliveryPanel({ data, toolArgs, onOpenFile }: PanelProps): React.ReactElement | null {
  const delivery = readMessageDelivery(data, toolArgs);
  return delivery && <MessageDeliveryDetail delivery={delivery} onOpenFile={onOpenFile} />;
}
