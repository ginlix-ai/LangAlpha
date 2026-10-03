import React from 'react';
import { useTranslation } from 'react-i18next';
import { FileText } from 'lucide-react';
import Markdown from '../Markdown';
import { DeliveryFileStatusLabel, DeliveryStatusPill } from './deliveryStatusUi';
import { messagingAppName, type MessageDelivery } from './messageDelivery';

const LABEL_CLASS = 'text-xs font-medium uppercase tracking-wider mb-2 px-1';
const QUIET = { color: 'var(--color-text-tertiary)' };

/**
 * A sent message in the tool detail panel: where it went, whether it arrived,
 * the message in full, and what happened to each file. The app's favicon is
 * on the panel's tab just above, so the heading is the name alone.
 */
export function MessageDeliveryDetail({ delivery, onOpenFile }: {
  delivery: MessageDelivery;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const app = messagingAppName(delivery.platform);
  const label = app ?? t('toolArtifact.messageDelivery.message');
  // A bare address ("telegram") only repeats the app's name above it.
  const address = delivery.address && delivery.address.toLowerCase() !== delivery.platform ? delivery.address : null;

  return (
    <div className="space-y-5 min-w-0" data-testid="message-delivery-detail">
      <div className="space-y-1 min-w-0 px-1">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {label}
          </span>
          {delivery.current && (
            <>
              <span aria-hidden className="text-xs" style={QUIET}>·</span>
              <span className="text-xs truncate" style={QUIET}>
                {t('toolArtifact.messageDelivery.thisConversation')}
              </span>
            </>
          )}
          <span className="ml-auto shrink-0">
            <DeliveryStatusPill status={delivery.status} />
          </span>
        </div>
        {address && (
          <div className="text-xs font-mono truncate" style={QUIET} title={address}>
            {address}
          </div>
        )}
      </div>

      {delivery.text.trim() && (
        <section>
          <div className={LABEL_CLASS} style={QUIET}>{t('toolArtifact.messageDelivery.message')}</div>
          <div
            className="rounded-lg px-3 py-3 min-w-0"
            style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
          >
            <Markdown variant="panel" content={delivery.text} className="text-sm" />
          </div>
        </section>
      )}

      {delivery.files.length > 0 && (
        <section>
          <div className={LABEL_CLASS} style={QUIET}>{t('toolArtifact.messageDelivery.files')}</div>
          <ul className="space-y-2 px-1">
            {delivery.files.map((file, i) => (
              <li key={`${file.path}-${i}`} className="flex items-start gap-2 min-w-0" data-testid="message-delivery-detail-file">
                <FileText className="h-3.5 w-3.5 shrink-0 mt-0.5" style={QUIET} aria-hidden />
                <div className="min-w-0 flex-1">
                  {onOpenFile ? (
                    <button
                      type="button"
                      onClick={() => onOpenFile(file.path, delivery.workspaceId ?? undefined)}
                      className="text-left text-xs font-mono hover:underline cursor-pointer"
                      style={{ color: 'var(--color-text-primary)', overflowWrap: 'anywhere' }}
                    >
                      {file.path}
                    </button>
                  ) : (
                    <div className="text-xs font-mono" style={{ color: 'var(--color-text-primary)', overflowWrap: 'anywhere' }}>
                      {file.path}
                    </div>
                  )}
                  {file.reason && (
                    <div className="text-xs mt-0.5" style={{ ...QUIET, overflowWrap: 'anywhere' }}>{file.reason}</div>
                  )}
                </div>
                <span className="shrink-0">
                  <DeliveryFileStatusLabel status={file.status} />
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {(delivery.duplicate || delivery.outcome) && (
        <div className="space-y-1 px-1 text-xs" style={QUIET}>
          {delivery.duplicate && <p>{t('toolArtifact.messageDelivery.sentEarlier')}</p>}
          {delivery.outcome && <p style={{ overflowWrap: 'anywhere' }}>{delivery.outcome}</p>}
        </div>
      )}
    </div>
  );
}
