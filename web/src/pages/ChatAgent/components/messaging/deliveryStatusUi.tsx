/**
 * The delivery vocabulary: one table for a send, one for each file on it, one
 * renderer each. The inline card and the detail panel both read this file, so
 * a send cannot read "Sent" on one and wear another verdict on the other.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import { TermStatus, type TermTone } from '../charts/TermCard';
import type { DeliveryFileStatus, DeliveryStatus } from './messageDelivery';

interface DeliveryStatusUi {
  labelKey: string;
  tone: TermTone;
}

/**
 * Only a clean send is quiet. Anything short of it is the reader's to act on,
 * so it carries colour as well as words: a partial or unknown send may have
 * left the user without part of it, and a failed one left them without all of
 * it. Failure is the danger token, not `--color-loss`, which is market P&L.
 */
const DELIVERY_STATUS_UI: Record<DeliveryStatus, DeliveryStatusUi> = {
  sent: { labelKey: 'toolArtifact.messageDelivery.status.sent', tone: 'success' },
  partial: { labelKey: 'toolArtifact.messageDelivery.status.partial', tone: 'warning' },
  failed: { labelKey: 'toolArtifact.messageDelivery.status.failed', tone: 'danger' },
  unknown: { labelKey: 'toolArtifact.messageDelivery.status.unknown', tone: 'warning' },
};

/** The verdict as a word in brackets in its colour, `[PARTLY SENT]`, for the
 *  pill a thread draws in the terminal type its data cards use. */
export function DeliveryStatusWord({ status }: { status: DeliveryStatus }): React.ReactElement {
  const { t } = useTranslation();
  const ui = DELIVERY_STATUS_UI[status];
  return (
    <TermStatus tone={ui.tone} testid={`delivery-status-${status}`}>
      {t(ui.labelKey)}
    </TermStatus>
  );
}

/** A file that did not go is flagged; one that went as a link says so, since
 *  the recipient gets a download rather than the file. */
const FILE_STATUS_KEY: Record<Exclude<DeliveryFileStatus, null>, string> = {
  sent: 'toolArtifact.messageDelivery.file.sent',
  linked: 'toolArtifact.messageDelivery.file.linked',
  failed: 'toolArtifact.messageDelivery.file.failed',
  not_sent: 'toolArtifact.messageDelivery.file.notSent',
};

/** One file's outcome as a bracket word, `[FAILED]`, quieter than the send's
 *  own verdict; only a file that failed takes a colour. Nothing for a file
 *  whose fate is unknown. */
export function DeliveryFileStatusWord({ status }: { status: DeliveryFileStatus }): React.ReactElement | null {
  const { t } = useTranslation();
  if (!status) return null;
  return (
    <span className="term-tag" data-tone={status === 'failed' ? 'bad' : undefined} data-testid={`delivery-file-${status}`}>
      {t(FILE_STATUS_KEY[status])}
    </span>
  );
}
