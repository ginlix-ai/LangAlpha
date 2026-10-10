/**
 * The delivery vocabulary: one table for a send, one for each file on it, one
 * renderer each. The inline card and the detail panel both read this file, so
 * a send cannot read "Sent" on one and wear another verdict on the other.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  AlertCircle, Check, CheckCircle2, HelpCircle, Link2, MinusCircle, XCircle,
  type LucideIcon,
} from 'lucide-react';
import { StatusPill } from '@/components/mcp/McpPrimitives';
import type { DeliveryFileStatus, DeliveryStatus } from './messageDelivery';

interface DeliveryStatusUi {
  labelKey: string;
  color: string;
  bg: string;
  icon: LucideIcon;
}

/**
 * Only a clean send is quiet. Anything short of it is the reader's to act on,
 * so it carries colour as well as words: a partial or unknown send may have
 * left the user without part of it, and a failed one left them without all of
 * it. Failure is the danger token, not `--color-loss`, which is market P&L.
 */
const DELIVERY_STATUS_UI: Record<DeliveryStatus, DeliveryStatusUi> = {
  sent: {
    labelKey: 'toolArtifact.messageDelivery.status.sent',
    color: 'var(--color-success)',
    bg: 'var(--color-success-soft)',
    icon: CheckCircle2,
  },
  partial: {
    labelKey: 'toolArtifact.messageDelivery.status.partial',
    color: 'var(--color-warning)',
    bg: 'var(--color-warning-soft)',
    icon: AlertCircle,
  },
  failed: {
    labelKey: 'toolArtifact.messageDelivery.status.failed',
    color: 'var(--color-icon-danger)',
    bg: 'var(--color-danger-soft)',
    icon: XCircle,
  },
  unknown: {
    labelKey: 'toolArtifact.messageDelivery.status.unknown',
    color: 'var(--color-warning)',
    bg: 'var(--color-warning-soft)',
    icon: HelpCircle,
  },
};

export function DeliveryStatusPill({ status }: { status: DeliveryStatus }): React.ReactElement {
  const { t } = useTranslation();
  const ui = DELIVERY_STATUS_UI[status];
  return (
    <StatusPill
      icon={ui.icon}
      label={t(ui.labelKey)}
      color={ui.color}
      bg={ui.bg}
      testid={`delivery-status-${status}`}
    />
  );
}

interface FileStatusUi {
  labelKey: string;
  color: string;
  icon: LucideIcon;
}

/** A file that did not go is flagged; one that went as a link says so, since
 *  the recipient gets a download rather than the file. */
const FILE_STATUS_UI: Record<Exclude<DeliveryFileStatus, null>, FileStatusUi> = {
  sent: {
    labelKey: 'toolArtifact.messageDelivery.file.sent',
    color: 'var(--color-text-tertiary)',
    icon: Check,
  },
  linked: {
    labelKey: 'toolArtifact.messageDelivery.file.linked',
    color: 'var(--color-text-tertiary)',
    icon: Link2,
  },
  failed: {
    labelKey: 'toolArtifact.messageDelivery.file.failed',
    color: 'var(--color-icon-danger)',
    icon: AlertCircle,
  },
  not_sent: {
    labelKey: 'toolArtifact.messageDelivery.file.notSent',
    color: 'var(--color-text-tertiary)',
    icon: MinusCircle,
  },
};

/** One file's outcome as glyph + word; nothing for a file whose fate is unknown. */
export function DeliveryFileStatusLabel({ status }: { status: DeliveryFileStatus }): React.ReactElement | null {
  const { t } = useTranslation();
  if (!status) return null;
  const ui = FILE_STATUS_UI[status];
  const Icon = ui.icon;
  return (
    <span
      className="inline-flex items-center gap-1 text-xs whitespace-nowrap"
      style={{ color: ui.color }}
      data-testid={`delivery-file-${status}`}
    >
      <Icon className="h-3 w-3 shrink-0" />
      {t(ui.labelKey)}
    </span>
  );
}
