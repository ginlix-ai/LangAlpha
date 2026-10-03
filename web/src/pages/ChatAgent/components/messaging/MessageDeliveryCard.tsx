import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, Link2, Paperclip, Send } from 'lucide-react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { clip, plainText } from '../minimapEntries';
import {
  SIZES_DESKTOP,
  SIZES_MOBILE,
  TEXT_COLOR,
  cardStyle,
  mobileCardStyle,
  type InlineCardProps,
} from '../charts/inlineCardsShared';
import { DeliveryStatusPill } from './deliveryStatusUi';
import { fileName, messagingAppName, readMessageDelivery, type DeliveryFile } from './messageDelivery';

const PREVIEW_MAX = 400;
const MAX_CHIPS = 3;

/**
 * A message the agent sent to a chat app: where it went, whether it arrived,
 * and the head of what it said. The text is the call's own argument, since the
 * delivery result does not repeat it. A click opens the detail panel, which
 * carries the whole message and each file's outcome.
 */
export function MessageDeliveryCard({ artifact, toolArgs, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const delivery = readMessageDelivery(artifact, toolArgs);
  if (!delivery) return null;

  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const app = messagingAppName(delivery.platform);
  const preview = delivery.text ? clip(plainText(delivery.text.slice(0, PREVIEW_MAX * 4)), PREVIEW_MAX) : '';
  // A file that did not go is never the one hidden behind "+N".
  const files = [...delivery.files].sort((a, b) => Number(b.status === 'failed') - Number(a.status === 'failed'));
  const chips = files.slice(0, MAX_CHIPS);
  const more = files.length - chips.length;

  return (
    <div
      data-testid="message-delivery-card"
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onClick={onClick}
      onKeyDown={onClick ? (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onClick();
        }
      } : undefined}
      className="border border-(--color-border-muted) hover:border-(--color-border-elevated) min-w-0"
      // The shell's `outline: none` would hide the keyboard ring, and its
      // inline border would beat the hover class.
      style={{
        ...(isMobile ? mobileCardStyle : cardStyle),
        outline: undefined,
        border: undefined,
        cursor: onClick ? 'pointer' : 'default',
      }}
    >
      <div className="flex items-center min-w-0" style={{ gap: sz.gap }}>
        <Send className="h-3.5 w-3.5 shrink-0" style={{ color: TEXT_COLOR }} aria-hidden />
        <span
          className="truncate"
          style={{ fontWeight: 600, fontSize: sz.headerFs, color: 'var(--color-text-primary)' }}
        >
          {app ?? t('toolArtifact.messageDelivery.message')}
        </span>
        {delivery.current && (
          <>
            <span aria-hidden style={{ fontSize: sz.labelFs, color: TEXT_COLOR }}>·</span>
            <span className="truncate" style={{ fontSize: sz.labelFs, color: TEXT_COLOR }}>
              {t('toolArtifact.messageDelivery.thisConversation')}
            </span>
          </>
        )}
        <span className="ml-auto shrink-0">
          <DeliveryStatusPill status={delivery.status} />
        </span>
      </div>

      {preview && (
        <p
          data-testid="message-delivery-preview"
          style={{
            margin: `${sz.sectionMb}px 0 0`,
            fontSize: sz.rowFs,
            lineHeight: 1.5,
            color: 'var(--color-text-secondary)',
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            overflowWrap: 'anywhere',
          }}
        >
          {preview}
        </p>
      )}

      {chips.length > 0 && (
        <div className="flex flex-wrap items-center min-w-0" style={{ gap: 4, marginTop: sz.sectionMb }}>
          {chips.map((file, i) => <FileChip key={`${file.path}-${i}`} file={file} fontSize={sz.badgeFs} />)}
          {more > 0 && <span style={{ fontSize: sz.badgeFs, color: TEXT_COLOR }}>+{more}</span>}
        </div>
      )}
    </div>
  );
}

function FileChip({ file, fontSize }: { file: DeliveryFile; fontSize: string }): React.ReactElement {
  const failed = file.status === 'failed';
  const Icon = failed ? AlertCircle : file.status === 'linked' ? Link2 : Paperclip;
  return (
    <span
      data-testid={failed ? 'message-delivery-file-failed' : 'message-delivery-file'}
      title={file.reason ? `${file.path} · ${file.reason}` : file.path}
      className="inline-flex items-center gap-1 min-w-0 max-w-full rounded-full px-1.5 py-px"
      style={{
        fontSize,
        color: failed ? 'var(--color-icon-danger)' : 'var(--color-text-tertiary)',
        backgroundColor: failed ? 'var(--color-danger-soft)' : 'var(--color-bg-tag)',
      }}
    >
      <Icon className="h-3 w-3 shrink-0" aria-hidden />
      <span className="truncate">{fileName(file.path)}</span>
    </span>
  );
}
