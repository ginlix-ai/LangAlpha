/**
 * A bot-to-bot message delivered into this thread, drawn the way ds-bot draws
 * a message from another bot: the sender's mark and a "Request from Scout"
 * line, with what it said under it in a quiet bubble.
 *
 * The turn's own message is the framed text the sender wrote for the model
 * (`[Request from Scout]\n…`), so the frame is stripped before drawing (see
 * `session/provenance.ts`) and the row never offers the user's own actions:
 * it is not the user's message, and there is nothing here to edit, retry or
 * steer.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import { BotMark } from '@/components/bots/BotMark';
import { useBot } from '@/hooks/useBots';
import { cn } from '@/lib/utils';
import type { MessageProvenance } from '@/types/chat';
import { OverflowCollapse } from './OverflowCollapse';

export type BotMessageProvenance = Extract<MessageProvenance, { kind: 'bot_message' }>;

/** The line's head, one per kind the delivery frames name. */
const PREFIX_KEY: Record<BotMessageProvenance['messageKind'], string> = {
  message: 'chat.botMessage.prefix.message',
  request: 'chat.botMessage.prefix.request',
  reply: 'chat.botMessage.prefix.reply',
  report: 'chat.botMessage.prefix.report',
  question: 'chat.botMessage.prefix.question',
  notice: 'chat.botMessage.prefix.notice',
};

/** About six lines of chat text; the sender's words are not a report to read
 *  in full every time, and the rest is one press away. */
const BODY_COLLAPSE_PX = 168;

export interface BotMessageRowProps {
  id: string;
  provenance: BotMessageProvenance;
  /** The sender's words, frame already stripped by the session layer. */
  content: string;
  /** Set when the row above is another bot message from the same arrival. */
  together?: boolean;
}

export function BotMessageRow({ id, provenance, content, together }: BotMessageRowProps): React.ReactElement {
  const { t } = useTranslation();
  const { data: bot } = useBot(provenance.fromBotId);
  const sender = provenance.fromName;
  const who = bot
    ? [t(bot.role === 'chief_of_staff' ? 'agents.chiefOfStaff' : 'agents.analyst'), bot.model]
        .filter(Boolean)
        .join(' · ')
    : undefined;

  return (
    <div
      data-message-id={id}
      data-message-role="user"
      data-bot-message
      tabIndex={-1}
      className={cn('group/bot-message min-w-0 outline-hidden', together && 'mt-0')}
    >
      <div className="flex min-w-0 items-start gap-2.5">
        <span className="mt-0.5 shrink-0">
          {/* The resolved bot draws its own mark; a sender with no row (or a
              legacy one) still gets a deterministic mark from its name. */}
          <BotMark bot={bot ?? { name: sender }} size={22} decorative />
        </span>
        <div className="flex min-w-0 max-w-[min(100%,42rem)] flex-col items-start gap-1">
          <div className="flex min-w-0 max-w-full items-center gap-1.5 text-xs leading-4">
            <span className="shrink-0" style={{ color: 'var(--color-text-tertiary)' }}>
              {t(PREFIX_KEY[provenance.messageKind])}
            </span>
            <span
              className="min-w-0 truncate font-medium"
              style={{ color: 'var(--color-text-secondary)' }}
              title={who}
            >
              {sender}
            </span>
          </div>
          {content && (
            <div
              className="max-w-full overflow-hidden rounded-xl border px-3 py-2 text-[0.8125rem] leading-relaxed wrap-break-word"
              style={{
                backgroundColor: 'var(--color-bg-elevated)',
                borderColor: 'var(--color-border-muted)',
                color: 'var(--color-text-secondary)',
              }}
            >
              <OverflowCollapse enabled maxHeight={BODY_COLLAPSE_PX}>
                <p className="whitespace-pre-wrap">{content}</p>
              </OverflowCollapse>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
