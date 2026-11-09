/**
 * The bot a chat thread answers as, drawn in the chat header.
 *
 * The thread row carries only `bot_id` (the server resolves identity, not the
 * display), so this reads the row from the same detail query the composer
 * already keeps warm and then reads that bot. A thread with no bot — one the
 * resolver has never stamped, or the not-yet-created `__default__` thread —
 * draws nothing at all and the header keeps its generic shape.
 */
import { useQuery } from '@tanstack/react-query';
import { BotMark } from '@/components/bots/BotMark';
import { useBot } from '@/hooks/useBots';
import { threadDetailQuery } from '../../utils/threadQueries';

export function ThreadBotChip({ threadId }: { threadId: string | null | undefined }) {
  const liveThreadId = threadId && threadId !== '__default__' ? threadId : null;
  const { data: thread } = useQuery({
    ...threadDetailQuery(liveThreadId ?? ''),
    enabled: !!liveThreadId,
  });
  const botId = thread?.bot_id ?? null;
  const { data: bot } = useBot(botId);

  if (!bot) return null;
  return (
    <span
      data-testid="thread-bot"
      title={bot.name}
      className="inline-flex min-w-0 max-w-44 shrink items-center gap-1.5 rounded-full border px-1.5 py-0.5"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <BotMark bot={bot} size={16} decorative />
      <span className="truncate text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        {bot.name}
      </span>
    </span>
  );
}
