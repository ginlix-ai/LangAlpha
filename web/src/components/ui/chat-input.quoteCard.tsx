import { useState, type ReactElement } from 'react';
import { UserRound } from 'lucide-react';
import { useUser } from '@/hooks/useUser';
import { HoverCard, HoverCardContent, HoverCardTrigger } from './hover-card';

const HOVER_DELAY_MS = 150;
/** A paste can run to tens of KB, and a card has no need of it all. */
const CARD_QUOTE_CHARS = 2000;

/**
 * All that a folded quote carries, on hover or focus: where it is from if that
 * is a file, the text, and what the user said about it. A touch has no hover, so
 * a tap toggles it. Reads the user the app already holds, since a signed-out
 * reader of a shared thread can open this too.
 */
export function QuoteCard({
  children, quote, reply, source, side = 'bottom',
}: {
  /** The chip, which must take a ref and the trigger's props. */
  children: ReactElement;
  quote: string;
  reply?: string;
  source?: string;
  side?: 'top' | 'bottom';
}) {
  const { user } = useUser({ enabled: false });
  const [open, setOpen] = useState(false);
  const [avatarFailed, setAvatarFailed] = useState(false);
  const avatar = user?.avatar_url && !avatarFailed ? user.avatar_url : null;
  return (
    <HoverCard open={open} onOpenChange={setOpen} openDelay={HOVER_DELAY_MS} closeDelay={HOVER_DELAY_MS}>
      <HoverCardTrigger
        asChild
        onPointerDown={(e) => { if (e.pointerType === 'touch') setOpen((o) => !o); }}
      >
        {children}
      </HoverCardTrigger>
      <HoverCardContent
        side={side}
        align="start"
        className="w-[380px] max-w-[calc(100vw-16px)] overflow-hidden rounded-xl border-(--color-border-muted) bg-(--color-bg-elevated) p-0 shadow-(--shadow-card)"
      >
        <div className="max-h-60 overflow-y-auto px-3 py-2.5">
          {source && <div className="mb-1 truncate text-xs text-(--color-text-tertiary)">{source}</div>}
          <div className="font-content text-[0.8125rem] leading-normal wrap-anywhere whitespace-pre-wrap text-(--color-text-secondary)">{quote.length > CARD_QUOTE_CHARS ? `${quote.slice(0, CARD_QUOTE_CHARS)}…` : quote}</div>
        </div>
        {reply && (
          <div className="flex items-start gap-2 border-t border-(--color-border-muted) px-3 py-2.5">
            <span className="grid h-5 w-5 flex-none place-items-center overflow-hidden rounded-full bg-(--color-bg-hover) text-(--color-icon-muted)">
              {avatar
                ? <img src={avatar} alt="" className="h-full w-full object-cover" onError={() => setAvatarFailed(true)} />
                : <UserRound className="h-3 w-3" />}
            </span>
            <span className="font-content min-w-0 text-sm leading-5 wrap-anywhere whitespace-pre-wrap text-(--color-text-primary)">{reply}</span>
          </div>
        )}
      </HoverCardContent>
    </HoverCard>
  );
}
