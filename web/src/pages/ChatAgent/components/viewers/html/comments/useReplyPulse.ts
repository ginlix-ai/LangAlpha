import { useEffect, useState } from 'react';
import { DURATION } from '@/lib/motion';
import type { ArtifactComment } from '@/types/api';
import { latestSeq } from '@/lib/artifactCommentReads';

const NO_PULSE: ReadonlySet<string> = new Set();
/** How long a pin wears its pulse: the ring leaving it is the longer part (PinBadge.css). */
const PULSE_MS = DURATION.reveal * 2000;

/** A comment's unread and its agent's newest message, as last drawn. */
type Noted = { unread: number; agent: number };

/**
 * The pins a reply that landed while the page was open pulses once: its
 * unread rose and the agent wrote something new. Each comment is noted from
 * the first settled read of the comments on (`ready`), so what was waiting
 * when the page opened, or came with the read that opened it, does not pulse;
 * nor a reply landing in the card shown (`shownId`), nor a refused read put back.
 */
export function useReplyPulse(
  comments: readonly ArtifactComment[],
  ready: boolean,
  shownId: string | null,
): ReadonlySet<string> {
  const [unreadWas, setUnreadWas] = useState<{ of: readonly ArtifactComment[]; map: ReadonlyMap<string, Noted> } | null>(null);
  const [pulse, setPulse] = useState({ ids: NO_PULSE, serial: 0 });
  if (unreadWas ? unreadWas.of !== comments : ready) {
    const noted = new Map(comments.map((c) => [c.comment_id, { unread: c.unread, agent: latestSeq(c, 'agent') }]));
    const rose = unreadWas
      ? comments.filter((c) => {
          const was = unreadWas.map.get(c.comment_id) ?? { unread: 0, agent: 0 };
          return c.unread > was.unread && latestSeq(c, 'agent') > was.agent && c.comment_id !== shownId;
        })
      : [];
    setUnreadWas({ of: comments, map: noted });
    if (rose.length > 0) {
      setPulse((prev) => ({ ids: new Set([...prev.ids, ...rose.map((c) => c.comment_id)]), serial: prev.serial + 1 }));
    }
  }
  useEffect(() => {
    if (pulse.serial === 0) return;
    const timer = setTimeout(() => setPulse((prev) => ({ ids: NO_PULSE, serial: prev.serial })), PULSE_MS);
    return () => clearTimeout(timer);
  }, [pulse.serial]);
  return pulse.ids;
}
