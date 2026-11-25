import { useEffect, useMemo } from 'react';
import { useStableHandler } from '@/hooks/useStableHandler';
import type { ArtifactCommentOpeners, ArtifactCommentRef } from '../../ChatAgent/components/artifactComments/artifactCommentReply';
import {
  useChatCommentSends,
  type ChatCommentSends,
} from '../../ChatAgent/components/artifactComments/useChatCommentSends';

type ChatCommentSendsOptions = Parameters<typeof useChatCommentSends>[0];

/**
 * The market chat's side of the chart's comments: the queue for its
 * composer, its sends handed up to the chart beside it, which sends its
 * comments here as a file panel does to the chat beside it, and the opener a
 * line naming a comment calls. The chart is the only target this page opens
 * a comment on; a file has no panel to open in here.
 */
export function useMarketCommentSends({ onCommentSends, onOpenChartComment, ...chat }: Omit<ChatCommentSendsOptions, 'chatCovered'> & {
  /** The chat's sends for the chart beside; null once this chat is gone. */
  onCommentSends?: (sends: Pick<ChatCommentSends, 'sender' | 'sends'> | null) => void;
  /** Brings a comment on the chart beside forward. */
  onOpenChartComment?: (comment: ArtifactCommentRef) => void;
}): Pick<ChatCommentSends, 'deck' | 'take'> & { openers: ArtifactCommentOpeners | undefined } {
  // The chat is always on screen beside the chart.
  const { deck, take, sender, sends } = useChatCommentSends({ ...chat, chatCovered: () => false });
  useEffect(() => {
    onCommentSends?.({ sender, sends });
  }, [onCommentSends, sender, sends]);
  useEffect(() => () => onCommentSends?.(null), [onCommentSends]);

  const open = useStableHandler((comment: ArtifactCommentRef) => onOpenChartComment?.(comment));
  const opens = !!onOpenChartComment;
  const openers = useMemo(() => (opens ? { chart: open } : undefined), [opens, open]);
  return { deck, take, openers };
}
