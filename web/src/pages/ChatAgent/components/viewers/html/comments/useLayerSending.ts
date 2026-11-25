import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useArtifactBinding } from '@/hooks/useArtifact';
import type { ArtifactComment, ArtifactTarget } from '@/types/api';
import type { CommentTarget } from '../../../../utils/api/artifacts';
import { useArtifactCommentSender } from '../../../artifactComments/sendContext';
import type { CommentDestinationProps } from './CommentDestination';
import type { CommentLayerValue } from './commentLayerContext';
import { targetKey, threadOf, type CommentIntent, type CommentRoute } from '../../../artifactComments/destinations';
import { useCommentActions } from './useCommentActions';
import { useCommentDestinations } from '../../../artifactComments/useCommentDestinations';
import type { CommentUi } from './useCommentUi';

interface LayerSendingOptions {
  workspaceId: string;
  /** What the frame holds, as `targetFor` keys it: its comments are the layer's. */
  doc: ArtifactTarget;
  /** What the comment being written is on, which a link may have led the frame away from. */
  draft: ArtifactTarget;
  ui: CommentUi;
  /** Where the comment being written goes when the owner picked other than the default. */
  to: CommentTarget | null;
  /** The owner picked where it goes; null for the default. */
  onTo: (to: CommentTarget | null) => void;
}

export interface LayerSending {
  /** What the layer's pins and cards share. */
  layer: CommentLayerValue;
  /** Where the comment being written goes for `intent`; null while that is not known. */
  draftRoute: (intent: CommentIntent) => CommentRoute | null;
  /** Its "To:" line. */
  destination: CommentDestinationProps;
}

/**
 * Where the layer's comments go, and the moves that take them there: the
 * "To:" menu, binding the thread beside, and the writes every pin and card
 * makes. The comment being written goes by its own file's destinations, the
 * pinned ones by the frame's.
 */
export function useLayerSending({ workspaceId, doc, draft, ui, to, onTo }: LayerSendingOptions): LayerSending {
  const { t } = useTranslation();
  const sender = useArtifactCommentSender();
  const beside = threadOf(sender.beside);
  const destinations = useCommentDestinations(workspaceId, doc);
  const forDraft = useCommentDestinations(workspaceId, draft);
  const { bind } = useArtifactBinding();
  // The agent is on a comment while a turn runs on the thread beside and the
  // comment went there; a turn on another thread is not one this view hears.
  const runningFor = (comment: ArtifactComment) =>
    sender.running && (comment.thread_id ?? beside) === beside;
  const actions = useCommentActions({ workspaceId, sender, sends: ui.sends });

  // Binding makes the thread beside the default for this file's comments.
  const bindBeside = () => {
    if (!beside) return;
    bind.mutate({ workspaceId, target: draft, threadId: beside }, {
      onError: () => toast({ description: t('pageViewer:comments.bindFailed'), variant: 'destructive' }),
    });
    onTo(null);
  };
  const picked = to ?? forDraft.fallback;

  return {
    layer: { actions, besideThreadId: beside, destinations, runningFor },
    draftRoute: (intent) => forDraft.targetFor(null, intent, to),
    destination: {
      choices: forDraft.choices,
      selected: picked ? targetKey(picked) : '',
      onPick: (choice) => onTo(choice.isDefault ? null : choice.target),
      onBindBeside: forDraft.canBind ? bindBeside : null,
    },
  };
}
