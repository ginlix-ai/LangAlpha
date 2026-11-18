import type React from 'react';
import type { NotificationSegment, SegmentNotice } from '@/types/chat';
import { CliWaitNotice } from '../CliQueueNotice';
import { NotificationDivider } from './NotificationDivider';
import { PlanBilledNotice } from './PlanBilledNotice';

type NoticeKind = SegmentNotice['kind'];
type NoticeOf<K extends NoticeKind> = Extract<SegmentNotice, { kind: K }>;
type Notification = Omit<NotificationSegment, 'type'>;

interface NoticeSpec<K extends NoticeKind> {
  /** A collapsed turn keeps it with its answer rather than folding it away
   *  with the working (`foldRole`). */
  retain: boolean;
  render: (notice: NoticeOf<K>, segment: Notification) => React.ReactNode;
}

/**
 * How each kind of notice is drawn, and whether a collapsed turn keeps it.
 * A model switch folds with the rest of the working. A turn that opened on
 * another model's brief keeps its mark, which says what the reply was
 * written from. A reply billed to usage credits keeps its notice, since it
 * is what the answer cost, and so does a turn that waited for a Claude Code
 * CLI, since the wait is part of how long the answer took and the line
 * offers the fix. Keeping one is a product decision, which
 * `contentProjection.foldRole.test.ts` pins.
 */
export const NOTICES: { readonly [K in NoticeKind]: NoticeSpec<K> } = {
  fallback: {
    retain: false,
    render: (notice, { detail, detailKind }) => <NotificationDivider fallback={notice} detail={detail} detailKind={detailKind} />,
  },
  handoff: {
    retain: true,
    render: (notice, { detail, detailKind }) => <NotificationDivider handoff={notice} detail={detail} detailKind={detailKind} />,
  },
  planBilled: { retain: true, render: (notice) => <PlanBilledNotice billed={notice} /> },
  cliWait: { retain: true, render: (notice) => <CliWaitNotice wait={notice} /> },
};

/** Whether a collapsed turn keeps this notification. */
export const retainsNotice = (segment: Notification): boolean => !!segment.notice && NOTICES[segment.notice.kind].retain;

/** A notification in a reply: its notice as `NOTICES` draws it, else a
 *  divider with its label. */
export function renderNotification(segment: Notification): React.ReactNode {
  const { notice } = segment;
  if (!notice) return <NotificationDivider content={segment.content} detail={segment.detail} detailKind={segment.detailKind} />;
  // TypeScript cannot pair the kind with its entry through the lookup.
  const render = NOTICES[notice.kind].render as (notice: SegmentNotice, segment: Notification) => React.ReactNode;
  return render(notice, segment);
}
