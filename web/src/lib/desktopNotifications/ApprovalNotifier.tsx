/**
 * A desktop notification for each request the agent made from a command that
 * waits on the user, taken down once it is answered, runs out, or the page
 * goes. The main window asks the question itself; this only reaches a user
 * who is not looking at it.
 */
import { useEffect, useRef } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import type { CliApproval } from '@/api/cliApprovals';
import { approvalTitle, requestSentenceText } from '@/lib/cliApprovals/copy';
import { usePendingCliApprovals } from '@/hooks/useCliApprovals';
import { clearDesktopNotifications, desktopNotify, type DesktopNotification } from '@/lib/desktop';
import { threadPath } from './notifications';

/** Apart from the thread notices, which are keyed by thread id. */
const approvalNotificationId = (approvalId: string): string => `cli-approval:${approvalId}`;

/**
 * Posted as `needs_input`: a question to answer, not news to read, so the
 * shell gives it no "Mark as read" and the user's setting for questions
 * governs it. A click opens the thread that asked, or the app.
 */
function toApprovalNotification(t: TFunction, a: CliApproval): DesktopNotification {
  return {
    id: approvalNotificationId(a.approval_id),
    kind: 'needs_input',
    title: approvalTitle(t, a),
    body: requestSentenceText(t, a),
    path: a.requester.thread_id ? threadPath(a.requester.thread_id) : '/',
  };
}

export function ApprovalNotifier(): null {
  const { t } = useTranslation();
  const pending = usePendingCliApprovals();
  const posted = useRef(new Set<string>());

  useEffect(() => {
    const shown = posted.current;
    const waiting = new Set<string>();
    for (const approval of pending) {
      const id = approvalNotificationId(approval.approval_id);
      waiting.add(id);
      if (shown.has(id)) continue;
      shown.add(id);
      void desktopNotify(toApprovalNotification(t, approval));
    }
    const gone = [...shown].filter((id) => !waiting.has(id));
    for (const id of gone) shown.delete(id);
    clearDesktopNotifications(gone);
  }, [pending, t]);

  // Signed out, or the page is going: a click on one would answer for nobody.
  useEffect(() => {
    const shown = posted.current;
    return () => clearDesktopNotifications([...shown]);
  }, []);

  return null;
}
