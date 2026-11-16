/**
 * A desktop notification for each vendor's terms that wait on the user, taken
 * down once they are answered or the page goes. The main window asks the
 * question itself; this only reaches a user who is not looking at it while
 * the agent waits.
 */
import { useEffect, useRef } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import type { PendingVendorTerms } from '@/api/vendorTerms';
import { usePendingVendorTerms } from '@/hooks/useVendorTerms';
import { clearDesktopNotifications, desktopNotify, type DesktopNotification } from '@/lib/desktop';
import { vendorTermsRequestKey } from '@/lib/vendorTerms/reconcile';

/** Apart from the thread notices and the command requests. */
const termsNotificationId = (p: PendingVendorTerms): string => `vendor-terms:${vendorTermsRequestKey(p)}`;

/** A question to answer, as the command requests are; a click opens the app, where the dialog waits. */
function toTermsNotification(t: TFunction, p: PendingVendorTerms): DesktopNotification {
  return {
    id: termsNotificationId(p),
    kind: 'needs_input',
    title: t('vendorTerms.title', { name: p.name }),
    body: t('vendorTerms.notification', { name: p.name }),
    path: '/',
  };
}

export function VendorTermsNotifier(): null {
  const { t } = useTranslation();
  const pending = usePendingVendorTerms();
  const posted = useRef(new Set<string>());

  useEffect(() => {
    const shown = posted.current;
    const waiting = new Set<string>();
    for (const terms of pending) {
      const id = termsNotificationId(terms);
      waiting.add(id);
      if (shown.has(id)) continue;
      shown.add(id);
      void desktopNotify(toTermsNotification(t, terms));
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
