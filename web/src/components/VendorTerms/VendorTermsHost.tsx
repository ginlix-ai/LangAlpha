import { lazy, Suspense, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { acceptVendorTerms, dismissVendorTerms, type PendingVendorTerms } from '@/api/vendorTerms';
import { apiErrorStatus } from '@/api/errors';
import { toast } from '@/components/ui/use-toast';
import { usePendingVendorTerms } from '@/hooks/useVendorTerms';
import { refreshPendingVendorTerms, vendorTermsRequestKey } from '@/lib/vendorTerms/reconcile';

// Off first load: a user sees it once per vendor at most.
const VendorTermsDialog = lazy(() => import('./VendorTermsDialog'));

const EMPTY: ReadonlySet<string> = new Set();

/**
 * Puts each vendor's terms that wait on the user, one at a time, on whatever
 * page is open. The agent's command requests' fallback (`CliApprovalHost`):
 * `held` while one of those shows, it shows nothing but keeps what it has
 * answered.
 *
 * An answer closes the dialog at once, though an accept sets up the user's
 * account and takes seconds. A failed answer brings it back, an accept with a
 * toast: the server still holds the same request, which the agent's next call
 * only renews, so a request left hidden here would never show again.
 */
export function VendorTermsHost({ held = false }: { held?: boolean } = {}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const pending = usePendingVendorTerms();
  // Answered here, until the list catches up. Each change keeps only the
  // requests still waiting, so it never outgrows the list.
  const [hidden, setHidden] = useState<ReadonlySet<string>>(EMPTY);

  const hide = (key: string, on: boolean) =>
    setHidden((prev) => {
      const next = new Set(pending.map(vendorTermsRequestKey).filter((k) => k !== key && prev.has(k)));
      if (on) next.add(key);
      return next;
    });

  const answer = useMutation({
    mutationFn: async ({ terms, accept }: { terms: PendingVendorTerms; accept: boolean }) => {
      if (accept) await acceptVendorTerms(terms.vendor);
      else await dismissVendorTerms(terms.vendor);
    },
    onMutate: ({ terms }) => hide(vendorTermsRequestKey(terms), true),
    onSuccess: (_data, { terms, accept }) => {
      if (accept) toast({ description: t('vendorTerms.accepted', { name: terms.name }) });
    },
    onError: (err, { terms, accept }) => {
      // A 404 is a vendor the server no longer knows, or an older worker
      // mid-deploy without the route: nothing to tell either way.
      if (accept && apiErrorStatus(err) !== 404) {
        toast({ variant: 'destructive', description: t('vendorTerms.failed', { name: terms.name }) });
      }
    },
    // A failed answer is shown again once the list, read again first, still
    // names it, so a vendor the server dropped does not flash back.
    onSettled: async (_data, err, { terms }) => {
      await refreshPendingVendorTerms(queryClient);
      if (err) hide(vendorTermsRequestKey(terms), false);
    },
  });

  const current = pending.find((p) => !hidden.has(vendorTermsRequestKey(p)));
  if (held || !current) return null;

  return (
    <Suspense fallback={null}>
      <VendorTermsDialog
        key={vendorTermsRequestKey(current)}
        terms={current}
        onAccept={(terms) => answer.mutate({ terms, accept: true })}
        onDismiss={(terms) => answer.mutate({ terms, accept: false })}
      />
    </Suspense>
  );
}
