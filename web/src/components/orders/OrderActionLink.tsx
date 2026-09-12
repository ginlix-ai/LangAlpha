import { useTranslation } from 'react-i18next';
import { ExternalLink } from 'lucide-react';
import { isOrderOpen } from '@/pages/ChatAgent/utils/api/orders';
import type { OrderStatus } from '@/types/orders';

/**
 * A vendor link the page is willing to open, or null.
 *
 * The href arrives from a brokerage adapter rather than from this app, and an
 * anchor is the one place a string turns into code, so only the two schemes a
 * deep link can honestly have reach the DOM. Anything else is dropped rather
 * than drawn dead: a button on an order card that does nothing when clicked is
 * worse than no button.
 */
function safeExternalHref(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url, window.location.origin);
    return parsed.protocol === 'https:' || parsed.protocol === 'http:' ? parsed.href : null;
  } catch {
    return null;
  }
}

/**
 * The way out to the vendor's own client, for an order only the user can
 * finish there. A staged instruction is not an order until it is opened at the
 * broker, so on the surfaces that carry one this is the action, not a
 * footnote: it wears the primary button's fill, and the quieter links beside
 * it stay text. Its click stops here, since both surfaces that draw it sit
 * inside something else clickable.
 *
 * Drawn only while the ledger can still move the order. The href is frozen
 * when the vendor answers and nothing afterwards clears it, so an instruction
 * the broker no longer holds still carries one; offering it beside a verdict
 * that already reads cancelled points at an instruction that is gone.
 */
export function OrderActionLink({
  href,
  vendorLabel,
  status,
}: {
  href: string | null | undefined;
  vendorLabel: string;
  status: OrderStatus | null | undefined;
}) {
  const { t } = useTranslation();
  const safe = safeExternalHref(href);
  if (!safe || !isOrderOpen(status)) return null;
  return (
    <a
      href={safe}
      target="_blank"
      rel="noopener noreferrer"
      onClick={(e) => e.stopPropagation()}
      data-testid="order-action-link"
      className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md font-medium transition-colors hover:brightness-110 whitespace-nowrap"
      style={{
        backgroundColor: 'var(--color-btn-primary-bg)',
        color: 'var(--color-btn-primary-text)',
      }}
    >
      {t('toolArtifact.directTool.orderReceipt.openAt', { vendor: vendorLabel })}
      <ExternalLink className="h-3 w-3" aria-hidden="true" />
    </a>
  );
}
