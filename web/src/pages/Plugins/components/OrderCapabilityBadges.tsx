import { useTranslation } from 'react-i18next';
import { TagBadge } from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { activeRungs, type Brokerage } from '../brokerages';

/**
 * What a brokerage can do about orders, said on the row rather than found by
 * connecting it.
 *
 * The one question a broker is asked first and the page could not answer:
 * paper, preview, staged, live. It is four different things at four different
 * costs, and until the ladder had four keys the row could only have said
 * "orders" about all of them.
 *
 * The label is the consent toggle's own, deliberately. A badge that named a
 * capability one way and the switch that grants it another would be two
 * vocabularies for one fact, and the badge is where the user meets it first.
 */
export function OrderCapabilityBadges({
  vendor,
  granted,
}: {
  vendor: Brokerage | null | undefined;
  /** The connection's grant, or null when there is no connection to read. */
  granted: string[] | null | undefined;
}) {
  const { t } = useTranslation();
  const { rungs, settled } = activeRungs(vendor, granted);
  if (rungs.length === 0) return null;
  // Same badges either way, because the difference is not one a colour can
  // carry: before a connection these are what the broker offers, after one they
  // are what it may actually do. The title says which, and the section in the
  // detail overlay says it in full.
  const state = t(
    settled ? 'plugins.brokerages.detail.granted' : 'plugins.brokerages.detail.offered',
  );
  return (
    <>
      {rungs.map((group) => {
        const label = t(`plugins.brokerages.capabilities.${group.key}.label`);
        return (
          <TagBadge
            key={group.key}
            // Warning weight only once it is real: a broker that *offers* live
            // orders has not been given them, and painting the offer amber
            // spends the page's loudest ink on something nobody agreed to.
            tone={settled && group.tone === 'danger' ? 'warning' : 'muted'}
            title={`${label} · ${state}`}
          >
            {label}
          </TagBadge>
        );
      })}
    </>
  );
}
