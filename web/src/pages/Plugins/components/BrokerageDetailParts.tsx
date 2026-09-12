import { useTranslation } from 'react-i18next';
import { AlertTriangle, Check, Minus, Monitor } from 'lucide-react';
import type { Brokerage, CapabilityGroup } from '../brokerages';
import { DetailField } from './DetailOverlay';
import { RowNote } from './RowNote';

/**
 * The two sections a brokerage detail has that no other server does: what the
 * connection was granted, and what the vendor is.
 *
 * Their own file because they are the only brokerage-shaped thing in an
 * overlay shared by four origins, and the shared half -- identity, config,
 * dates -- is worth keeping legible. `ServerDetail` decides when they render;
 * these decide what they say.
 */

/** Granted / declined / offered, as one line per capability group. */
export function CapabilityList({
  groups,
  granted,
}: {
  groups: CapabilityGroup[];
  /** The connection's grant, or null when there is no connection to read. */
  granted: string[] | null | undefined;
}) {
  const { t } = useTranslation();
  const settled = granted != null;
  return (
    <div className="flex flex-col gap-2.5">
      {granted?.length === 0 && (
        <RowNote icon={AlertTriangle} tone="warning">
          {t('plugins.brokerages.detail.grantedNone')}
        </RowNote>
      )}

      {groups.map((group) => {
        const on = !settled || granted.includes(group.key);
        return (
          <div key={group.key} className="flex items-start gap-2">
            <GrantGlyph on={on} settled={settled} />
            <div className="min-w-0">
              <p
                className="text-xs font-medium"
                style={{
                  color:
                    // Amber only where it is both real and granted. A declined
                    // group has no consequence to warn about, and an offer is
                    // not a grant.
                    on && settled && group.tone === 'danger'
                      ? 'var(--color-warning)'
                      : on
                        ? 'var(--color-text-primary)'
                        : 'var(--color-text-quaternary)',
                }}
              >
                {t(`plugins.brokerages.capabilities.${group.key}.label`)}
              </p>
              <p
                className="text-[0.6875rem] mt-0.5"
                style={{
                  color: on
                    ? 'var(--color-text-tertiary)'
                    : 'var(--color-text-quaternary)',
                }}
              >
                {t(`plugins.brokerages.capabilities.${group.key}.desc`)}
              </p>
            </div>
          </div>
        );
      })}

      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-quaternary)' }}>
        {t(
          settled
            ? 'plugins.brokerages.detail.capabilitiesNote'
            : 'plugins.brokerages.detail.notConnectedNote',
        )}
      </p>
    </div>
  );
}

/**
 * Whether this group is on, drawn as a glyph rather than a colour.
 *
 * The one thing on this page that must not be a colour alone: the difference
 * between a connection that can place live orders and one that cannot is the
 * whole reason the section exists, and it has to survive a colourblind reader
 * and a screenshot in grayscale.
 */
function GrantGlyph({ on, settled }: { on: boolean; settled: boolean }) {
  const { t } = useTranslation();
  const label = t(
    !settled
      ? 'plugins.brokerages.detail.offered'
      : on
        ? 'plugins.brokerages.detail.granted'
        : 'plugins.brokerages.detail.declined',
  );
  const Icon = settled && !on ? Minus : Check;
  return (
    <span
      title={label}
      aria-label={label}
      role="img"
      className="flex-shrink-0 mt-0.5"
      style={{
        color: on ? 'var(--color-text-secondary)' : 'var(--color-text-quaternary)',
      }}
    >
      <Icon className="h-3.5 w-3.5" />
    </span>
  );
}

/** Who the broker is, and what connecting to it costs. */
export function BrokerFacts({
  vendor,
  rowUrl,
}: {
  vendor: Brokerage;
  /** The row's own address, when it has one. The registry's endpoint is shown
   *  only when it is not already below as the row's URL, or when there is no
   *  row to show one -- and it is worth showing when the two differ, which is
   *  exactly a row someone has pointed somewhere else. */
  rowUrl?: string | null;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-1.5">
      <DetailField label={t('plugins.brokerages.detail.broker')}>
        {vendor.label}
      </DetailField>
      {vendor.site && (
        <DetailField label={t('plugins.brokerages.detail.website')}>
          <a
            href={`https://${vendor.site}`}
            target="_blank"
            rel="noopener noreferrer"
            className="hover:underline underline-offset-2"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {vendor.site}
          </a>
        </DetailField>
      )}
      {rowUrl !== vendor.url && (
        <DetailField label={t('plugins.brokerages.detail.endpoint')}>
          {vendor.url}
        </DetailField>
      )}
      {(vendor.exclusive_connection || vendor.native_callback_only) && (
        <div className="flex flex-col gap-1 pt-1">
          {vendor.exclusive_connection && (
            <RowNote icon={AlertTriangle} tone="warning">
              {t('plugins.brokerages.exclusiveWarning')}
            </RowNote>
          )}
          {vendor.native_callback_only && (
            <RowNote icon={Monitor}>{t('plugins.oauth.nativeOnlyNote')}</RowNote>
          )}
        </div>
      )}
    </div>
  );
}
