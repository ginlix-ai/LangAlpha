import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';
import { EnabledToggle } from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { defaultGrant, type Brokerage } from '../brokerages';
import { PluginDialog } from './PluginDialog';
import { RowNote } from './RowNote';

/**
 * What a brokerage connection may do, answered before the vendor is reached.
 *
 * A dialog rather than the confirm strip the other questions on this page use,
 * and shared by both surfaces that can start a connect, for the same reason:
 * this is the only question here whose answer is enforced for the life of the
 * connection. What is not ticked is refused per call at the relay, so the
 * choice has to be legible enough to make deliberately, and a one-line strip
 * with a Yes cannot hold seven toggles and the sentence each one needs.
 *
 * The vendor's own terms ride inside it when there are any, rather than in a
 * second question stacked behind this one. They are two halves of one decision:
 * what this connection can do, and what making it costs elsewhere.
 */
export function BrokerageConsentDialog({
  vendor,
  name,
  granted: current,
  pending,
  onConfirm,
  onCancel,
}: {
  /** Null for a row whose vendor the registry has not resolved; the dialog then
   *  carries the vendor's terms alone, which is all there is to ask. */
  vendor: Brokerage | null | undefined;
  /** The row's name, for a vendor with no label of its own to show. */
  name: string;
  /** What the connection already grants, or null for a first connect. */
  granted?: string[] | null;
  pending: boolean;
  onConfirm: (grantedCapabilities: string[]) => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const groups = vendor?.capabilities ?? [];
  // What the user last chose, and the vendor's default only for a connection
  // that has never been asked. This dialog is also the way an existing grant is
  // narrowed, and opening it on the default re-ticked every group the user had
  // declined -- offering to widen consent while looking like it was showing it.
  //
  // Seeded once. Re-deriving per render would undo the user's own ticks, and
  // the dialog is mounted for exactly one question, so there is nothing for it
  // to go stale against: the call sites key it by row, so a different row
  // opens a different dialog.
  const [granted, setGranted] = useState<string[]>(
    () => current ?? defaultGrant(vendor),
  );
  const label = vendor?.label ?? name;

  function toggle(key: string) {
    setGranted((current) =>
      current.includes(key) ? current.filter((k) => k !== key) : [...current, key],
    );
  }

  return (
    <PluginDialog
      title={t('plugins.brokerages.consent.title', { server: label })}
      subtitle={t('plugins.brokerages.consent.subtitle')}
      onClose={onCancel}
      // The connect is running and the page is about to leave for the vendor;
      // closing here would strand a flow this dialog can no longer stop.
      dismissable={!pending}
    >
      <div className="flex flex-col gap-4">
        {vendor?.exclusive_connection && (
          <RowNote icon={AlertTriangle} tone="warning">
            {t('plugins.brokerages.exclusiveWarning')}
          </RowNote>
        )}

        {groups.length > 0 && (
          <ul className="flex flex-col gap-3">
            {groups.map((group) => {
              const groupLabel = t(`plugins.brokerages.capabilities.${group.key}.label`);
              return (
                <li key={group.key} className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p
                      className="text-xs font-medium"
                      style={{
                        color:
                          group.tone === 'danger'
                            ? 'var(--color-warning)'
                            : 'var(--color-text-primary)',
                      }}
                    >
                      {groupLabel}
                    </p>
                    <p
                      className="text-[0.6875rem] mt-0.5"
                      style={{ color: 'var(--color-text-tertiary)' }}
                    >
                      {t(`plugins.brokerages.capabilities.${group.key}.desc`)}
                    </p>
                  </div>
                  <div className="flex-shrink-0 pt-0.5">
                    <EnabledToggle
                      enabled={granted.includes(group.key)}
                      name={groupLabel}
                      disabled={pending}
                      onToggle={() => toggle(group.key)}
                    />
                  </div>
                </li>
              );
            })}
          </ul>
        )}

        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.brokerages.consent.footnote')}
        </p>

        <div className="flex items-center justify-end gap-2">
          {/* Live while pending, unlike the toggles and Confirm beside it. The
              request in flight is a registration at the vendor, not a connect,
              and backing out of it costs the user nothing; a Cancel that greys
              out the moment it is pressed leaves a slow vendor holding the
              dialog open with no way out but the escape key. */}
          <button
            type="button"
            onClick={onCancel}
            className="px-3 py-1.5 rounded text-xs hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('plugins.servers.deleteConfirmNo')}
          </button>
          <button
            type="button"
            onClick={() => onConfirm(granted)}
            disabled={pending}
            className="px-3 py-1.5 rounded text-xs disabled:opacity-50"
            style={{
              color: 'var(--color-btn-primary-text)',
              backgroundColor: 'var(--color-btn-primary-bg)',
            }}
          >
            {pending ? t('common.loading') : t('plugins.oauth.connect')}
          </button>
        </div>
      </div>
    </PluginDialog>
  );
}
