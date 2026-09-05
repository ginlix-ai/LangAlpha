import { useTranslation } from 'react-i18next';
import { AlertTriangle, Link2, Link2Off, Monitor, RefreshCw } from 'lucide-react';

import { Loader } from '@/components/ui/loader';
import { DropdownMenuItem } from '@/components/ui/dropdown-menu';
import { MetaText } from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { canDisconnectOauth } from '@/pages/ChatAgent/components/mcp/mcpState';
import type { McpOauthStatus } from '@/pages/ChatAgent/utils/api';
import { connectBlock, type Brokerage } from '../brokerages';
import { RowNote } from './RowNote';

/**
 * The parts every OAuth-capable row draws the same way.
 *
 * Two rows show a user-tier server that can connect: the catalog row under
 * `Your servers`, and the brokerage row on its own tab. They differ in real
 * ways — one offers Edit, the other exists before its row does — so they stay
 * two components. What they have no business differing on is the connect
 * button, the vendor's constraints, and the OAuth menu items, and the one time
 * those were written twice they disagreed about whether a flow this build
 * cannot finish was still clickable.
 */

/** Ties a row's connect button to the note saying why it cannot run. */
function blockedNoteId(rowKey: string): string {
  return `oauth-block-${rowKey}`;
}

/**
 * Where the page explains a registry it asked for and did not get.
 *
 * One id for the whole list rather than one note per row: the cause is the
 * page's and not any row's, and a sentence repeated under every server the user
 * owns is the same sentence that many times. Several buttons pointing at one
 * description is what `aria-describedby` is for.
 */
export const REGISTRY_NOTE_ID = 'oauth-block-registry';

/**
 * What stands between this row and a connect, said before anyone clicks.
 *
 * Nothing at all once connected, which is why this takes `unconnected` rather
 * than reading the row: a constraint on starting a flow has nothing to say
 * about one that finished.
 *
 * Not every note here is the vendor's. A shell too old to hold a loopback
 * listener blocks every row on the page, so that note renders for a row with no
 * vendor at all -- which is why the vendor is read after `connectBlock` rather
 * than guarded on before it.
 */
export function VendorNotes({
  vendor,
  unconnected,
  rowKey,
}: {
  vendor: Brokerage | null | undefined;
  unconnected: boolean;
  rowKey: string;
}) {
  const { t } = useTranslation();
  if (!unconnected) return null;
  const block = connectBlock(vendor);
  return (
    <>
      {block === 'shell-outdated' && (
        <RowNote icon={Monitor} id={blockedNoteId(rowKey)}>
          {t('plugins.oauth.shellOutdatedNote')}
        </RowNote>
      )}
      {block === 'native-only' && (
        <RowNote icon={Monitor} id={blockedNoteId(rowKey)}>
          {t('plugins.oauth.nativeOnlyNote')}
        </RowNote>
      )}
      {vendor?.exclusive_connection && (
        // Said before the click, not after: connecting here takes the
        // account's one slot from wherever it is now. Warning weight because
        // that is a consequence and not a capability note -- the two sat at
        // the same tertiary grey, so the one with a cost read as the quieter.
        <RowNote icon={AlertTriangle} tone="warning">
          {t('plugins.brokerages.exclusiveNote')}
        </RowNote>
      )}
    </>
  );
}

/** How many tools the last good discovery found, once there is a connection. */
export function ToolCountText({
  status,
  count,
}: {
  status: McpOauthStatus | null;
  count: number | null | undefined;
}) {
  const { t } = useTranslation();
  if (status !== 'connected' || typeof count !== 'number' || count <= 0) return null;
  return <MetaText>{t('mcp.row.toolCount', { count })}</MetaText>;
}

/**
 * Start or repair a connection.
 *
 * `blockedBy` is the id of the note saying why this cannot run, and it is
 * `aria-disabled` rather than `disabled` on purpose: a disabled button is not
 * focusable, so the one user who most needs the reason is the one who can
 * never reach the control it belongs to. Busy is a real `disabled` — that one
 * is transient and has nothing to explain.
 */
export function ConnectButton({
  status,
  connecting,
  vendor,
  rowKey,
  emphasis = 'quiet',
  registryUnavailable = false,
  testid,
  onClick,
}: {
  status: McpOauthStatus | null;
  connecting: boolean;
  /** Whose constraints decide this, matched to the row's own `VendorNotes`. */
  vendor: Brokerage | null | undefined;
  rowKey: string;
  /** `loud` is for the one row whose whole point is that it is not connected. */
  emphasis?: 'quiet' | 'loud';
  /** The registry was asked and did not answer, so the list is carrying the
   *  note this button points at. False while it is merely still in flight. */
  registryUnavailable?: boolean;
  testid?: string;
  onClick: () => void;
}) {
  const { t } = useTranslation();
  // Derived here rather than passed in, so the button and the note it points
  // at cannot be given different answers by two different callers.
  const block = connectBlock(vendor);
  const blocked = block !== null;
  // Every block has a note to point at except an unsettled registry, which has
  // nothing to say yet -- and naming a missing id is worse than silence. Once
  // the registry has been asked and failed there is something to say, the list
  // says it once, and every button held for it points there.
  let noteId: string | undefined;
  if (block === 'unknown') {
    noteId = registryUnavailable ? REGISTRY_NOTE_ID : undefined;
  } else if (block) {
    noteId = blockedNoteId(rowKey);
  }
  // No pointer affordance on a button that will not act, and the transition
  // travels with the hover it belongs to: the loud variant fades its opacity
  // and the quiet one crosses a background, so a single `transition-colors` on
  // the base left the filled button snapping between two opacities.
  const hover = blocked
    ? ''
    : emphasis === 'loud'
      ? 'transition-opacity enabled:hover:opacity-90'
      : 'transition-colors enabled:hover:bg-[var(--color-bg-hover)]';
  return (
    <button
      type="button"
      onClick={() => {
        if (!blocked) onClick();
      }}
      disabled={connecting}
      aria-disabled={blocked || undefined}
      aria-describedby={noteId}
      data-testid={testid}
      // `px-2 py-1` matches ScopeControl's badge and trigger, which sit
      // immediately beside this at the same size.
      className={`inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md disabled:opacity-50 aria-disabled:opacity-50 aria-disabled:cursor-not-allowed ${hover}`}
      style={
        // The same pair the page's own primary CTA uses. Emphasis here is ink
        // against outline, which is the contrast the system is built on; the
        // amber is annotation language, and painting a CTA with it is the one
        // thing DESIGN.md names outright. It also read at about 2.99:1 in light
        // theme on 11px text, because the token that belongs on the accent is a
        // different one again.
        emphasis === 'loud'
          ? {
              color: 'var(--color-btn-primary-text)',
              backgroundColor: 'var(--color-btn-primary-bg)',
            }
          : {
              color: 'var(--color-text-primary)',
              border: '1px solid var(--color-border-muted)',
            }
      }
    >
      {connecting ? (
        <Loader size={12} className="text-current" />
      ) : (
        <Link2 className="h-3 w-3" />
      )}
      {status ? t('plugins.oauth.reconnect') : t('plugins.oauth.connect')}
    </button>
  );
}

/** The kebab items a live connection owns, in the order both rows show them. */
export function OauthMenuItems({
  status,
  onRefreshSchemas,
  onDisconnect,
}: {
  status: McpOauthStatus | null;
  onRefreshSchemas: () => void;
  onDisconnect: () => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      {status === 'connected' && (
        <DropdownMenuItem onSelect={onRefreshSchemas}>
          <RefreshCw className="h-3.5 w-3.5 mr-2" />
          {t('plugins.oauth.refreshSchemas')}
        </DropdownMenuItem>
      )}
      {canDisconnectOauth(status) && (
        <DropdownMenuItem onSelect={onDisconnect}>
          <Link2Off className="h-3.5 w-3.5 mr-2" />
          {t('plugins.oauth.disconnect')}
        </DropdownMenuItem>
      )}
    </>
  );
}
