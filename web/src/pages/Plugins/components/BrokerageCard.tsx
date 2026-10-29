import { Fragment, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  AlertTriangle,
  Banknote,
  Check,
  ChevronRight,
  Layers,
  Minus,
  Power,
  Server,
  Trash2,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { Loader } from '@/components/ui/loader';
import { brokerageArt } from '@/lib/brandArt';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import {
  activeRungs,
  connectBlock,
  settledGrant,
  type Brokerage,
  type CapabilityGroup,
  type ResolvedBroker,
} from '../brokerages';
import { useHomeOption } from '../hooks/useWorkspaceOptions';
import { isEffectivelyEnabled, isPluginOwned } from '../utils/provenance';
import { ConnectButton, OauthMenuItems, VendorNotes } from './OauthRowParts';
import { partialReachText } from './pluginsPage/pluginReach';
import { RowNote } from './RowNote';
import type { ScopeWorkspace } from './ScopeControl';
import { CellMenu, CellState, StatusDot } from './surface/Surface';
import './BrokerageCard.css';

/** What the Brokerages tab does with a row beyond connecting it: the ⋯ menu,
 *  the broker's page, and where the row reaches. */
export interface BrokerageCardManage {
  workspaces: ScopeWorkspace[];
  /** `workspaces` has not arrived yet; the reach waits for it. */
  workspacesLoading?: boolean;
  refreshing: boolean;
  onDisconnect: () => void;
  onRefreshSchemas: () => void;
  onRequestRemove: () => void;
  onOpenInMcpTab: () => void;
  /** Open this broker's page. Offered on an offer too: what a broker can do
   *  is the thing to read before deciding to add it. Workspaces live there. */
  onOpen: () => void;
}

/**
 * One shipped brokerage as a card, in whichever of its two states the user is
 * in: an offer with no row behind it yet, or their own catalog row.
 *
 * A card rather than a cell because there are only a handful and each is a
 * decision about real money: what the agent could reach there and how far it
 * could go with an order are the things to read before connecting, and a
 * one-line cell truncated them away.
 *
 * Both states are one card rather than two components because the second state
 * is the first one plus a row: the identity, the vendor's constraints and the
 * description never change, and rendering them from two places is how the two
 * would eventually disagree about a broker that can place orders.
 *
 * The foot says the one thing a brokerage is asked first, Connect or what the
 * connection may do about orders, beside whatever connecting costs. On/off,
 * workspaces and removal live in the ⋯ menu, and every action there is the
 * same mutation the Connectors cell calls. What it does not offer is Edit: the
 * address is the one thing we pick rather than the user, so changing it means
 * going to the section where it is an ordinary server.
 *
 * Without `manage` (the welcome flow) the card is the offer alone: the same
 * identity, groups and notes, with no menu and no page to open, and a row
 * that is switched off is switched back on from its foot.
 */
export function BrokerageCard({
  broker,
  connecting,
  toggling = false,
  onConnect,
  onToggle,
  manage,
}: {
  /** Resolved by the list rather than here, so the section and this card
   *  cannot reach different answers about which broker the row is. */
  broker: ResolvedBroker;
  connecting: boolean;
  toggling?: boolean;
  onConnect: () => void;
  onToggle: (enabled: boolean) => void;
  manage?: BrokerageCardManage;
}) {
  const { t } = useTranslation();
  const home = useHomeOption();
  const refreshing = manage?.refreshing ?? false;
  const {
    b: brokerage,
    row,
    vendor,
    redirected,
    label: title,
    needsSignIn: unconnected,
  } = broker;
  const status = row?.oauth_status ?? null;
  // One reading of the grant for the whole cell: the trailing state and the
  // note both draw it, and they used to disagree about null.
  const granted = settledGrant(row?.granted_capabilities, status);
  const rowKey = `brokerage-${brokerage.name}`;
  // A redirected row stops being presented as the broker: it goes back to its
  // own name and its own description, and the mark drops the vendor's art.
  const description = redirected
    ? row?.description
    : row?.description || brokerage.description;
  const enabled = !!row && isEffectivelyEnabled(row);
  const off = !!row && !unconnected && !enabled;
  // The vendor's site only while the row still reaches the vendor.
  const site = redirected ? null : brokerage.site;

  // What stands between the user and acting, said beside the button: a
  // consequence of connecting, a grant that permits nothing, an address the
  // user moved. `VendorNotes` has something to say only on an unconnected row
  // whose vendor carries a constraint, which is asked here the same way.
  const block = connectBlock(vendor);
  const vendorNotes =
    unconnected &&
    (block === 'shell-outdated' || block === 'native-only' || !!vendor?.exclusive_connection);
  const grantedNone = granted?.length === 0;
  const notes = vendorNotes || grantedNone || redirected;

  // An offer says nothing here: its section already says it is not added.
  const reach =
    manage && row && enabled && !manage.workspacesLoading
      ? partialReachText(t, row, manage.workspaces, home)
      : null;

  // The corner says where the broker stands, on every card, so the four read
  // as a set; the foot holds what to do about it. A grant that permits nothing
  // is fixed the way a missing one is, by signing in again, so it gets the same
  // button rather than a state to read.
  let action: ReactNode = null;
  let state: ReactNode;
  if (unconnected || (grantedNone && enabled && !refreshing)) {
    action = (
      <ConnectButton
        status={status}
        connecting={connecting}
        vendor={vendor}
        rowKey={rowKey}
        size="md"
        // The same outline on every card, a full-width bar of one weight in
        // each: which broker is waiting on the user is the corner's dot to say,
        // and a filled bar on one card tipped the whole grid towards it.
        testid={`brokerage-connect-${brokerage.name}`}
        onClick={onConnect}
      />
    );
  } else if (off && !manage) {
    // With no menu to hold the switch, it sits where Connect would. The
    // tokens are intact, so this is no second consent.
    action = (
      <ConnectButton
        status={status}
        connecting={toggling}
        vendor={vendor}
        rowKey={rowKey}
        size="md"
        label={t('plugins.shell.brokerage.switchOn')}
        testid={`brokerage-switch-on-${brokerage.name}`}
        onClick={() => onToggle(true)}
      />
    );
  }
  if (!row) {
    state = <span className="broker-card-state is-quiet">{t('plugins.brokerages.notAdded')}</span>;
  } else if (unconnected) {
    state = (
      <span className="broker-card-state">
        <StatusDot tone="attn" />
        {t('plugins.brokerages.sections.signIn')}
      </span>
    );
  } else if (refreshing) {
    state = <Loader size={14} className="text-current" />;
  } else if (!enabled) {
    state = <CellState state="off" />;
  } else if (granted && !grantedNone) {
    state = <GrantState vendor={vendor} granted={granted} />;
  } else if (!grantedNone) {
    state = <CellState state="on" />;
  }

  const menu =
    row &&
    manage && (
      <CellMenu label={t('mcp.row.actionsAria', { name: title })}>
        {enabled ? (
          <DropdownMenuItem disabled={toggling} onSelect={() => onToggle(false)}>
            <Power className="h-3.5 w-3.5 mr-2" />
            {t('plugins.shell.brokerage.switchOff')}
          </DropdownMenuItem>
        ) : (
          // A switch cannot sign anyone in, and Connect sits right
          // beside this; the reason is said rather than the item hidden.
          <DropdownMenuItem
            disabled={toggling || unconnected}
            onSelect={() => onToggle(true)}
          >
            <Power className="h-3.5 w-3.5 mr-2" />
            <span className="flex flex-col">
              <span>{t('plugins.shell.brokerage.switchOn')}</span>
              {unconnected && (
                <span className="brokerage-menu-hint">
                  {t('plugins.shell.brokerage.signInFirst')}
                </span>
              )}
            </span>
          </DropdownMenuItem>
        )}
        <DropdownMenuItem onSelect={manage.onOpen}>
          <Layers className="h-3.5 w-3.5 mr-2" />
          {t('plugins.shell.brokerage.workspaces')}
        </DropdownMenuItem>
        <OauthMenuItems
          status={status}
          onRefreshSchemas={manage.onRefreshSchemas}
          onDisconnect={manage.onDisconnect}
        />
        {/* The escape hatch, named for where it goes rather than for
            what it does there: the address and headers stay editable,
            just not from the section whose point is that we picked it. */}
        <DropdownMenuItem onSelect={manage.onOpenInMcpTab}>
          <Server className="h-3.5 w-3.5 mr-2" />
          {t('plugins.brokerages.openInMcp')}
        </DropdownMenuItem>
        {/* Hidden on a plugin's row, the way the Connectors section hides
            it: that row belongs to the plugin, and the delete route's
            ownership check does not stop this one. */}
        {!isPluginOwned(row) && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem onSelect={manage.onRequestRemove} variant="destructive">
              <Trash2 className="h-3.5 w-3.5 mr-2" />
              {t('plugins.shell.brokerage.remove')}
            </DropdownMenuItem>
          </>
        )}
      </CellMenu>
    );

  // The vendor's paragraph is written for the agent, in English, and the
  // groups below already say what it lists. A card with groups leaves it to the
  // broker's page; one without (a moved address) has nothing else to say.
  const groups = !redirected && vendor ? (vendor.capabilities ?? []) : [];
  const showDescription = !!description && groups.length === 0;

  return (
    <article data-testid={rowKey} className={cn('broker-card', off && 'is-off')}>
      <header className="broker-card-head">
        {/* Off `vendor`, like every other vendor-specific thing here: a row
            edited to another host keeps its name and drops to a monogram,
            rather than drawing a broker's mark over somebody else's endpoint. */}
        {/* On a tile of its own, so a transparent mark sits in the same square
            as a full-bleed one. */}
        <span className="broker-card-mark">
          <BrandMark name={brokerage.name} kind="server" size="lg" art={brokerageArt(vendor)} />
        </span>
        <div className="broker-card-id">
          {/* The name is the card's button and its hit area is the whole card;
              the controls and links sit above it. */}
          {manage ? (
            <button type="button" className="broker-card-open" onClick={manage.onOpen}>
              {title}
            </button>
          ) : (
            <span className="broker-card-name">{title}</span>
          )}
          {(site || reach) && (
            <div className="broker-card-meta">
              {site && (
                <a href={`https://${site}`} target="_blank" rel="noopener noreferrer">
                  {site}
                </a>
              )}
              {reach && <span>{reach}</span>}
            </div>
          )}
        </div>
        {(state || menu) && (
          <div className="broker-card-end">
            {state}
            {menu && <div className="broker-card-menu">{menu}</div>}
          </div>
        )}
      </header>

      {showDescription && <p className="broker-card-desc">{description}</p>}

      {groups.length > 0 && vendor && (
        <BrokerCapabilities vendor={vendor} granted={granted} />
      )}

      {/* The action and what it costs, side by side: a note about connecting
          is read at the moment of clicking or not at all. */}
      {(action || notes) && (
        <footer className="broker-card-foot">
          {notes && (
            <div className="broker-card-notes">
              {grantedNone && (
                <RowNote icon={AlertTriangle} tone="warning">
                  {t('plugins.brokerages.grantedNone')}
                </RowNote>
              )}
              {redirected && (
                <RowNote icon={AlertTriangle} tone="warning">
                  {t('plugins.brokerages.redirectedNote')}
                </RowNote>
              )}
              <VendorNotes vendor={vendor} unconnected={unconnected} rowKey={rowKey} />
            </div>
          )}
          {action && <div className="broker-card-action">{action}</div>}
        </footer>
      )}
    </article>
  );
}

/**
 * What the agent could reach at this broker, and how far it could go with an
 * order, from the registry's own groups. Before a connection every group is
 * the offer; after one, each says whether it was granted, by glyph as well as
 * weight, because which side of that line live orders fall on is the reason
 * this card exists.
 *
 * Under the order steps, one sentence on the furthest of them, in the card's
 * own words: the consent dialog's description of a step goes on to say how
 * approval works, which the trading permission beside the list says once for
 * every broker and which this card would only repeat, or get wrong after the
 * user changed it.
 */
function BrokerCapabilities({
  vendor,
  granted,
}: {
  vendor: Brokerage;
  granted: string[] | null;
}) {
  const { t, i18n } = useTranslation();
  const groups = vendor.capabilities ?? [];
  const access = groups.filter((group) => !group.rung);
  const orders = groups.filter((group) => group.rung);
  const furthest = activeRungs(vendor, granted).rungs.at(-1);
  // Where the broker trades, for the brokers whose registry entry says so.
  // Written here rather than read off the vendor's paragraph, which is the
  // agent's and in English.
  const coverageKey = `plugins.brokerages.coverage.${vendor.name}`;
  const reachKey = furthest && `plugins.brokerages.card.reach.${furthest.key}`;
  let ordersNote: string | null = null;
  if (orders.length === 0) ordersNote = t('plugins.brokerages.card.noOrders');
  else if (!furthest) ordersNote = t('plugins.brokerages.card.noOrdersGranted');
  else if (reachKey && i18n.exists(reachKey)) ordersNote = t(reachKey);
  else if (furthest) ordersNote = t(`plugins.brokerages.capabilities.${furthest.key}.desc`);
  return (
    <dl className="broker-caps">
      {i18n.exists(coverageKey) && (
        <div className="broker-caps-row">
          <dt>{t('plugins.brokerages.card.covers')}</dt>
          <dd className="broker-caps-text">{t(coverageKey)}</dd>
        </div>
      )}
      {access.length > 0 && (
        <div className="broker-caps-row">
          <dt>{t('plugins.brokerages.card.access')}</dt>
          <dd className="broker-caps-chips">
            {access.map((group) => (
              <CapabilityChip key={group.key} group={group} granted={granted} />
            ))}
          </dd>
        </div>
      )}
      <div className="broker-caps-row">
        <dt>{t('plugins.brokerages.card.orders')}</dt>
        <dd>
          {orders.length > 0 && (
            <span className="broker-caps-chips">
              {orders.map((group, i) => (
                <Fragment key={group.key}>
                  {i > 0 && <ChevronRight className="broker-caps-step" aria-hidden />}
                  <CapabilityChip group={group} granted={granted} />
                </Fragment>
              ))}
            </span>
          )}
          {ordersNote && (
            <p className={cn('broker-caps-note', orders.length === 0 && 'is-alone')}>
              {ordersNote}
            </p>
          )}
        </dd>
      </div>
    </dl>
  );
}

function CapabilityChip({
  group,
  granted,
}: {
  group: CapabilityGroup;
  granted: string[] | null;
}) {
  const { t } = useTranslation();
  const settled = granted != null;
  const on = !settled || granted.includes(group.key);
  const state = t(
    !settled
      ? 'plugins.brokerages.detail.offered'
      : on
        ? 'plugins.brokerages.detail.granted'
        : 'plugins.brokerages.detail.declined',
  );
  const money = group.tone === 'danger';
  const Icon = on ? Check : Minus;
  return (
    <span
      className={cn(
        'broker-cap',
        money && 'is-money',
        settled && !on && 'is-declined',
        // Amber only where it is both real and granted, as on the detail page.
        settled && on && money && 'is-live',
      )}
    >
      {settled ? (
        <Icon className="broker-cap-glyph" role="img" aria-label={state} />
      ) : (
        // The step that spends real money, findable across the cards at a
        // glance; neutral ink, since nothing is granted yet.
        money && <Banknote className="broker-cap-glyph" aria-hidden />
      )}
      {t(`plugins.brokerages.capabilities.${group.key}.label`)}
    </span>
  );
}

/**
 * What a live connection may do about orders, in the consent dialog's own
 * words. Real-money orders say so plainly; a step short of them names the
 * furthest one granted (a staged or paper order is not nothing, and "read-only"
 * would be a claim it is), and a grant with no order step at all is read-only.
 */
function GrantState({ vendor, granted }: { vendor: Brokerage | null; granted: string[] }) {
  const { t } = useTranslation();
  const { rungs } = activeRungs(vendor, granted);
  const live = rungs.some((group) => group.tone === 'danger');
  const furthest = rungs[rungs.length - 1];
  const label = live
    ? t('plugins.shell.brokerage.canPlaceOrders')
    : furthest
      ? t(`plugins.brokerages.capabilities.${furthest.key}.label`)
      : t('plugins.shell.brokerage.readOnly');
  return <span className={live ? 'brokerage-grant is-live' : 'brokerage-grant'}>{label}</span>;
}
