import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, Layers, Pencil, Power, Trash2 } from 'lucide-react';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { Loader } from '@/components/ui/loader';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { oauthLabelKey } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import { probeStillLanding } from '@/pages/ChatAgent/components/mcp/mcpState';
import type { BuiltinMcpServer, CatalogServer } from '@/pages/ChatAgent/utils/api';
import { brokerageArt, mcpServerArt } from '@/lib/brandArt';
import { useNow } from '@/hooks/useNow';
import { connectBlock, type Brokerage } from '../../brokerages';
import { useHomeOption } from '../../hooks/useWorkspaceOptions';
import { connectorState, switchOnBlock } from '../../utils/connectorState';
import { builtinServerMark } from '../../utils/pluginSurface';
import { isEffectivelyEnabled, isPluginOwned } from '../../utils/provenance';
import { secretEditHref, secretSetupHref } from '../../utils/secretParam';
import { BLOCKED_NOTE_PREFIX, ConnectButton, OauthMenuItems } from '../OauthRowParts';
import type { ScopeWorkspace } from '../ScopeControl';
import { partialReachText } from '../pluginsPage/pluginReach';
import { CellAction, CellMenu, CellState, SurfaceCell } from '../surface/Surface';
import { capitalizeFirst } from '../../utils/sentence';

/**
 * A connector as one cell: logo, name, one line, and the trailing state that
 * is the cell's only state signal. The state comes from `connectorState`, the
 * same rule the rail's dots and the page summary read, so the three never
 * disagree. The menu is the caller's: the Connectors grid passes
 * `ConnectorMenu`, and a plugin's page passes none, since switching one of its
 * connectors is that connector's own page's business.
 */
export function ConnectorCell({
  server,
  vendor,
  registryUnavailable = false,
  workspaces,
  workspacesLoading,
  connecting,
  refreshing = false,
  onOpen,
  onConnect,
  menu,
}: {
  server: CatalogServer;
  /** The shipped brokerage this row's URL resolves to, `null` for none and
   *  `undefined` while the registry is unanswered, which holds Connect. */
  vendor: Brokerage | null | undefined;
  /** The registry was asked and failed; the list carries the note. */
  registryUnavailable?: boolean;
  workspaces: ScopeWorkspace[];
  workspacesLoading: boolean;
  connecting: boolean;
  refreshing?: boolean;
  onOpen: () => void;
  onConnect: () => void;
  menu?: ReactNode;
}) {
  const { t } = useTranslation();
  const home = useHomeOption();
  const state = connectorState(server);
  const now = useNow(5_000, (at) => probeStillLanding(server, at));
  const checking = probeStillLanding(server, now);
  const blocked = connectBlock(vendor);
  const fix = state.kind === 'failing' ? state.fix : null;

  let line: ReactNode = server.description || null;
  let lineTone: 'default' | 'warn' = 'default';
  const broken = state.kind === 'connect' ? oauthLabelKey(state.status) : null;
  const blockedLine =
    state.kind === 'connect' && (blocked === 'native-only' || blocked === 'shell-outdated');
  if (blockedLine) {
    // The reason Connect is held, under the id the button points at.
    line = (
      <span id={`${BLOCKED_NOTE_PREFIX}${server.name}`}>
        {t(blocked === 'native-only' ? 'plugins.oauth.nativeOnlyNote' : 'plugins.oauth.shellOutdatedNote')}
      </span>
    );
  } else if (broken) {
    line = t(broken);
  } else if (state.kind === 'failing') {
    lineTone = 'warn';
    line = (
      <>
        {state.wire ?? capitalizeFirst(t(state.noteKey))}
        {state.keys.length > 0 && (
          <span className="connector-line-code"> · {state.keys.join(', ')}</span>
        )}
      </>
    );
  } else if (state.kind === 'connect') {
    line =
      server.probe?.verdict === 'oauth'
        ? t('plugins.connectorsPage.lineSignIn')
        : server.description || t('plugins.connectorsPage.lineNotChecked');
  } else if (!line) {
    // Nothing of its own to say: what it is, in the words a list of hand-made
    // servers tells apart, instead of an empty second line.
    const count = server.tool_count;
    line = [
      typeof count === 'number' && count > 0 ? t('mcp.row.toolCount', { count }) : null,
      server.transport,
    ]
      .filter(Boolean)
      .join(' · ');
  }

  const meta = checking
    ? t('plugins.connectorsPage.lineChecking')
    : state.kind === 'on' && !workspacesLoading
      ? partialReachText(t, server, workspaces, home)
      : null;

  const trailing =
    state.kind === 'connect' ? (
      // The row's own connect button, drawn as the cell's action: the vendor
      // gate, the focusable refusal and the note it points at all come with it.
      <ConnectButton
        status={state.status}
        connecting={connecting}
        vendor={vendor}
        rowKey={server.name}
        size="cell"
        registryUnavailable={registryUnavailable}
        testid={`catalog-connect-${server.name}`}
        onClick={onConnect}
      />
    ) : fix ? (
      // The fix is a key, so the cell offers it rather than a warning the
      // user has to open to act on; the line already says what is wrong.
      <CellAction
        to={fix.kind === 'add' ? secretSetupHref(fix.key) : secretEditHref(fix.key)}
        icon={<KeyRound className="h-3.5 w-3.5" aria-hidden />}
        testId={`connector-key-${server.name}`}
      >
        {fix.kind === 'add' ? t('plugins.surface.addKey') : t('plugins.surface.replaceKey')}
      </CellAction>
    ) : state.kind === 'failing' ? (
      <CellState state="warn" label={capitalizeFirst(t(state.noteKey))} />
    ) : state.kind === 'off' ? (
      <CellState state="off" />
    ) : refreshing ? (
      <Loader size={14} className="text-current" />
    ) : (
      <CellState state="on" />
    );

  return (
    <SurfaceCell
      mark={
        <BrandMark
          name={server.name}
          kind="server"
          size="md"
          art={brokerageArt(vendor) ?? mcpServerArt(server)}
        />
      }
      name={server.name}
      meta={meta}
      line={line}
      lineTone={lineTone}
      off={state.kind === 'off'}
      trailing={trailing}
      onOpen={onOpen}
      menu={menu}
    />
  );
}

/**
 * The grid's menu for a connector: on/off, workspaces, edit, the OAuth verbs
 * and removal. A switch-on the connection holds stays listed with the reason.
 * Switch off is never held: a row a check refused after it was switched on is
 * still sent to every workspace until it is switched off.
 */
export function ConnectorMenu({
  server,
  toggling,
  onOpen,
  onToggle,
  onEdit,
  onDisconnect,
  onRefreshSchemas,
  onRemove,
}: {
  server: CatalogServer;
  toggling: boolean;
  onOpen: () => void;
  onToggle: (enabled: boolean) => void;
  onEdit: () => void;
  onDisconnect: () => void;
  onRefreshSchemas: () => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const block = switchOnBlock(connectorState(server));
  const owned = isPluginOwned(server);
  return (
    <CellMenu label={t('mcp.row.actionsAria', { name: server.name })}>
      {server.enabled ? (
        <DropdownMenuItem disabled={toggling} onSelect={() => onToggle(false)}>
          <Power className="h-3.5 w-3.5 mr-2" />
          {t('plugins.connectorsPage.switchOff')}
        </DropdownMenuItem>
      ) : (
        <DropdownMenuItem disabled={toggling || block !== null} onSelect={() => onToggle(true)}>
          <Power className="h-3.5 w-3.5 mr-2" />
          <span className="flex flex-col">
            <span>{t('plugins.connectorsPage.switchOn')}</span>
            {block && (
              <span className="connector-menu-hint">
                {block === 'connect'
                  ? t('plugins.connectorsPage.switchOnNeedsConnect')
                  : t('plugins.connectorsPage.switchOnNeedsFix')}
              </span>
            )}
          </span>
        </DropdownMenuItem>
      )}
      <DropdownMenuItem onSelect={onOpen}>
        <Layers className="h-3.5 w-3.5 mr-2" />
        {t('plugins.connectorsPage.workspaces')}
      </DropdownMenuItem>
      {/* Editing a plugin's row detaches it from the plugin: the save is the
          same PUT, and the backend's detach warning comes back as a toast. */}
      <DropdownMenuItem onSelect={onEdit}>
        <Pencil className="h-3.5 w-3.5 mr-2" />
        <span className="flex flex-col">
          <span>{t('plugins.connectorsPage.edit')}</span>
          {owned && (
            <span className="connector-menu-hint">
              {t('plugins.connectorsPage.editDetaches', { plugin: server.plugin_name })}
            </span>
          )}
        </span>
      </DropdownMenuItem>
      <OauthMenuItems
        status={server.transport === 'http' ? (server.oauth_status ?? null) : null}
        onRefreshSchemas={onRefreshSchemas}
        onDisconnect={onDisconnect}
      />
      {/* A plugin's row goes with its plugin: uninstalling it, or Edit,
          which detaches it, are the two ways it leaves. */}
      {!owned && (
        <>
          <DropdownMenuSeparator />
          <DropdownMenuItem variant="destructive" onSelect={onRemove}>
            <Trash2 className="h-3.5 w-3.5 mr-2" />
            {t('plugins.connectorsPage.remove')}
          </DropdownMenuItem>
        </>
      )}
    </CellMenu>
  );
}

/**
 * A server a bundled plugin ships. Always connected (it runs beside the app),
 * so its state is on or off; it is switched off, never removed. The menu is
 * the caller's, as on `ConnectorCell`.
 */
export function BuiltinConnectorCell({
  server,
  workspaces,
  workspacesLoading,
  busy = false,
  onOpen,
  menu,
}: {
  server: BuiltinMcpServer;
  workspaces: ScopeWorkspace[];
  workspacesLoading: boolean;
  busy?: boolean;
  onOpen: () => void;
  menu?: ReactNode;
}) {
  const { t } = useTranslation();
  const on = isEffectivelyEnabled(server);
  return (
    <SurfaceCell
      mark={<BrandMark name={server.name} size="md" {...builtinServerMark(server)} />}
      name={server.name}
      meta={workspacesLoading ? null : partialReachText(t, server, workspaces)}
      line={server.description || server.plugin_name}
      off={!on}
      trailing={busy ? <Loader size={14} className="text-current" /> : <CellState state={on ? 'on' : 'off'} />}
      onOpen={onOpen}
      menu={menu}
    />
  );
}

/** The grid's menu for a built-in connector: on/off and workspaces. */
export function BuiltinConnectorMenu({
  server,
  busy,
  onOpen,
  onToggle,
}: {
  server: BuiltinMcpServer;
  busy: boolean;
  onOpen: () => void;
  onToggle: (enabled: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <CellMenu label={t('mcp.row.actionsAria', { name: server.name })}>
      <DropdownMenuItem disabled={busy} onSelect={() => onToggle(!server.enabled)}>
        <Power className="h-3.5 w-3.5 mr-2" />
        {server.enabled
          ? t('plugins.connectorsPage.switchOff')
          : t('plugins.connectorsPage.switchOn')}
      </DropdownMenuItem>
      <DropdownMenuItem onSelect={onOpen}>
        <Layers className="h-3.5 w-3.5 mr-2" />
        {t('plugins.connectorsPage.workspaces')}
      </DropdownMenuItem>
    </CellMenu>
  );
}
