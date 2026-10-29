import { useTranslation } from 'react-i18next';
import { Download, KeyRound, Power, RefreshCw, Trash2 } from 'lucide-react';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { McpSignInNeeded, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { hasLifecycle, pluginMark } from '../utils/pluginSurface';
import { secretEditHref, secretSetupHref } from '../utils/secretParam';
import { CellAction, CellMenu, CellState, SurfaceCell } from './surface/Surface';
import { partsLabel, statusLine, type PluginStatus } from './pluginsPage/pluginStatus';
import { usePluginLifecycle } from './pluginsPage/usePluginLifecycle';
import './pluginsPage/pluginsPage.css';

/**
 * One plugin as a cell: its logo, what it carries, one line (its description,
 * the problem when there is one, or else a newer version in the catalog) and
 * the state in the trailing slot. No switch on the cell; switching, updating,
 * exporting and uninstalling live in its menu, and the page it opens carries
 * the full controls.
 *
 * A switched-off plugin reads as Off even when a connector of its own wants a
 * sign-in: nothing reaches the agent from it until it is back on, so there is
 * nothing to fix yet.
 */
export function PluginCard({
  plugin,
  status,
  onOpen,
  onConnect,
  connecting = false,
  onSignInNeeded,
}: {
  plugin: PluginInfo;
  status: PluginStatus;
  onOpen: () => void;
  /** Start the sign-in the problem connector is waiting on. */
  onConnect?: () => void;
  connecting?: boolean;
  /** The switch was refused until these servers are connected. */
  onSignInNeeded?: (servers: McpSignInNeeded[]) => void;
}) {
  const { t } = useTranslation();
  const life = usePluginLifecycle(plugin, { onSignInNeeded });
  const line = statusLine(t, plugin, status, life.updateAvailable);
  const installed = hasLifecycle(plugin);

  if (life.confirmingUninstall) {
    return (
      <div className="pp-confirm">
        <ConfirmStrip
          message={t('plugins.pluginsPage.uninstallAsk', { plugin: plugin.name })}
          confirmLabel={life.uninstalling ? t('common.loading') : t('plugins.card.uninstallConfirmYes')}
          cancelLabel={t('plugins.card.uninstallConfirmNo')}
          pending={life.uninstalling}
          onConfirm={() => void life.uninstall()}
          onCancel={life.cancelUninstall}
        />
      </div>
    );
  }

  // A key the plugin's connector is missing, or one its server refused, is
  // fixed in Secrets: the cell links there rather than only warning.
  const { fix } = status;
  let trailing;
  if (fix?.kind === 'addKey' || fix?.kind === 'replaceKey') {
    trailing = (
      <CellAction
        to={
          fix.kind === 'addKey'
            ? secretSetupHref(fix.key)
            : fix.key
              ? secretEditHref(fix.key)
              : '/plugins?tab=secrets'
        }
        icon={<KeyRound className="h-3.5 w-3.5" aria-hidden />}
      >
        {fix.kind === 'addKey' ? t('plugins.surface.addKey') : t('plugins.surface.replaceKey')}
      </CellAction>
    );
  } else if (fix?.kind === 'signIn' && onConnect) {
    trailing = (
      <CellAction onClick={onConnect} disabled={connecting}>
        {t('plugins.oauth.connect')}
      </CellAction>
    );
  } else if (fix?.kind === 'open') {
    trailing = <CellState state="warn" label={line.text} />;
  } else {
    trailing = <CellState state={plugin.enabled ? 'on' : 'off'} />;
  }

  return (
    <>
      <SurfaceCell
        mark={<BrandMark name={plugin.name} {...pluginMark(plugin)} size="md" />}
        name={plugin.name}
        meta={partsLabel(t, plugin)}
        line={line.text}
        lineTone={line.warn ? 'warn' : 'default'}
        off={!plugin.enabled}
        onOpen={onOpen}
        trailing={trailing}
        menu={
          <CellMenu label={t('mcp.row.actionsAria', { name: plugin.name })}>
            <DropdownMenuItem onSelect={() => void life.toggle()} disabled={life.toggling}>
              <Power className="h-3.5 w-3.5" />
              {plugin.enabled ? t('plugins.pluginsPage.switchOff') : t('plugins.pluginsPage.switchOn')}
            </DropdownMenuItem>
            {installed ? (
              <>
                <DropdownMenuItem onSelect={life.update} disabled={life.busy}>
                  <RefreshCw className="h-3.5 w-3.5" />
                  {t('plugins.card.update')}
                </DropdownMenuItem>
                <DropdownMenuItem onSelect={() => void life.exportPackage()} disabled={life.exporting}>
                  <Download className="h-3.5 w-3.5" />
                  {t('plugins.card.export')}
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem variant="destructive" onSelect={life.requestUninstall}>
                  <Trash2 className="h-3.5 w-3.5" />
                  {t('plugins.pluginsPage.uninstall')}
                </DropdownMenuItem>
              </>
            ) : (
              <>
                <DropdownMenuSeparator />
                <p className="pp-menu-note">{t('plugins.pluginsPage.builtinNote')}</p>
              </>
            )}
          </CellMenu>
        }
      />
      {life.dialogs}
    </>
  );
}
