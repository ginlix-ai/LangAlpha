import type { ReactNode } from 'react';
import type { TFunction } from 'i18next';
import { Link } from 'react-router';
import { Receipt, Trash2 } from 'lucide-react';

/**
 * The verbs at the foot of a connector's page, as a list so the page can
 * leave the footer out when there are none.
 */
export function serverDetailVerbs(
  t: TFunction,
  {
    brokerage,
    builtin,
    ordersVendor,
    reconnect,
    plugin,
    owned,
    onOpenPlugin,
    edit,
    onRemove,
  }: {
    brokerage: boolean;
    builtin: boolean;
    /** The vendor whose orders to link to, for a server that places them. */
    ordersVendor: string | null;
    /** A broker's Connect on a live connection: reconnecting is the only way
     *  to change what was granted, so the control that made the grant is the
     *  one that changes it. */
    reconnect: ReactNode;
    plugin: string | null;
    /** The row is its plugin's, which leaves with it and never on its own. */
    owned: boolean;
    onOpenPlugin: (plugin: string) => void;
    edit: ReactNode;
    onRemove?: () => void;
  },
): ReactNode[] {
  const verbs: ReactNode[] = [];
  const orders = ordersVendor && (
    <Link
      key="orders"
      to={`/orders?vendor=${encodeURIComponent(ordersVendor)}`}
      className="inline-flex items-center gap-1.5 text-xs hover:underline"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      <Receipt className="h-3 w-3" />
      {t('plugins.detail.viewOrders')}
    </Link>
  );
  if (brokerage && (orders || reconnect)) {
    verbs.push(
      <div key="broker" className="flex items-center justify-between gap-2">
        {orders || <span />}
        {reconnect}
      </div>,
    );
  } else if (orders) {
    verbs.push(orders);
  }
  if (owned && plugin) {
    verbs.push(
      <button key="plugin" type="button" className="connector-btn is-full" onClick={() => onOpenPlugin(plugin)}>
        {t('plugins.connectorsPage.openPluginButton', { plugin })}
      </button>,
      <p key="owned" className="connector-aside-note" style={{ marginTop: 0 }}>
        {t('plugins.connectorsPage.pluginOwnedNote', { plugin })}
      </p>,
    );
  }
  const remove = onRemove && !owned;
  if (edit || remove) {
    verbs.push(
      <div key="edit" className="connector-verbs-row">
        {edit}
        {remove && (
          <button type="button" className="connector-btn is-danger" onClick={onRemove}>
            <Trash2 className="h-3.5 w-3.5" />
            {t('plugins.connectorsPage.remove')}
          </button>
        )}
      </div>,
    );
  }
  // Editing a plugin's row is the same save as any other, and the save detaches
  // it from the plugin: said under the button, before it is pressed.
  if (owned && edit && plugin) {
    verbs.push(
      <p key="detach" className="connector-aside-note" style={{ marginTop: 0 }}>
        {t('plugins.connectorsPage.editDetaches', { plugin })}
      </p>,
    );
  }
  if (builtin) {
    verbs.push(
      <p key="builtin" className="connector-aside-note" style={{ marginTop: 0 }}>
        {t('plugins.connectorsPage.builtinNote')}
      </p>,
    );
  }
  return verbs;
}
