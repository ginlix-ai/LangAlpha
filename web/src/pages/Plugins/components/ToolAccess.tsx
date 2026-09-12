import { useTranslation } from 'react-i18next';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { EnabledToggle } from '@/components/mcp/McpPrimitives';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import {
  orderApprovalOf,
  type CatalogServer,
  type McpOrderMode,
  type McpServerBindingPatch,
  type McpToolBinding,
  type McpToolSummary,
} from '@/pages/ChatAgent/utils/api';
import { bindingOptions, permitsBinding } from './toolSelection';

/**
 * How a server's tools reach the model, and the row-wide switches a broker
 * gets on top of that. Precedence is the whole reason this is two surfaces:
 * a tool's own override beats the row preset, which beats the group default,
 * so the control that sets the override sits on the tool and the preset up
 * here says what the rest fall back to.
 */

/** Loudest first, so the switch that spends real money is the one on top. The
 *  server answers which modes a vendor has, not what order to read them in. */
const ORDER_MODE_LADDER: readonly McpOrderMode[] = ['live', 'paper', 'staged'];

/**
 * A switch per kind of order, written out rather than built from the mode, so
 * the tree-wide locale sweep can see every key and a mode added later cannot
 * ship as a raw key beside a switch that decides whether an agent may spend
 * real money. The badge naming the mode is `OrderModeBadge`, shared with the
 * chat card that answers a stopped order.
 */
const ORDER_COPY: Record<McpOrderMode, { label: string; desc: string }> = {
  live: {
    label: 'plugins.detail.orderApprovalLive',
    desc: 'plugins.detail.orderApprovalLiveDesc',
  },
  paper: {
    label: 'plugins.detail.orderApprovalPaper',
    desc: 'plugins.detail.orderApprovalPaperDesc',
  },
  staged: {
    label: 'plugins.detail.orderApprovalStaged',
    desc: 'plugins.detail.orderApprovalStagedDesc',
  },
};

/**
 * The row-wide switches. `binding_preset` has one non-null value, `ptc_only`,
 * which sends everything the row is allowed to move through the sandbox;
 * null leaves each group's own default in force, and for a group that
 * supports direct calls that default is direct, so anything other than
 * `ptc_only` reads as the switch being on. A tool the server pins, which for
 * an order tool is to direct, stays where it is under either setting, so
 * the switch names what it reaches rather than promising to move everything.
 *
 * The gates below it are orthogonal to that one: they decide whether an order
 * stops for the user, not where the call runs. One per kind of order the
 * connection has, because the three cost different things and a single switch
 * priced them all at whichever one the user was thinking of. A vendor with no
 * order tool gets none of them.
 */
export function ToolAccessSwitches({
  catalog,
  orderModes,
  busy,
  onPatch,
}: {
  catalog: CatalogServer;
  /** The kinds of order this vendor has, as the server answers it. Off the
   *  vendor's curation and not the tool snapshot, so a row discovery has not
   *  reached yet still offers the gates that will govern its orders. */
  orderModes: McpOrderMode[] | undefined;
  busy: boolean;
  onPatch: (body: McpServerBindingPatch) => void;
}) {
  const { t } = useTranslation();
  const groupDefaults = catalog.binding_preset !== 'ptc_only';
  const approval = orderApprovalOf(catalog.order_approval);
  const gates = ORDER_MODE_LADDER.filter((mode) => orderModes?.includes(mode));
  return (
    <ul className="flex flex-col gap-3">
      <SwitchRow
        label={t('plugins.detail.groupDefaults')}
        desc={t('plugins.detail.groupDefaultsDesc')}
        enabled={groupDefaults}
        disabled={busy}
        onToggle={() => onPatch({ binding_preset: groupDefaults ? 'ptc_only' : null })}
      />
      {gates.map((mode) => (
        <SwitchRow
          key={mode}
          label={t(ORDER_COPY[mode].label)}
          desc={t(ORDER_COPY[mode].desc)}
          enabled={approval[mode]}
          disabled={busy}
          // Only the mode the user touched travels. The server merges it, so a
          // second tab holding an older map cannot write back the other two.
          onToggle={() => onPatch({ order_approval: { [mode]: !approval[mode] } })}
        />
      ))}
    </ul>
  );
}

function SwitchRow({
  label,
  desc,
  enabled,
  disabled,
  onToggle,
}: {
  label: string;
  desc: string;
  enabled: boolean;
  disabled: boolean;
  onToggle: () => void;
}) {
  return (
    <li className="flex items-start justify-between gap-3">
      <div className="min-w-0">
        <p className="text-xs font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {label}
        </p>
        <p className="text-[0.6875rem] mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
          {desc}
        </p>
      </div>
      <div className="flex-shrink-0 pt-0.5">
        <EnabledToggle enabled={enabled} name={label} disabled={disabled} onToggle={onToggle} />
      </div>
    </li>
  );
}

/**
 * One tool's binding, as the compact control at the right edge of its row.
 * Writes the override every time: the server owns the precedence, so the
 * client does not guess what the row would fall back to. Reset is the one
 * way back, and it removes the key rather than writing a matching value.
 */
export function ToolBindingControl({
  tool,
  busy,
  error,
  onPatch,
}: {
  tool: McpToolSummary;
  busy: boolean;
  /** The server's refusal of the last change to this tool, verbatim. */
  error?: string | null;
  onPatch: (body: McpServerBindingPatch) => void;
}) {
  const { t } = useTranslation();
  const overridden = tool.binding_source === 'override';
  // A pinned tool (an order tool, which the server holds to direct) gets
  // a 422 for any other value. Say so on the row rather than let the user
  // find out by picking one. Which values it takes comes from `allowed`, not
  // from the pin: an absent list is an unrestricted tool, never a locked one.
  const pinned = tool.binding_source === 'policy';
  const value = tool.binding ?? 'ptc';

  // Only this tool travels. A stored key that is another spelling of the same
  // name is dropped server-side, which reads the map folded anyway, so the
  // page never has to carry the map to write one switch.
  function write(next: McpToolBinding) {
    onPatch({ tool_binding_set: { [tool.name]: next } });
  }
  function reset() {
    onPatch({ tool_binding_unset: [tool.name] });
  }

  return (
    <div className="flex flex-col items-end gap-0.5 flex-shrink-0">
      <div className="flex items-center gap-1.5">
        {pinned && (
          <span
            className="text-[0.625rem] text-right"
            style={{ color: 'var(--color-text-quaternary)' }}
          >
            {t('plugins.detail.bindingPinned')}
          </span>
        )}
        <OrderModeBadge mode={tool.order?.mode} />
        {tool.approval && (
          <span
            className="text-[0.625rem]"
            title={t('plugins.detail.asksFirstTitle')}
            style={{ color: 'var(--color-text-quaternary)' }}
          >
            {t('plugins.detail.asksFirst')}
          </span>
        )}
        {overridden && (
          <>
            <span className="text-[0.625rem]" style={{ color: 'var(--color-text-quaternary)' }}>
              {t('plugins.detail.bindingCustom')}
            </span>
            <button
              type="button"
              disabled={busy}
              onClick={reset}
              aria-label={t('plugins.detail.bindingResetAria', { name: tool.name })}
              className="text-[0.625rem] hover:underline underline-offset-2 disabled:opacity-50"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('plugins.detail.bindingReset')}
            </button>
          </>
        )}
        <SegmentedControl
          size="compact"
          value={value}
          label={t('plugins.detail.bindingAria', { name: tool.name })}
          disabled={busy}
          options={bindingOptions(t, (b) => permitsBinding(tool, b))}
          onChange={write}
        />
      </div>
      {error && (
        <span role="alert" className="text-[0.625rem] text-right" style={{ color: 'var(--color-loss)' }}>
          {error}
        </span>
      )}
    </div>
  );
}
