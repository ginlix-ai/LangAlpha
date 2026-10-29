import React from 'react';
import { maskAccountIdsDeep, maskedAccountValue } from '../../utils/directTools';

function renderValue(value: unknown): string {
  if (value == null) return String(value);
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(maskAccountIdsDeep(value));
  } catch {
    return String(value);
  }
}

/**
 * A zero where an id belongs is the vendor's placeholder, not an id.
 *
 * IBKR's order frame carries `contract_id: 0` beside the `contract_id_ex` that
 * actually addresses the instrument, and a reader comparing the frame against
 * the broker's own app has to be able to tell which field named the contract.
 * The key is what decides it, never the value: a quantity of zero is a real
 * quantity, and a price of zero is a real price.
 */
function isBlank(key: string, value: unknown): boolean {
  if (value === null || value === '') return true;
  return /(_id|Id)$/.test(key) && (value === 0 || value === '0');
}

/**
 * The arguments of a direct tool call as a key/value list: the frame the vendor
 * sees, field for field.
 *
 * Every field but one, that is. The account id is masked at whatever depth and
 * under whichever spelling it arrives, by the same key set the summary card
 * above it uses: an unmasked account number here made the card's mask
 * decorative, since both are on screen at once.
 *
 * `hideEmpty` drops the fields the caller left null, and the zero ids a vendor
 * asks for and does not use. It is off by default, because on most tools an
 * argument explicitly sent as null is part of what was sent; an order form that
 * carries every optional leg of every order type is the case where those rows
 * are only noise around the ones that decided a trade.
 */
export function ArgsTable({
  args,
  emptyLabel,
  hideEmpty = false,
}: {
  args: Record<string, unknown>;
  emptyLabel: string;
  hideEmpty?: boolean;
}): React.ReactElement {
  const entries = Object.entries(args).filter(
    ([k, v]) => v !== undefined && !(hideEmpty && isBlank(k, v)),
  );
  if (entries.length === 0) {
    return <div className="text-xs" style={{ color: 'var(--color-text-quaternary)' }}>{emptyLabel}</div>;
  }
  return (
    <dl className="grid gap-x-4 gap-y-1 text-xs" style={{ gridTemplateColumns: 'max-content minmax(0, 1fr)' }}>
      {entries.map(([k, v]) => (
        <React.Fragment key={k}>
          <dt className="font-mono whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>{k}</dt>
          <dd className="font-mono break-all min-w-0" style={{ color: 'var(--color-text-primary)' }}>
            {maskedAccountValue(k, v) ?? renderValue(v)}
          </dd>
        </React.Fragment>
      ))}
    </dl>
  );
}
