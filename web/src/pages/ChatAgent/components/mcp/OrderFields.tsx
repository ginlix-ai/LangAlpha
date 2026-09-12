import React from 'react';
import { useTranslation } from 'react-i18next';
import type { OrderSummaryRow } from './orderSummary';

/**
 * What an amend acts on, said next to the verb that acts on it.
 *
 * A cancel or a confirm carries almost none of the order fields below, so
 * without this the card asks a person to approve cancelling nothing in
 * particular. It rides the action line rather than the field list because it
 * is the subject of the sentence the card's first line already starts, and it
 * is drawn whole: this is the string the user matches against the order in the
 * broker's own app, and an elided one cannot be matched.
 */
export function OrderTargetRef({
  targetRef,
}: {
  targetRef?: string | null;
}): React.ReactElement | null {
  const value = typeof targetRef === 'string' ? targetRef.trim() : '';
  if (!value) return null;
  return (
    <span
      className="text-sm font-medium font-mono break-all min-w-0"
      style={{ color: 'var(--color-text-primary)' }}
      data-testid="order-target-ref"
    >
      {value}
    </span>
  );
}

/** Label/value pairs on one grid, so the approval card and the receipt draw an
 *  order's fields identically. */
export function OrderFieldList({ rows }: { rows: OrderSummaryRow[] }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <dl className="grid gap-x-4 gap-y-1 text-xs" style={{ gridTemplateColumns: 'max-content minmax(0, 1fr)' }}>
      {rows.map((row) => (
        <React.Fragment key={row.field}>
          <dt className="whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>
            {t(row.labelKey)}
          </dt>
          <dd className="break-all min-w-0" style={{ color: 'var(--color-text-primary)' }}>
            {row.valueKey ? t(row.valueKey, row.valueParams) : row.value}
          </dd>
        </React.Fragment>
      ))}
    </dl>
  );
}
