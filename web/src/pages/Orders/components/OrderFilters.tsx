import React, { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useBrokerages } from '@/hooks/useMcpServers';
import {
  ORDER_ASSET_CLASSES,
  ORDER_STATUS_GROUPS,
  type OrderAssetClass,
  type OrderFilters as Filters,
  type OrderStatusGroup,
} from '@/pages/ChatAgent/utils/api';
import { ORDER_MODE_KEY, ORDER_MODES } from '@/components/orders/mode';
import type { OrderMode } from '@/types/orders';
import { ORDER_ASSET_CLASS_KEY, ORDER_STATUS_GROUP_KEY } from '../utils/labels';

function Select({
  label,
  value,
  onChange,
  children,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  children: React.ReactNode;
}) {
  const id = useId();
  return (
    <div className="flex flex-col gap-1 min-w-[8rem]">
      <label
        htmlFor={id}
        className="text-[0.625rem] font-medium uppercase tracking-[0.14em]"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {label}
      </label>
      <select
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="text-sm px-2 py-1.5 rounded-md"
        style={{
          color: 'var(--color-text-primary)',
          backgroundColor: 'var(--color-bg-input)',
          border: '1px solid var(--color-border-muted)',
        }}
      >
        {children}
      </select>
    </div>
  );
}

/**
 * The four facets the server pages on. Every one of them is a server filter,
 * not a view over the loaded rows, so an empty result here means the ledger
 * has nothing rather than that this page does.
 */
export function OrderFilters({
  filters,
  onChange,
}: {
  filters: Filters;
  onChange: (next: Filters) => void;
}) {
  const { t } = useTranslation();
  const { data: brokerages } = useBrokerages();
  const all = t('orders.filter.all');
  const active =
    !!filters.vendor || !!filters.mode || !!filters.status || !!filters.asset_class;

  return (
    <div className="flex flex-wrap items-end gap-3">
      <Select
        label={t('orders.filter.vendor')}
        value={filters.vendor ?? ''}
        onChange={(vendor) => onChange({ ...filters, vendor: vendor || null })}
      >
        <option value="">{all}</option>
        {(brokerages ?? []).map((b) => (
          <option key={b.name} value={b.name}>
            {b.label}
          </option>
        ))}
      </Select>

      <Select
        label={t('orders.filter.mode')}
        value={filters.mode ?? ''}
        onChange={(mode) =>
          onChange({ ...filters, mode: (mode as OrderMode) || null })
        }
      >
        <option value="">{all}</option>
        {ORDER_MODES.map((mode) => (
          <option key={mode} value={mode}>
            {t(ORDER_MODE_KEY[mode])}
          </option>
        ))}
      </Select>

      <Select
        label={t('orders.filter.status')}
        value={filters.status ?? ''}
        onChange={(status) =>
          onChange({ ...filters, status: (status as OrderStatusGroup) || null })
        }
      >
        <option value="">{all}</option>
        {ORDER_STATUS_GROUPS.map((group) => (
          <option key={group} value={group}>
            {t(ORDER_STATUS_GROUP_KEY[group])}
          </option>
        ))}
      </Select>

      <Select
        label={t('orders.filter.assetClass')}
        value={filters.asset_class ?? ''}
        onChange={(assetClass) =>
          onChange({
            ...filters,
            asset_class: (assetClass as OrderAssetClass) || null,
          })
        }
      >
        <option value="">{all}</option>
        {ORDER_ASSET_CLASSES.map((assetClass) => (
          <option key={assetClass} value={assetClass}>
            {t(ORDER_ASSET_CLASS_KEY[assetClass])}
          </option>
        ))}
      </Select>

      {active && (
        <HeaderButton variant="ghost" icon={X} onClick={() => onChange({})}>
          {t('orders.filter.clear')}
        </HeaderButton>
      )}
    </div>
  );
}
