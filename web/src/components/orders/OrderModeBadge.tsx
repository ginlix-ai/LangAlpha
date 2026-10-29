import { useTranslation } from 'react-i18next';
import { TagBadge } from '@/components/mcp/McpPrimitives';
import { ORDER_MODE_KEY } from './mode';
import type { OrderMode } from '@/types/orders';

/**
 * The kind of order a tool acts on, said on its own row. Amber only for live:
 * it is the one that spends real money, and every surface that draws this
 * keeps its loudest ink for that.
 */
export function OrderModeBadge({ mode }: { mode: OrderMode | null | undefined }) {
  const { t } = useTranslation();
  if (!mode) return null;
  return (
    <TagBadge
      tone={mode === 'live' ? 'warning' : 'muted'}
      title={t('plugins.detail.orderModeTitle')}
    >
      {t(ORDER_MODE_KEY[mode])}
    </TagBadge>
  );
}
