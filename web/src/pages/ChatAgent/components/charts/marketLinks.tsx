import React from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate, useParams } from 'react-router';
import { ArrowUpRight } from 'lucide-react';
import { isIndexListing } from '@/lib/bars';
import { readTypedTicker } from '@/lib/marketUtils';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import { useRouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import type { PanelProps } from './cardReaders';
import { QuoteSections } from './InlineQuoteCard';
import { TermLink } from './TermPanel';

/** Opens a symbol in Market View; the chat it left is offered as the way back. */
function useOpenInMarket(): (symbol: string) => void {
  const navigate = useNavigate();
  const params = useParams();
  const guardLeave = useRouteLeaveGuard();
  return (symbol) =>
    guardLeave(() => navigate(buildMarketViewUrl({ symbol, returnTo: params.threadId ? `/chat/t/${params.threadId}` : null })));
}

/**
 * A way into Market View: the panel's `OPEN IN MARKET VIEW ↗`, or with
 * `inline`, a table's symbol itself, which opens only when it reads as a
 * ticker and is plain text otherwise.
 */
export function MarketLink({ symbol, inline = false }: { symbol: string | undefined; inline?: boolean }): React.ReactElement | null {
  const { t } = useTranslation();
  const open = useOpenInMarket();
  const target = symbol && (inline ? readTypedTicker(symbol) : symbol);
  if (!target) return inline && symbol ? <>{symbol}</> : null;
  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    open(target);
  };
  if (!inline) return <TermLink onClick={handleClick}>{t('toolArtifact.openInMarketView')}</TermLink>;
  return (
    <button type="button" className="term-sym-link" title={t('toolArtifact.openInMarketView')} onClick={handleClick}>
      <span>{symbol}</span>
      <ArrowUpRight className="term-link-mark" aria-hidden="true" />
    </button>
  );
}

/**
 * A `get_quote` result in the panel: the card's frames a size up, each row a
 * way into Market View. An index row the vendor echoed without its caret
 * (`GSPC`) gets no link, since Market View would read the bare spelling as a
 * stock.
 */
export function QuotePanel({ data }: PanelProps): React.ReactElement {
  return (
    <QuoteSections
      artifact={data}
      link={(symbol, isIndex) => (isIndex && !isIndexListing(symbol) ? null : <MarketLink symbol={symbol} />)}
    />
  );
}
