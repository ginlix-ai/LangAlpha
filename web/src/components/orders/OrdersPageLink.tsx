import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useInRouterContext } from 'react-router-dom';
import { ArrowUpRight } from 'lucide-react';

/**
 * The one way off a card in a thread: the same attempt on the Orders page,
 * opened in its detail overlay.
 *
 * Stops its click at the link so a transcript surface that wraps the block can
 * never read it as a click on the card. Falls back to a plain anchor outside a
 * router, because a thread also renders where there is none.
 */
export function OrdersPageLink({ attemptId }: { attemptId: string }): React.ReactElement {
  const { t } = useTranslation();
  const inRouter = useInRouterContext();
  const href = `/orders?detail=order:${encodeURIComponent(attemptId)}`;
  const className = 'inline-flex items-center gap-1 text-xs hover:underline';
  const style = { color: 'var(--color-text-tertiary)' };
  const label = (
    <>
      {t('toolArtifact.directTool.orderReceipt.viewInOrders')}
      <ArrowUpRight className="w-3 h-3" aria-hidden="true" />
    </>
  );
  return (
    <div className="shrink-0 pb-0.5">
      {inRouter ? (
        <Link to={href} className={className} style={style} onClick={(e) => e.stopPropagation()}>
          {label}
        </Link>
      ) : (
        <a href={href} className={className} style={style} onClick={(e) => e.stopPropagation()}>
          {label}
        </a>
      )}
    </div>
  );
}
