import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link, useInRouterContext, useNavigate } from 'react-router';
import { ArrowRight } from 'lucide-react';
import { useRouteLeaveGuard } from '@/pages/ChatAgent/contexts/RouteLeaveGuardContext';

/**
 * The one way off a card in a thread: the same attempt on the Orders page,
 * opened in its detail overlay. Its arrow points across rather than out,
 * because it stays in the app; the out-arrow is the vendor link's, which
 * leaves it.
 *
 * Stops its click at the link so a transcript surface that wraps the block can
 * never read it as a click on the card. Falls back to a plain anchor outside a
 * router, because a thread also renders where there is none. Inside one, the
 * route change goes through the host's leave guard, since a panel holding
 * unsaved drafts is otherwise unmounted with no question asked.
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
      <ArrowRight className="w-3 h-3" aria-hidden="true" />
    </>
  );
  return (
    <div className="shrink-0">
      {inRouter ? (
        <GuardedLink href={href} className={className} style={style}>
          {label}
        </GuardedLink>
      ) : (
        <a href={href} className={className} style={style} onClick={(e) => e.stopPropagation()}>
          {label}
        </a>
      )}
    </div>
  );
}

function GuardedLink({ href, className, style, children }: {
  href: string; className: string; style: React.CSSProperties; children: React.ReactNode;
}): React.ReactElement {
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  const onClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.stopPropagation();
    // Modified clicks open a new tab and leave nothing behind; the Link keeps those.
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.altKey || e.ctrlKey || e.shiftKey) return;
    e.preventDefault();
    guardLeave(() => navigate(href));
  };
  return (
    <Link to={href} className={className} style={style} onClick={onClick}>
      {children}
    </Link>
  );
}
