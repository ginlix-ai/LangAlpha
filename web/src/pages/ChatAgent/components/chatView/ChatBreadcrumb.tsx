import { ChevronRight } from 'lucide-react';
import { Fragment } from 'react';

export interface Crumb {
  label: string;
  /** Goes there; absent on the last one, which is the view on screen. */
  onOpen?: () => void;
}

/**
 * Where the chat on screen sits: its workspace, its thread, and the subagent
 * or workflow open in it. Each step but the last goes there. The thread's
 * name gives way first when the row runs short, since the workspace and the
 * agent open are the parts a glance looks for.
 */
export function ChatBreadcrumb({ crumbs }: { crumbs: Crumb[] }) {
  return (
    <span className="flex min-w-0 items-center gap-1.5">
      {crumbs.map((crumb, i) => {
        const last = i === crumbs.length - 1;
        const shrink = crumbs.length > 1 && i === 1 ? 'min-w-0 shrink' : 'min-w-0 shrink-0 max-w-[16rem]';
        return (
          <Fragment key={i}>
            {i > 0 && <ChevronRight className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} aria-hidden="true" />}
            {last || !crumb.onOpen ? (
              <span
                className={`${shrink} truncate`}
                aria-current={last ? 'page' : undefined}
                title={crumb.label}
              >
                {crumb.label}
              </span>
            ) : (
              <button
                type="button"
                onClick={crumb.onOpen}
                className={`${shrink} truncate rounded-sm text-left transition-colors hover:text-(--color-text-primary)`}
                style={{ color: 'var(--color-text-secondary)' }}
                title={crumb.label}
              >
                {crumb.label}
              </button>
            )}
          </Fragment>
        );
      })}
    </span>
  );
}
