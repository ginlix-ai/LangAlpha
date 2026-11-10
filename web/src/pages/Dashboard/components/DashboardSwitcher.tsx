import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation, useNavigate } from 'react-router';
import { AppWindow, Check, ChevronDown, LayoutGrid, PanelsTopLeft, Sparkles, X, type LucideProps } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { OPEN_SWITCHER, type SavedDashboard, type SavedDashboardsApi } from '@/lib/dashboards';
import { cn } from '@/lib/utils';
import { useDashboardAgent } from '../hooks/useDashboardAgent';

const BUILT_IN = '';

/** A page opens from the server's copy; an app is a window onto the user's computer; neither is the built-in layout. */
function DashboardIcon({ dashboard, ...props }: { dashboard: SavedDashboard | null } & LucideProps): React.ReactElement {
  if (!dashboard) return <LayoutGrid {...props} />;
  return dashboard.kind === 'page' ? <PanelsTopLeft {...props} /> : <AppWindow {...props} />;
}

interface DashboardSwitcherProps {
  dashboards: SavedDashboardsApi;
  /** The one already chosen was chosen again: the user is asking to see it now. */
  onReselect?: () => void;
}

/**
 * Which dashboard the page shows: the built-in layout or one the user set.
 * Switching never forgets one, so trying another costs nothing; removing one
 * only takes it off this list, with an Undo. Sent here to make room for
 * another (`OPEN_SWITCHER` in the route's state), it opens on arrival.
 */
export function DashboardSwitcher({ dashboards, onReselect }: DashboardSwitcherProps): React.ReactElement {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const agent = useDashboardAgent();
  const { list, current, select, remove } = dashboards;
  const nameOf = (title: string) => title || t('customDashboard.untitled');
  const label = current ? nameOf(current.title) : t('customDashboard.builtIn');

  // The ask rides the route, so the menu stays open across the switch from
  // one header to the other; closing it clears the ask, so a reload does not
  // open it again.
  const asked = (location.state as Record<string, unknown> | null)?.[OPEN_SWITCHER] === true;
  const [opened, setOpened] = useState(false);
  const onOpenChange = (open: boolean) => {
    setOpened(open);
    if (!open && asked) navigate(location.pathname, { replace: true, state: null });
  };

  const choose = (value: string) => {
    const code = value === BUILT_IN ? null : value;
    if (code === (current?.code ?? null)) onReselect?.();
    else select(code);
  };

  const mark = (on: boolean) => <Check className={cn('h-3.5 w-3.5 shrink-0', !on && 'invisible')} aria-hidden />;

  return (
    <DropdownMenu open={opened || asked} onOpenChange={onOpenChange}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="tap-target flex h-9 min-w-0 items-center gap-1.5 rounded-md px-2 text-sm transition-colors hover:bg-(--color-bg-hover)"
          style={{ color: 'var(--color-text-secondary)' }}
          aria-label={t('customDashboard.switchLabel', { name: label })}
          title={t('customDashboard.switchLabel', { name: label })}
        >
          <DashboardIcon dashboard={current} size={16} className="shrink-0" />
          <span className="hidden max-w-[14rem] truncate sm:inline">{label}</span>
          <ChevronDown size={14} className="shrink-0 opacity-70" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72">
        <DropdownMenuRadioGroup value={current?.code ?? BUILT_IN} onValueChange={choose}>
          <DropdownMenuRadioItem value={BUILT_IN}>
            {mark(!current)}
            <DashboardIcon dashboard={null} className="h-3.5 w-3.5 shrink-0" />
            <span className="truncate">{t('customDashboard.builtIn')}</span>
          </DropdownMenuRadioItem>
          {list.map((dashboard) => {
            const name = nameOf(dashboard.title);
            return (
              // The remove button is an item of its own beside the row, so it
              // takes one press, by pointer or arrow keys, without choosing it.
              <div key={dashboard.code} className="flex items-center gap-0.5">
                <DropdownMenuRadioItem value={dashboard.code} className="min-w-0 flex-1">
                  {mark(current?.code === dashboard.code)}
                  <DashboardIcon dashboard={dashboard} className="h-3.5 w-3.5 shrink-0" />
                  <span className="truncate">{name}</span>
                </DropdownMenuRadioItem>
                <DropdownMenuItem
                  onSelect={() => remove(dashboard.code)}
                  className="tap-target w-auto shrink-0 px-1.5"
                  style={{ color: 'var(--color-text-tertiary)' }}
                  aria-label={t('customDashboard.remove', { name })}
                  title={t('customDashboard.remove', { name })}
                >
                  <X className="h-3.5 w-3.5" aria-hidden />
                </DropdownMenuItem>
              </div>
            );
          })}
        </DropdownMenuRadioGroup>
        <DropdownMenuSeparator />
        {list.length === 0 && (
          <p className="px-2.5 pt-1 pb-1.5 text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('customDashboard.emptyHint')}
          </p>
        )}
        <DropdownMenuItem onSelect={agent.build}>
          <Sparkles className="h-3.5 w-3.5 shrink-0" />
          {t('customDashboard.build')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
