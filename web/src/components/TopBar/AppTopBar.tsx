import { useEffect, useRef } from 'react';
import { Globe, Plus, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { NAV_ITEMS, SETTINGS_ITEM } from '@/components/nav/navItems';
import { useDesktopNavItems } from '@/components/nav/useNavItems';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  usePageTabActions,
  usePageTabs,
  type PageTab,
  type PageTabActions,
  type PageTabsState,
} from '@/components/PageTabs/pageTabsContext';
import { SidebarCorner } from '@/components/Sidebar/SidebarCorner';
import type { SidebarPeek } from '@/components/Sidebar/useSidebarPeek';
import { cn } from '@/lib/utils';
import './TopBar.css';

const ROUTE_ITEMS = [...NAV_ITEMS, SETTINGS_ITEM];

/** A tab's name and icon: what its page called itself, else the section its
 *  address is in. */
function describe(tab: PageTab, t: TFunction): { label: string; Icon: LucideIcon } {
  const pathname = tab.path.split(/[?#]/)[0];
  const item = ROUTE_ITEMS.find((i) => pathname === i.key || pathname.startsWith(`${i.key}/`));
  return { label: tab.title || (item ? t(item.labelKey) : t('pageTabs.untitled')), Icon: item?.icon ?? Globe };
}

function PageTabStrip({ tabs, actions }: { tabs: PageTabsState; actions: PageTabActions }) {
  const { t } = useTranslation();
  const places = useDesktopNavItems();
  // The tab in front is kept in view, a new one opened at the strip's end too.
  const listRef = useRef<HTMLElement>(null);
  useEffect(() => {
    listRef.current?.querySelector('[aria-current="page"]')?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }, [tabs.activeId]);
  return (
    <div className="page-tabs">
      {/* The tabs scroll once they reach their narrowest; the new-tab button
          stays outside them, so it is always in reach. Navigation rather than
          an ARIA tab list: choosing one changes the page, there is no panel
          of its own, and each carries a close button beside it. */}
      <nav
        ref={listRef}
        className="page-tabs-list"
        aria-label={t('pageTabs.label')}
        onWheel={(e) => {
          // A wheel that only scrolls up and down moves the strip along.
          if (e.deltaX === 0) e.currentTarget.scrollLeft += e.deltaY;
        }}
      >
        {tabs.tabs.map((tab) => {
          const { label, Icon } = describe(tab, t);
          const active = tab.id === tabs.activeId;
          return (
            <div
              key={tab.id}
              className={cn('page-tab', active && 'is-active')}
              // A middle click closes, as in a browser.
              onAuxClick={(e) => {
                if (e.button === 1) actions.close(tab.id);
              }}
            >
              <button
                type="button"
                aria-current={active ? 'page' : undefined}
                className="page-tab-main"
                title={label}
                onClick={() => actions.select(tab.id)}
              >
                <Icon className="page-tab-icon" aria-hidden="true" />
                <span className="page-tab-label">{label}</span>
              </button>
              {tabs.tabs.length > 1 && (
                <button
                  type="button"
                  className="page-tab-close"
                  aria-label={t('pageTabs.close', { title: label })}
                  onClick={() => actions.close(tab.id)}
                >
                  <X className="h-3 w-3" aria-hidden="true" />
                </button>
              )}
            </div>
          );
        })}
      </nav>
      {/* A new tab asks where it goes, from what the sidebar lists, in its
          order and under its names, rather than landing on one and making
          you move it. */}
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            className="page-tab-new"
            aria-label={t('pageTabs.newTab')}
            title={t('pageTabs.newTab')}
          >
            <Plus className="h-3.5 w-3.5" aria-hidden="true" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="min-w-44">
          {places.map((item) => (
            <DropdownMenuItem key={item.labelKey} onSelect={() => actions.open(item.key)}>
              <item.icon className="h-4 w-4 text-(--color-icon-muted)" aria-hidden="true" />
              {t(item.labelKey)}
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator />
          <DropdownMenuItem onSelect={() => actions.open(SETTINGS_ITEM.key)}>
            <SETTINGS_ITEM.icon className="h-4 w-4 text-(--color-icon-muted)" aria-hidden="true" />
            {t(SETTINGS_ITEM.labelKey)}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

interface AppTopBarProps {
  sidebarHidden: boolean;
  onToggleSidebar: () => void;
  peek: SidebarPeek;
  cornerRef: React.Ref<HTMLDivElement>;
  /** Across the page column too, rather than only the corner over the sidebar. */
  spans: boolean;
  /** Where a page's own controls go (TopBarActions). */
  slotRef: (node: HTMLElement | null) => void;
}

/**
 * The row across the top of the window. Its left end is the sidebar's corner
 * (the toggle and history, with the brand while the sidebar is shown). While
 * it spans the page, the rest is the window's drag handle holding, with page
 * tabs on, the open pages, and at its right end the controls of a page that
 * puts them here. Not drawn while a hidden sidebar's toggle is in the page's
 * own top row (App.tsx), unless the sidebar peeks.
 */
export default function AppTopBar({ sidebarHidden, onToggleSidebar, peek, cornerRef, spans, slotRef }: AppTopBarProps) {
  const tabs = usePageTabs();
  const actions = usePageTabActions();
  return (
    <div className={cn('app-topbar', spans && 'app-topbar--spans')} data-chrome="drag">
      <SidebarCorner hidden={sidebarHidden} onToggleHidden={onToggleSidebar} peek={peek} cornerRef={cornerRef} />
      {spans && (
        <>
          {tabs && actions && <PageTabStrip tabs={tabs} actions={actions} />}
          <div ref={slotRef} className="app-topbar-actions" />
        </>
      )}
    </div>
  );
}
