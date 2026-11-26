import React, { lazy, useCallback, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { LayoutGrid, Plus, Search } from 'lucide-react';
import { useNavActive } from '../nav/useNavActive';
import { useDesktopNavItems } from '../nav/useNavItems';
import { cn } from '@/lib/utils';
import NavigationPanel from '@/pages/ChatAgent/components/NavigationPanel';
import NavDisplayOptions from '@/pages/ChatAgent/components/NavDisplayOptions';
import { useNavTreeProps } from '@/pages/ChatAgent/hooks/useNavTreeProps';
import type { NavWorkspace } from '@/pages/ChatAgent/hooks/useNavigationData';
import AccountMenu from './AccountMenu';
import { MountOnceOpen } from '@/components/MountOnceOpen';
import { canShowUpdates } from '@/lib/desktop';
import { withCopy } from '@/i18n';
import { draftKeys } from '@/lib/composerDrafts';
import { DraftPencil } from '@/components/ui/draft-mark';
import { useChatRoute } from './useChatRoute';
import { paletteKeyShortcuts, paletteShortcut, useCommandPalette } from '../CommandPalette/CommandPaletteProvider';
import { useOpenCreateWorkspace } from '../CreateWorkspaceHost';
import { useSidebarAgents } from '@/pages/ChatAgent/components/sidebarAgentsBridge';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { GettingStartedGate } from '@/pages/Onboarding';
import { SIDEBAR_DEFAULT_WIDTH, clampSidebarWidth } from './sidebarWidth';
import type { SidebarPeek } from './useSidebarPeek';
import './Sidebar.css';

// Off first load: only the desktop app has an update to offer, so a browser
// never fetches the button or its copy.
const AppUpdate = lazy(withCopy(() => import('./AppUpdate'), ['appUpdate']));

/** The pages whose own composer keeps a draft, which their item points at
 *  while you are elsewhere. */
const PAGE_DRAFTS: Record<string, string> = {
  '/dashboard': draftKeys.dashboard,
  '/market': draftKeys.market,
};

interface AppSidebarProps {
  /** Out of the layout, Arc-style; the shell owns it and the content column. */
  hidden: boolean;
  /** Current expanded-panel width (px). */
  width: number;
  /** Live width update during a resize drag; commit=true persists (drag end / reset). */
  onWidthChange: (width: number, commit?: boolean) => void;
  /** Hidden, the panel peeks in over the page from the top bar's toggle. */
  peek: SidebarPeek;
  panelRef: React.Ref<HTMLElement>;
}

/**
 * App-shell sidebar: primary nav up top, the workspace/thread tree below,
 * account row at the bottom. Hides out of the layout entirely (state owned by
 * AuthenticatedShell, which also drives --sidebar-width so the content column
 * follows) and peeks back in over the page from the top bar's toggle.
 */
function AppSidebar({ hidden, width, onWidthChange, peek, panelRef }: AppSidebarProps) {
  const navigate = useNavigate();
  const { t } = useTranslation();
  const isActive = useNavActive();
  const navItems = useDesktopNavItems();

  const widthRef = useRef(width);
  widthRef.current = width;

  // Armed for the duration of a resize drag; unmount mid-drag (hiding,
  // route swap) would otherwise strand the app-wide sidebar-resizing body
  // class (col-resize cursor + no text selection everywhere).
  const dragCleanupRef = useRef<(() => void) | null>(null);
  useEffect(() => () => dragCleanupRef.current?.(), []);
  // Hiding mid-drag takes the handle away before its pointerup.
  useEffect(() => {
    if (hidden) dragCleanupRef.current?.();
  }, [hidden]);

  // Edge-drag resize. Pointer capture keeps move/up events routed to the handle
  // even when the cursor outruns it; a body class pins the col-resize cursor,
  // blocks text selection, and drops the width transitions for 1:1 tracking.
  const handleResizeStart = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const handle = e.currentTarget;
    try {
      handle.setPointerCapture(e.pointerId);
    } catch {
      // Pointer already released (or synthetic) — listeners below still work
      // for moves over the handle; the drag just loses off-element tracking.
    }
    const startX = e.clientX;
    const startWidth = widthRef.current;
    document.body.classList.add('sidebar-resizing');
    const onMove = (ev: PointerEvent) => {
      onWidthChange(clampSidebarWidth(startWidth + (ev.clientX - startX)));
    };
    // Hoisted declarations: teardown and onEnd reference each other.
    function teardown() {
      dragCleanupRef.current = null;
      document.body.classList.remove('sidebar-resizing');
      handle.removeEventListener('pointermove', onMove);
      handle.removeEventListener('pointerup', onEnd);
      handle.removeEventListener('pointercancel', onEnd);
    }
    function onEnd(ev: PointerEvent) {
      teardown();
      onWidthChange(clampSidebarWidth(startWidth + (ev.clientX - startX)), true);
    }
    handle.addEventListener('pointermove', onMove);
    handle.addEventListener('pointerup', onEnd);
    handle.addEventListener('pointercancel', onEnd);
    dragCleanupRef.current = teardown;
  }, [onWidthChange]);

  const handleResizeReset = useCallback(() => {
    onWidthChange(SIDEBAR_DEFAULT_WIDTH, true);
  }, [onWidthChange]);

  const { currentWorkspaceId, currentThreadId } = useChatRoute();
  // Subagent registry published by the visible ChatView (sidebarAgentsBridge).
  // Only surfaced when the slice belongs to the thread this sidebar shows as
  // current — a stale slice from another thread must never render a tree.
  const sidebarAgents = useSidebarAgents();
  const agentsSlice = sidebarAgents && sidebarAgents.threadId === currentThreadId ? sidebarAgents : null;

  // DESKTOP INTENT: the tree's ✎ opens the workspace HOME (thread gallery —
  // centered composer + recent tasks, Codex-style D14), not a blank thread.
  // The mobile drawer deliberately differs; see ChatView's handleNewThread.
  const workspaceLabel = useWorkspaceLabel();
  const openWorkspaceHome = useCallback((wsId: string, ws: NavWorkspace | undefined) => {
    navigate(`/chat/${wsId}`, {
      state: {
        workspaceName: workspaceLabel(ws) || 'Workspace',
        workspaceStatus: ws?.status || null,
      },
    });
  }, [navigate, workspaceLabel]);

  const navTreeProps = useNavTreeProps({
    currentWorkspaceId,
    currentThreadId,
    agents: agentsSlice,
    onNewThread: openWorkspaceHome,
  });

  const openGallery = useCallback(() => navigate('/chat'), [navigate]);
  const palette = useCommandPalette();

  // Same create flow as the gallery, without leaving the current page first.
  const openCreateWorkspace = useOpenCreateWorkspace();

  return (
    <aside
      ref={panelRef}
      className={cn('sidebar sidebar--panel', hidden && 'sidebar--hidden')}
      data-chrome
      data-peek={peek.open ? '' : undefined}
      inert={hidden && !peek.open}
      style={{ '--sidebar-panel-width': `${width}px` } as React.CSSProperties}
      onPointerEnter={peek.panelHover?.onPointerEnter}
      onPointerLeave={peek.panelHover?.onPointerLeave}
    >
      <div className="sidebar-window-drag" aria-hidden="true" />
      {/* The panel's share of the top bar, under the bar's corner. */}
      <div className="sidebar-panel-header" data-chrome="drag" />

      <nav className="sidebar-panel-nav">
        {palette && (
          <button
            className="sidebar-panel-item"
            onClick={() => palette.open()}
            onPointerEnter={palette.preload}
            aria-keyshortcuts={paletteKeyShortcuts()}
          >
            <Search className="sidebar-panel-item-icon" />
            <span>{t('palette.search')}</span>
            <kbd className="sidebar-shortcut" aria-hidden>{paletteShortcut()}</kbd>
          </button>
        )}
        {/* New chat opens the workspace gallery: picking (or creating) the
            workspace is part of starting a conversation. The tree's
            per-workspace ✎ skips the chooser for that workspace's home. */}
        {navItems.map((item) => {
          const Icon = item.icon;
          const active = isActive(item);
          return (
            <button
              key={item.labelKey}
              className={cn('sidebar-panel-item', active && 'active')}
              onClick={() => navigate(item.key)}
              aria-current={active ? 'page' : undefined}
            >
              <Icon className="sidebar-panel-item-icon" />
              <span>{t(item.labelKey)}</span>
              {PAGE_DRAFTS[item.key] && (
                <span className="ml-auto flex items-center">
                  <DraftPencil draftKey={PAGE_DRAFTS[item.key]} />
                </span>
              )}
            </button>
          );
        })}
      </nav>

      <div className="sidebar-section-header">
        <button className="sidebar-section-title" onClick={openGallery}>
          {t('sidebar.workspaces')}
        </button>
        <div className="sidebar-section-actions">
          <button
            className="nav-panel-dismiss-btn sidebar-section-action-btn"
            onClick={() => openCreateWorkspace?.()}
            aria-label={t('workspace.newWorkspace')}
            title={t('workspace.newWorkspace')}
          >
            <Plus className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
          </button>
          <NavDisplayOptions />
          <button
            className="nav-panel-dismiss-btn sidebar-section-action-btn"
            onClick={openGallery}
            aria-label={t('sidebar.workspaceGallery')}
            title={t('sidebar.workspaceGallery')}
          >
            <LayoutGrid className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
          </button>
        </div>
      </div>

      <div className="sidebar-tree">
        <NavigationPanel {...navTreeProps} />
      </div>

      <GettingStartedGate />

      <div className="sidebar-panel-bottom">
        <MountOnceOpen open={canShowUpdates()}>
          <AppUpdate variant="row" />
        </MountOnceOpen>
        <AccountMenu />
      </div>

      {/* Edge drag = resize (double-click resets to the default width). */}
      {!hidden && (
        <div
          className="sidebar-resize-handle"
          role="separator"
          aria-orientation="vertical"
          aria-label={t('sidebar.resize')}
          title={t('sidebar.resize')}
          onPointerDown={handleResizeStart}
          onDoubleClick={handleResizeReset}
        />
      )}
    </aside>
  );
}

export default AppSidebar;
