import React, { useCallback, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { LayoutGrid, PanelLeftClose, PanelLeftOpen, SquarePen } from 'lucide-react';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import { useTheme } from '../../contexts/ThemeContext';
import { NAV_ITEMS } from '../nav/navItems';
import { useNavActive } from '../nav/useNavActive';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import NavigationPanel from '@/pages/ChatAgent/components/NavigationPanel';
import NavDisplayOptions from '@/pages/ChatAgent/components/NavDisplayOptions';
import { useNavTreeProps } from '@/pages/ChatAgent/hooks/useNavTreeProps';
import type { NavWorkspace } from '@/pages/ChatAgent/hooks/useNavigationData';
import AccountMenu from './AccountMenu';
import { useChatRoute } from './useChatRoute';
import { useSidebarAgents } from '@/pages/ChatAgent/components/sidebarAgentsBridge';
import { SIDEBAR_DEFAULT_WIDTH, clampSidebarWidth } from './sidebarWidth';
import './Sidebar.css';

interface AppSidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
  /** Current expanded-panel width (px). */
  width: number;
  /** Live width update during a resize drag; commit=true persists (drag end / reset). */
  onWidthChange: (width: number, commit?: boolean) => void;
}

/**
 * App-shell sidebar combining the old icon rail and the chat-only overlay
 * thread panel into one Codex-style panel: primary nav up top, the
 * workspace/thread tree below, account row at the bottom. Collapses back to
 * the 80px icon rail (state owned by AuthenticatedShell, which also drives
 * --sidebar-width so the content column follows).
 */
function AppSidebar({ collapsed, onToggleCollapse, width, onWidthChange }: AppSidebarProps) {
  const navigate = useNavigate();
  const { t } = useTranslation();
  const { theme } = useTheme();
  const isActive = useNavActive();
  const logo = theme === 'light' ? logoDark : logoLight;

  const widthRef = useRef(width);
  widthRef.current = width;

  // Armed for the duration of a resize drag; unmount mid-drag (rail collapse,
  // route swap) would otherwise strand the app-wide sidebar-resizing body
  // class (col-resize cursor + no text selection everywhere).
  const dragCleanupRef = useRef<(() => void) | null>(null);
  useEffect(() => () => dragCleanupRef.current?.(), []);

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

  // Read back the tree's rows without making the new-thread handler depend on
  // them — the handler is an input to the hook that produces them.
  const workspacesRef = useRef<NavWorkspace[]>([]);

  // DESKTOP INTENT: the tree's ✎ opens the workspace HOME (thread gallery —
  // centered composer + recent tasks, Codex-style D14), not a blank thread.
  // The mobile drawer deliberately differs; see ChatView's handleNewThread.
  const openWorkspaceHome = useCallback((wsId: string) => {
    const ws = workspacesRef.current.find((w) => w.workspace_id === wsId);
    navigate(`/chat/${wsId}`, {
      state: {
        workspaceName: ws?.name || 'Workspace',
        workspaceStatus: ws?.status || null,
      },
    });
  }, [navigate]);

  const navTreeProps = useNavTreeProps({
    currentWorkspaceId,
    currentThreadId,
    agents: agentsSlice,
    onNewThread: openWorkspaceHome,
  });
  workspacesRef.current = navTreeProps.workspaces;

  // New chat opens the workspace gallery — picking (or creating) the workspace
  // is part of starting a conversation. The tree's per-workspace ✎ skips the
  // chooser and goes straight to that workspace's home.
  const handleNewChat = useCallback(() => {
    navigate('/chat');
  }, [navigate]);

  const openGallery = useCallback(() => navigate('/chat'), [navigate]);

  // One persistent <aside> so the width transition runs across mode flips;
  // the keyed fragments force each mode's content to cleanly remount (React
  // would otherwise morph rail nodes into panel nodes in place), which lets
  // the sidebar-content-in mount animation choreograph the swap.
  if (collapsed) {
    return (
      <aside className="sidebar">
        <React.Fragment key="rail">
        <button
          type="button"
          className="sidebar-logo"
          onClick={() => navigate('/dashboard')}
          aria-label={t('sidebar.dashboard')}
        >
          <img src={logo} alt="" style={{ width: '28px', height: '28px', objectFit: 'contain' }} />
        </button>
        <button
          className="sidebar-rail-toggle"
          onClick={onToggleCollapse}
          aria-label={t('sidebar.expand')}
          title={t('sidebar.expand')}
        >
          <PanelLeftOpen className="h-4 w-4" />
        </button>
        <nav className="sidebar-nav">
          <TooltipProvider delayDuration={300}>
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              const label = t(item.labelKey);
              const active = isActive(item);
              return (
                <Tooltip key={item.key}>
                  <TooltipTrigger asChild>
                    <button
                      className={`sidebar-nav-item ${active ? 'active' : ''}`}
                      onClick={() => navigate(item.key)}
                      aria-label={label}
                      aria-current={active ? 'page' : undefined}
                    >
                      <Icon className="sidebar-nav-icon" />
                    </button>
                  </TooltipTrigger>
                  <TooltipContent side="right" sideOffset={8}>
                    {label}
                  </TooltipContent>
                </Tooltip>
              );
            })}
          </TooltipProvider>
        </nav>
        <div className="sidebar-bottom">
          <AccountMenu />
        </div>
        </React.Fragment>
      </aside>
    );
  }

  return (
    <aside className="sidebar sidebar--panel">
      <React.Fragment key="panel">
      <div className="sidebar-panel-header">
        <button
          type="button"
          className="sidebar-brand"
          onClick={() => navigate('/dashboard')}
          aria-label={t('sidebar.dashboard')}
        >
          <img src={logo} alt="" style={{ width: '28px', height: '28px', objectFit: 'contain' }} />
          <span className="sidebar-wordmark title-font">LangAlpha</span>
        </button>
        <button
          className="sidebar-collapse-btn"
          onClick={onToggleCollapse}
          aria-label={t('sidebar.collapse')}
          title={t('sidebar.collapse')}
        >
          <PanelLeftClose className="h-4 w-4" />
        </button>
      </div>

      <nav className="sidebar-panel-nav">
        <button className="sidebar-panel-item" onClick={handleNewChat}>
          <SquarePen className="sidebar-panel-item-icon" />
          <span>{t('sidebar.newChat')}</span>
        </button>
        {NAV_ITEMS.filter((item) => item.key !== '/chat').map((item) => {
          const Icon = item.icon;
          const active = isActive(item);
          return (
            <button
              key={item.key}
              className={`sidebar-panel-item ${active ? 'active' : ''}`}
              onClick={() => navigate(item.key)}
              aria-current={active ? 'page' : undefined}
            >
              <Icon className="sidebar-panel-item-icon" />
              <span>{t(item.labelKey)}</span>
            </button>
          );
        })}
      </nav>

      <div className="sidebar-section-header">
        <button className="sidebar-section-title" onClick={openGallery}>
          {t('sidebar.workspaces')}
        </button>
        <div className="sidebar-section-actions">
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

      <div className="sidebar-panel-bottom">
        <AccountMenu variant="row" />
      </div>

      {/* Edge drag = resize (double-click resets to the default width). */}
      <div
        className="sidebar-resize-handle"
        role="separator"
        aria-orientation="vertical"
        aria-label={t('sidebar.resize')}
        title={t('sidebar.resize')}
        onPointerDown={handleResizeStart}
        onDoubleClick={handleResizeReset}
      />
      </React.Fragment>
    </aside>
  );
}

export default AppSidebar;
