import React, { Suspense, lazy, useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, ArrowRight, LayoutGrid, PanelLeftClose, PanelLeftOpen, Plus, SquarePen } from 'lucide-react';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import { useTheme } from '../../contexts/ThemeContext';
import { useNavActive } from '../nav/useNavActive';
import { useNavItems } from '../nav/useNavItems';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import NavigationPanel from '@/pages/ChatAgent/components/NavigationPanel';
import NavDisplayOptions from '@/pages/ChatAgent/components/NavDisplayOptions';
import { useCreateWorkspace } from '@/pages/ChatAgent/hooks/useCreateWorkspace';
import { useNavTreeProps } from '@/pages/ChatAgent/hooks/useNavTreeProps';
import type { NavWorkspace } from '@/pages/ChatAgent/hooks/useNavigationData';
import type { Workspace } from '@/types/api';
import AccountMenu from './AccountMenu';
import { useChatRoute } from './useChatRoute';
import { useSidebarAgents } from '@/pages/ChatAgent/components/sidebarAgentsBridge';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { GettingStartedGate } from '@/pages/Onboarding';
import { SIDEBAR_DEFAULT_WIDTH, clampSidebarWidth } from './sidebarWidth';
import './Sidebar.css';

interface NavigationLike {
  canGoBack: boolean;
  canGoForward: boolean;
  addEventListener: (type: string, fn: () => void) => void;
  removeEventListener: (type: string, fn: () => void) => void;
}

/** Back/forward for the desktop titlebar row; hidden elsewhere by CSS. */
function HistoryButtons() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // Without the Navigation API the stack depth is unknowable, so both stay live.
  const [can, setCan] = useState({ back: true, forward: true });
  useEffect(() => {
    if (!document.documentElement.classList.contains('desktop-mac')) return;
    const nav = (window as unknown as { navigation?: NavigationLike }).navigation;
    if (!nav) return;
    const sync = () =>
      setCan((prev) =>
        prev.back === nav.canGoBack && prev.forward === nav.canGoForward
          ? prev
          : { back: nav.canGoBack, forward: nav.canGoForward },
      );
    sync();
    nav.addEventListener('currententrychange', sync);
    return () => nav.removeEventListener('currententrychange', sync);
  }, []);
  return (
    <div className="sidebar-history">
      <button type="button" className="sidebar-collapse-btn" disabled={!can.back} onClick={() => navigate(-1)} aria-label={t('sidebar.back', 'Back')} title={t('sidebar.back', 'Back')}>
        <ArrowLeft className="h-4 w-4" />
      </button>
      <button type="button" className="sidebar-collapse-btn" disabled={!can.forward} onClick={() => navigate(1)} aria-label={t('sidebar.forward', 'Forward')} title={t('sidebar.forward', 'Forward')}>
        <ArrowRight className="h-4 w-4" />
      </button>
    </div>
  );
}

// Off the entry chunk: the modal is only needed once the button is pressed.
const CreateWorkspaceModal = lazy(() => import('@/pages/ChatAgent/components/CreateWorkspaceModal'));

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
 * the icon rail (state owned by AuthenticatedShell, which also drives
 * --sidebar-width so the content column follows).
 */
function AppSidebar({ collapsed, onToggleCollapse, width, onWidthChange }: AppSidebarProps) {
  const navigate = useNavigate();
  const { t } = useTranslation();
  const { theme } = useTheme();
  const isActive = useNavActive();
  const navItems = useNavItems();
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

  // New chat opens the workspace gallery — picking (or creating) the workspace
  // is part of starting a conversation. The tree's per-workspace ✎ skips the
  // chooser and goes straight to that workspace's home.
  const handleNewChat = useCallback(() => {
    navigate('/chat');
  }, [navigate]);

  const openGallery = useCallback(() => navigate('/chat'), [navigate]);

  // Same create flow as the gallery, without leaving the current page first.
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const createWorkspace = useCreateWorkspace();
  const openCreatedWorkspace = useCallback((ws: Workspace) => {
    navigate(`/chat/${ws.workspace_id}`, { state: { workspaceName: ws.name, workspaceStatus: null } });
  }, [navigate]);

  // One persistent <aside> so the width transition runs across mode flips;
  // the keyed fragments force each mode's content to cleanly remount (React
  // would otherwise morph rail nodes into panel nodes in place), which lets
  // the sidebar-content-in mount animation choreograph the swap.
  if (collapsed) {
    return (
      <aside className="sidebar" data-chrome>
        <React.Fragment key="rail">
        <div className="sidebar-window-drag" aria-hidden="true" />
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
            {navItems.map((item) => {
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
          <GettingStartedGate variant="rail" onExpand={onToggleCollapse} />
          <AccountMenu />
        </div>
        </React.Fragment>
      </aside>
    );
  }

  return (
    <aside className="sidebar sidebar--panel" data-chrome>
      <React.Fragment key="panel">
      <div className="sidebar-window-drag" aria-hidden="true" />
      {/* The brand row doubles as titlebar: the buttons inside it declare
          no-drag for themselves, so only the gaps move the window. */}
      <div className="sidebar-panel-header" data-chrome="drag">
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
        <HistoryButtons />
      </div>

      <nav className="sidebar-panel-nav">
        <button className="sidebar-panel-item" onClick={handleNewChat}>
          <SquarePen className="sidebar-panel-item-icon" />
          <span>{t('sidebar.newChat')}</span>
        </button>
        {navItems.filter((item) => item.key !== '/chat').map((item) => {
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
          <button
            className="nav-panel-dismiss-btn sidebar-section-action-btn"
            onClick={() => setIsCreateOpen(true)}
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

      <GettingStartedGate variant="row" />

      <div className="sidebar-panel-bottom">
        <AccountMenu variant="row" />
      </div>

      {isCreateOpen && (
        <Suspense fallback={null}>
          <CreateWorkspaceModal
            isOpen
            onClose={() => setIsCreateOpen(false)}
            onCreate={createWorkspace}
            onComplete={openCreatedWorkspace}
          />
        </Suspense>
      )}

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
