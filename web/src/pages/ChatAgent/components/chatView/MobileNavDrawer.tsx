import { useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Minus } from 'lucide-react';
import { motion, AnimatePresence, type PanInfo } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import NavigationPanel from '../NavigationPanel';
import NavDisplayOptions from '../NavDisplayOptions';
import { useNavTreeProps, type NavTreeAgents } from '../../hooks/useNavTreeProps';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import type { NavWorkspace } from '../../hooks/useNavigationData';

interface MobileNavDrawerProps {
  visible: boolean;
  /** False when the drawer was already open on arrival, so it appears in place. */
  slideIn: boolean;
  onMinimize: () => void;
  isActive: boolean;
  workspaceId: string;
  threadId: string;
  agents: NavTreeAgents;
  workspaceName: string;
}

/**
 * ChatView's navigation drawer. Desktop navigation lives in the AppSidebar, so
 * this mounts on mobile only: the tree's data layer (workspace list, thread
 * queries, store subscriptions) then runs nowhere on desktop, where it was
 * once parked but still ran with every streamed token.
 */
export function MobileNavDrawer({ visible, slideIn, onMinimize, isActive, workspaceId, threadId, agents, workspaceName }: MobileNavDrawerProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const workspaceLabel = useWorkspaceLabel();

  // MOBILE INTENT: the drawer's ✎ opens a BLANK thread in that workspace, so a
  // one-tap new chat needs no second stop on the gallery. `__default__` + a
  // workspaceId in route state resolves to a brand-new thread (ChatAgent only
  // restores a stored session for the bare /chat route). The desktop sidebar
  // deliberately differs; see AppSidebar's openWorkspaceHome.
  const handleNewThread = useCallback((wsId: string, ws: NavWorkspace | undefined) => {
    const status = ws?.status || null;
    navigate('/chat/t/__default__', {
      state: {
        workspaceId: wsId,
        workspaceName: workspaceLabel(ws),
        workspaceStatus: status,
      },
    });
  }, [navigate, workspaceLabel]);

  const navTreeProps = useNavTreeProps({
    currentWorkspaceId: workspaceId,
    currentThreadId: threadId,
    agents,
    onNewThread: handleNewThread,
    fallbackWorkspaceName: workspaceName,
  });

  // NavigationPanel is memoized, and this node is one of its props: built
  // inline it would be new on every render, and the whole tree with it.
  const headerActions = useMemo(() => (
    <>
      {/* Sidebar display options (workspace/thread visibility) —
          pinned to the left edge; margin-right:auto pushes the
          minimize control to the right of the header row. */}
      <div style={{ marginRight: 'auto', display: 'flex', alignItems: 'center' }}>
        <NavDisplayOptions />
      </div>
      {/* Minimize button — closes the drawer */}
      <button
        onClick={onMinimize}
        className="nav-panel-dismiss-btn"
        style={{
          padding: 4,
          background: 'transparent',
          border: 'none',
          cursor: 'pointer',
          borderRadius: 4,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
        title={t('nav.minimize')}
        aria-label={t('nav.minimize')}
      >
        <Minus className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
      </button>
    </>
  ), [onMinimize, t]);

  return (
    <>
      {/* Dimmed backdrop behind the drawer */}
      {visible && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            zIndex: 39,
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
          }}
          onClick={onMinimize}
        />
      )}
      {/* Interactive only when visible */}
      <div
        style={{
          position: 'absolute',
          left: 0,
          top: 0,
          bottom: 0,
          width: 'min(320px, calc(100% - 48px))',
          zIndex: 40,
          pointerEvents: visible ? 'auto' : 'none',
        }}
      >
        <AnimatePresence>
          {visible && (
            <motion.div
              initial={slideIn ? { x: '-100%', opacity: 0 } : false}
              animate={{ x: 0, opacity: 1 }}
              exit={{ x: '-100%', opacity: 0, transition: TWEEN.exit }}
              transition={TWEEN.enter}
              drag="x"
              dragConstraints={{ left: -320, right: 0 }}
              dragElastic={{ left: 0.3, right: 0 }}
              onDragEnd={(_: unknown, info: PanInfo) => {
                if (info.velocity.x < -300 || info.offset.x < -100) onMinimize();
              }}
              style={{ width: '100%', height: '100%', position: 'absolute', left: 0, top: 0 }}
            >
              <NavigationPanel
                headerActions={headerActions}
                isActive={isActive}
                {...navTreeProps}
              />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </>
  );
}
