import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, ArrowRight, PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import { useTheme } from '../../contexts/ThemeContext';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import type { SidebarPeek } from './useSidebarPeek';
import { toggleKeyShortcuts, toggleShortcutLabel } from './sidebarShortcut';

interface NavigationLike {
  canGoBack: boolean;
  canGoForward: boolean;
  addEventListener: (type: string, fn: () => void) => void;
  removeEventListener: (type: string, fn: () => void) => void;
}

/** Back/forward for the desktop titlebar row; hidden elsewhere by CSS. */
export function HistoryButtons() {
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

/** The show/hide button, with its shortcut in the tooltip and on the window. */
export function SidebarToggle({ hidden, onToggle, hover }: { hidden: boolean; onToggle: () => void; hover: SidebarPeek['toggleHover'] }) {
  const { t } = useTranslation();
  const label = hidden ? t('sidebar.expand') : t('sidebar.collapse');
  const Icon = hidden ? PanelLeftOpen : PanelLeftClose;
  return (
    <TooltipProvider delayDuration={400}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="sidebar-collapse-btn"
            onClick={onToggle}
            onPointerEnter={hover?.onPointerEnter}
            onPointerLeave={hover?.onPointerLeave}
            aria-label={label}
            aria-keyshortcuts={toggleKeyShortcuts()}
          >
            <Icon className="h-4 w-4" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="bottom" align="start" sideOffset={6}>
          {label} <kbd className="sidebar-shortcut ml-2">{toggleShortcutLabel()}</kbd>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/**
 * The top bar's left end: the sidebar's toggle and history, with the brand
 * while the sidebar is shown. Drawn over the panel, peeking too, and part of
 * it while it peeks, so the pointer on its way between the toggle and the
 * panel keeps it open.
 */
export function SidebarCorner({ hidden, onToggleHidden, peek, cornerRef }: {
  hidden: boolean;
  onToggleHidden: () => void;
  peek: SidebarPeek;
  cornerRef: React.Ref<HTMLDivElement>;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { theme } = useTheme();
  return (
    <div
      ref={cornerRef}
      className="app-topbar-corner"
      onPointerEnter={peek.panelHover?.onPointerEnter}
      onPointerLeave={peek.panelHover?.onPointerLeave}
    >
      {/* Not while hidden, peeking too: in front of the toggle it would move
          the toggle out from under the pointer that opened the peek. */}
      {!hidden && (
        <button
          type="button"
          className="sidebar-brand"
          onClick={() => navigate('/dashboard')}
          aria-label={t('sidebar.dashboard')}
        >
          <img src={theme === 'light' ? logoDark : logoLight} alt="" style={{ width: '22px', height: '22px', objectFit: 'contain' }} />
          <span className="sidebar-wordmark title-font">LangAlpha</span>
        </button>
      )}
      <SidebarToggle hidden={hidden} onToggle={onToggleHidden} hover={peek.toggleHover} />
      <HistoryButtons />
    </div>
  );
}
