import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, ArrowRight, PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import { useTheme } from '../../contexts/ThemeContext';
import { clientOs } from '@/lib/clientOs';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import type { SidebarPeek } from './useSidebarPeek';

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

/** Cmd+B on a Mac, Ctrl+B elsewhere: the key most apps with a sidebar use. On
 *  a Mac only Cmd counts, since Ctrl+B there moves the caret back a character. */
function isToggleShortcut(e: KeyboardEvent): boolean {
  if (e.altKey || e.shiftKey || e.key.toLowerCase() !== 'b') return false;
  return clientOs() === 'mac' ? e.metaKey && !e.ctrlKey : e.ctrlKey && !e.metaKey;
}

const toggleShortcutLabel = () => (clientOs() === 'mac' ? '⌘B' : 'Ctrl B');
const toggleKeyShortcuts = () => (clientOs() === 'mac' ? 'Meta+B' : 'Control+B');

/** The show/hide button, with its shortcut in the tooltip and on the window. */
function SidebarToggle({ hidden, onToggle, hover }: { hidden: boolean; onToggle: () => void; hover: SidebarPeek['toggleHover'] }) {
  const { t } = useTranslation();

  // A key an editor already took (bold, in one that has it) is left to it, as
  // is a rich-text field, where the browser's own Cmd+B is bold. A held key is
  // taken but toggles once, so the desktop app's menu, which answers the key
  // the page leaves, does not toggle on each repeat.
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.defaultPrevented || !isToggleShortcut(e)) return;
      if (e.target instanceof HTMLElement && e.target.isContentEditable) return;
      e.preventDefault();
      if (!e.repeat) onToggle();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onToggle]);

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
 * The top bar's left end: the brand, the sidebar's toggle and history. One
 * set for both of the sidebar's states, so the toggle holds still as the panel
 * goes and comes, and drawn over the panel, peeking too, so the pointer that
 * slid it in can go back to the toggle without closing it.
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
    <div ref={cornerRef} className="app-topbar-corner">
      <button
        type="button"
        className="sidebar-brand"
        onClick={() => navigate('/dashboard')}
        aria-label={t('sidebar.dashboard')}
      >
        <img src={theme === 'light' ? logoDark : logoLight} alt="" style={{ width: '22px', height: '22px', objectFit: 'contain' }} />
        <span className="sidebar-wordmark title-font">LangAlpha</span>
      </button>
      <SidebarToggle hidden={hidden} onToggle={onToggleHidden} hover={peek.toggleHover} />
      <HistoryButtons />
    </div>
  );
}
