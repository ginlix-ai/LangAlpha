import { useContext } from 'react';
import { cn } from '@/lib/utils';
import { HistoryButtons, SidebarToggle } from './SidebarCorner';
import { SidebarRevealContext } from './sidebarRevealContext';

/**
 * The hidden sidebar's toggle (and in the desktop app back and forward) at
 * the start of a page's top row, where the page puts it. Nothing while the
 * toggle has another home. A row in the window's title bar band says so with
 * `titlebar`, and in the desktop app starts clear of the window buttons.
 */
export function SidebarReveal({ titlebar = false, className }: { titlebar?: boolean; className?: string }) {
  const reveal = useContext(SidebarRevealContext);
  if (!reveal) return null;
  return (
    <div className={cn('sidebar-reveal', titlebar && 'sidebar-reveal--titlebar', className)}>
      <SidebarToggle hidden onToggle={reveal.onToggle} hover={reveal.hover} />
      <HistoryButtons />
    </div>
  );
}
