import { lazy, Suspense, type ReactElement } from 'react';
import { canReceiveMenuCommands, isMainWindow } from '@/lib/desktop';

// Loaded only inside a shell that sends menu commands, so a browser build
// carries none of it on first load.
const MenuCommandHandler = lazy(() => import('./MenuCommandHandler'));

/**
 * The shell's menu commands (New Thread, Go › Market, a notification's Open),
 * handled in the main window. Mounted once in the app shell; renders nothing,
 * and does nothing outside the desktop shell's main window.
 */
export function DesktopMenuCommands({ onToggleSidebar }: { onToggleSidebar?: () => void }): ReactElement | null {
  if (!isMainWindow() || !canReceiveMenuCommands()) return null;
  return (
    <Suspense fallback={null}>
      <MenuCommandHandler onToggleSidebar={onToggleSidebar} />
    </Suspense>
  );
}
