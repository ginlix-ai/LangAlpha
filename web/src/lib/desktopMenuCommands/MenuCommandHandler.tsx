/**
 * The main window's half of the shell's menus: declares the commands this page
 * can carry out, and carries them out with the router, never a reload.
 *
 * `export-pdf` is not declared: the app's PDF exports are per file, from the
 * file panel, with nothing page-wide for a menu item to trigger.
 */
import { useEffect, useEffectEvent, useMemo } from 'react';
import { useNavigate } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { useOpenCreateWorkspace } from '@/components/CreateWorkspaceHost';
import { useNewThread, usePlace } from '@/components/CommandPalette/useVerbs';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useOrdersVisible } from '@/hooks/useOrdersVisible';
import { appRoutePath } from '@/lib/appRoutes';
import { canDesktopSettings, onMenuCommand, setMenuCommands, type DesktopMenuCommand } from '@/lib/desktop';

/** Commands that only go somewhere. Settings opens on its Desktop app section where the shell has one. */
const ROUTES: Partial<Record<DesktopMenuCommand, string>> = {
  'open-settings': canDesktopSettings() ? '/settings?tab=desktop' : '/settings',
  'go-market': '/market',
  'go-automations': '/automations',
  'go-orders': '/orders',
};

export default function MenuCommandHandler({ onToggleSidebar }: { onToggleSidebar?: () => void }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const openCreateWorkspace = useOpenCreateWorkspace();
  const place = usePlace();
  const newThread = useNewThread(place.workspaceId);
  const canCreateWorkspace = !!openCreateWorkspace;
  const canToggleSidebar = !!onToggleSidebar;
  // As the sidebar draws it: offered once the account has a brokerage or an order.
  const showOrders = useOrdersVisible() === true;

  const commands = useMemo(() => {
    const declared: DesktopMenuCommand[] = ['new-thread', 'open-settings', 'go-chief-of-staff', 'go-market', 'go-automations', 'open'];
    if (canCreateWorkspace) declared.push('new-workspace');
    if (canToggleSidebar) declared.push('toggle-sidebar');
    if (showOrders) declared.push('go-orders');
    return declared;
  }, [canCreateWorkspace, canToggleSidebar, showOrders]);

  useEffect(() => setMenuCommands(commands), [commands]);
  // Gone (signed out, the welcome flow): the menus stop offering what nothing handles.
  useEffect(() => () => setMenuCommands([]), []);

  const run = useEffectEvent((command: DesktopMenuCommand, path?: string) => {
    const route = ROUTES[command];
    if (route) {
      navigate(route);
      return;
    }
    switch (command) {
      case 'new-thread':
        // Where the command palette's New thread goes: the workspace on
        // screen, else Home and its Chief of Staff. Until a linked thread's
        // workspace is known, the chat home, where the user picks.
        if (place.pending || !place.workspaceId) navigate('/chat');
        else newThread(place.workspaceId);
        return;
      case 'new-workspace':
        openCreateWorkspace?.();
        return;
      case 'toggle-sidebar':
        onToggleSidebar?.();
        return;
      case 'go-chief-of-staff':
        queryClient.ensureQueryData(homeWorkspaceQuery(queryClient)).then(
          (home) => navigate(`/chat/${home.workspace_id}`),
          () => navigate('/chat'),
        );
        return;
      case 'open': {
        if (!path) return;
        const target = appRoutePath(path);
        if (target) navigate(target);
        else if (path === '/') navigate('/dashboard');
        return;
      }
      default:
        return;
    }
  });

  useEffect(() => onMenuCommand(({ command, path }) => run(command, path)), []);

  return null;
}
