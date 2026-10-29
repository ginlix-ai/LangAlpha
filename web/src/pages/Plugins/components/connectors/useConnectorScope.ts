import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  useSetMcpServerEnabledInWorkspace,
  useSetMcpServerNewWorkspaces,
} from '@/hooks/useMcpServers';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';

/**
 * A connector's availability writes, for its detail page: the per-workspace
 * deny checklist (builtin and catalog rows alike) and, on a catalog row, what
 * a workspace created later starts with. Held here rather than by the list,
 * because the list no longer has a scope control: the detail page is the one
 * place availability is changed, from Connectors and from Brokerages both.
 */
export function useConnectorScope(name: string | null) {
  const { t } = useTranslation();
  const wsEnable = useSetMcpServerEnabledInWorkspace();
  const newWorkspaces = useSetMcpServerNewWorkspaces();
  const [busy, setBusy] = useState(false);

  async function run(write: () => Promise<unknown>) {
    setBusy(true);
    try {
      await write();
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setBusy(false);
    }
  }

  return {
    busy,
    setWorkspaceDisabled: (workspaceId: string, disabled: boolean) => {
      if (!name) return;
      void run(() => wsEnable.mutateAsync({ workspaceId, name, enabled: !disabled }));
    },
    setNewWorkspacesOn: (on: boolean) => {
      if (!name) return;
      void run(() => newWorkspaces.mutateAsync({ name, enabled: on }));
    },
  };
}
