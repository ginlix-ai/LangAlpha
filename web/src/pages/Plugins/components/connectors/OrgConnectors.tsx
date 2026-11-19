import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { LogIn, LogOut } from 'lucide-react';
import { DropdownMenuItem } from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import { useAccount } from '@/hooks/useAccount';
import { useOrgConnectors } from '@/hooks/useMcpServers';
import { queryKeys } from '@/lib/queryKeys';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import {
  formatApiErrorDetail,
  signOutOrgConnector,
  startOrgConnectorSignIn,
  type OrgConnector,
} from '@/pages/ChatAgent/utils/api';
import { RetryNote } from '../RetryNote';
import { CellAction, CellMenu, CellState, SurfaceCell, SurfaceGrid, SurfaceSection } from '../surface/Surface';

/** What one organization connector's cell says, from the member's side. */
export type OrgCellState =
  | { kind: 'sign_in' }
  | { kind: 'sign_in_again' }
  | { kind: 'not_answering' }
  | { kind: 'ready' };

/**
 * A member's sign-in first, since nothing works without it; then whether the
 * connector answered its last listing. A lapsed or unreachable connector stays
 * in its own section rather than joining "Needs attention", which is about
 * what the member added.
 */
function orgCellState(c: OrgConnector): OrgCellState {
  if (c.auth === 'member') {
    if (c.connection_status === null) return { kind: 'sign_in' };
    if (c.connection_status !== 'connected') return { kind: 'sign_in_again' };
  }
  if (c.discovery === 'error') return { kind: 'not_answering' };
  return { kind: 'ready' };
}

/**
 * The connectors the account's organization sets up, above the member's own:
 * shared ones ready to use, and ones each member signs in to with their own
 * account. The member signs in or out and can switch one off per workspace
 * from a workspace; nothing here edits or removes one, and no address is
 * shown. Renders nothing for an account with no organization.
 */
export function OrgConnectorsSection() {
  const { t } = useTranslation();
  const { organization, orgT } = useAccount();
  const query = useOrgConnectors();
  const queryClient = useQueryClient();
  const [busy, setBusy] = useState<string | null>(null);

  if (!organization) return null;
  if (query.isError && !query.data) {
    return (
      <div className="connector-notes">
        <RetryNote text={t('organization.connectors.unreadable')} onRetry={() => void query.refetch()} />
      </div>
    );
  }
  const connectors = query.data ?? [];
  if (connectors.length === 0) return null;

  const signIn = async (c: OrgConnector) => {
    setBusy(c.id);
    try {
      const started = await startOrgConnectorSignIn(c.id, `${window.location.pathname}${window.location.search}`);
      if (started) window.location.assign(started.authorize_url);
      else setBusy(null);
    } catch (err) {
      setBusy(null);
      toast({ variant: 'destructive', title: t('organization.connectors.signInFailed'), description: formatApiErrorDetail(err) });
    }
  };
  const signOut = async (c: OrgConnector) => {
    setBusy(c.id);
    try {
      await signOutOrgConnector(c.id);
      await queryClient.invalidateQueries({ queryKey: queryKeys.mcp.orgConnectors() });
    } catch (err) {
      toast({ variant: 'destructive', title: t('organization.connectors.signOutFailed'), description: formatApiErrorDetail(err) });
    } finally {
      setBusy(null);
    }
  };

  return (
    <SurfaceSection
      title={t('organization.connectors.title')}
      count={connectors.length}
      note={orgT('organization.connectors.setUpBy')}
    >
      <SurfaceGrid>
        {connectors.map((c) => {
          const state = orgCellState(c);
          const off = c.disabled_workspace_ids.length;
          const meta = off > 0
            ? t('organization.connectors.offIn', { count: off })
            : c.auth === 'shared'
              ? t('organization.connectors.shared')
              : state.kind !== 'sign_in' && state.kind !== 'sign_in_again'
                ? t('organization.connectors.yourSignIn')
                : null;
          const tools = c.discovery === 'ok' && (c.tool_count ?? 0) > 0
            ? t('mcp.row.toolCount', { count: c.tool_count })
            : null;
          const line = state.kind === 'sign_in' ? t('organization.connectors.signInLine', { name: c.name })
            : state.kind === 'sign_in_again' ? t('organization.connectors.signInStopped')
            : state.kind === 'not_answering' ? t('organization.connectors.notAnswering')
            : c.description || tools;
          const trailing = state.kind === 'sign_in' || state.kind === 'sign_in_again' ? (
            <CellAction
              icon={<LogIn className="h-3.5 w-3.5" aria-hidden />}
              disabled={busy === c.id}
              onClick={() => void signIn(c)}
            >
              {state.kind === 'sign_in' ? t('organization.connectors.signIn') : t('organization.connectors.signInAgain')}
            </CellAction>
          ) : state.kind === 'not_answering' ? (
            <CellState state="warn" label={t('organization.connectors.notAnswering')} />
          ) : (
            <CellState state="on" />
          );
          // The member's one verb on a connector they signed in to.
          const menu = c.auth === 'member' && c.connection_status !== null ? (
            <CellMenu label={t('mcp.row.actionsAria', { name: c.name })}>
              <DropdownMenuItem disabled={busy === c.id} onSelect={() => void signOut(c)}>
                <LogOut className="h-4 w-4" aria-hidden />
                {t('organization.connectors.signOut')}
              </DropdownMenuItem>
            </CellMenu>
          ) : undefined;
          return (
            <SurfaceCell
              key={c.id}
              mark={<BrandMark name={c.name} kind="server" size="md" />}
              name={c.name}
              meta={meta}
              line={line}
              lineTone={state.kind === 'sign_in_again' || state.kind === 'not_answering' ? 'warn' : 'default'}
              trailing={trailing}
              menu={menu}
            />
          );
        })}
      </SurfaceGrid>
    </SurfaceSection>
  );
}
