import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import {
  EnabledToggle,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { useMcpCatalogServerTools } from '@/hooks/useMcpServers';
import { createDateFormatter } from '@/lib/format';
import type {
  BuiltinMcpServer,
  CatalogServer,
  WorkspaceScopedMcpServer,
} from '@/pages/ChatAgent/utils/api';
import {
  DetailField,
  DetailHeader,
  DetailOverlay,
  DetailSection,
} from './DetailOverlay';

/**
 * An MCP server's detail overlay, for all three origins the Plugins page
 * lists. The union keeps each origin honest about what it knows: builtins
 * carry no user config, workspace rows are summaries (their editing stays on
 * the workspace tab), catalog rows carry the full config plus the host-side
 * tool snapshot.
 */

export type ServerDetailData =
  | { origin: 'builtin'; server: BuiltinMcpServer }
  | { origin: 'user'; server: CatalogServer }
  | { origin: 'workspace'; server: WorkspaceScopedMcpServer };

const formatDate = createDateFormatter({ dateStyle: 'medium' });

export function ServerDetail({
  data,
  onClose,
  onToggle,
  toggling = false,
  workspaceName,
}: {
  data: ServerDetailData;
  onClose: () => void;
  /** Absent = the surface has no toggle for this row (render read-only). */
  onToggle?: (enabled: boolean) => void;
  toggling?: boolean;
  /** Workspace rows: the display name of the owning workspace. */
  workspaceName?: string;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const { server, origin } = data;
  // Only catalog rows have a host-side discovery snapshot to show.
  const toolsQuery = useMcpCatalogServerTools(origin === 'user' ? server.name : null);

  const catalog = origin === 'user' ? (data.server as CatalogServer) : null;
  const oauthStatus = catalog?.oauth_status ?? null;
  const oauthLabel =
    oauthStatus === 'connected'
      ? t('plugins.oauth.connected')
      : oauthStatus === 'needs_reauth'
        ? t('plugins.oauth.needsReauth')
        : oauthStatus === 'refresh_ambiguous'
          ? t('plugins.oauth.refreshAmbiguous')
          : oauthStatus === 'revoked'
            ? t('plugins.oauth.revoked')
            : null;

  return (
    <DetailOverlay
      labelId={labelId}
      onClose={onClose}
      header={
        <DetailHeader
          name={server.name}
          labelId={labelId}
          kind={t('plugins.detail.kindServer')}
          meta={
            <>
              <span>{server.transport}</span>
              {origin === 'builtin' && <span>{t('plugins.mcp.platformBadge')}</span>}
              {origin === 'workspace' && (
                <span>
                  {t('plugins.scope.inWorkspace', {
                    name: workspaceName ?? t('plugins.scope.unknownWorkspace'),
                  })}
                </span>
              )}
              {catalog?.plugin_name && (
                <span>
                  {t('plugins.component.fromPlugin', { plugin: catalog.plugin_name })}
                </span>
              )}
              {oauthLabel && <span>{oauthLabel}</span>}
            </>
          }
          controls={
            onToggle && (
              <EnabledToggle
                enabled={server.enabled ?? false}
                name={server.name}
                disabled={toggling}
                onToggle={() => onToggle(!(server.enabled ?? false))}
              />
            )
          }
        />
      }
    >
      {server.description && (
        <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {server.description}
        </p>
      )}

      {origin === 'user' && (
        <DetailSection
          title={t('plugins.detail.tools')}
          count={toolsQuery.data?.tools.length}
        >
          {toolsQuery.isLoading ? (
            <div className="flex items-center gap-2 py-3">
              <Loader size={14} className="text-current" />
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('common.loading')}
              </span>
            </div>
          ) : !toolsQuery.data || toolsQuery.data.tools.length === 0 ? (
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('plugins.detail.toolsEmpty')}
            </p>
          ) : (
            <div className="flex flex-col gap-2">
              {toolsQuery.data.tools.map((tool) => (
                <div key={tool.name} className="flex flex-col gap-0.5">
                  <span
                    className="text-[0.6875rem] font-medium break-all"
                    style={{
                      color: 'var(--color-text-secondary)',
                      fontFamily: "'JetBrains Mono', 'Menlo', monospace",
                    }}
                  >
                    {tool.name}
                  </span>
                  {tool.description && (
                    <span
                      className="text-[0.6875rem] line-clamp-2"
                      style={{ color: 'var(--color-text-tertiary)' }}
                    >
                      {tool.description}
                    </span>
                  )}
                </div>
              ))}
              {toolsQuery.data.discovered_at && (
                <span
                  className="text-[0.6875rem]"
                  style={{ color: 'var(--color-text-quaternary)' }}
                >
                  {t('plugins.detail.discovered', {
                    date: formatDate(new Date(toolsQuery.data.discovered_at)),
                  })}
                </span>
              )}
            </div>
          )}
        </DetailSection>
      )}

      <DetailSection title={t('plugins.detail.config')}>
        <div className="flex flex-col gap-1.5">
          <DetailField label={t('plugins.detail.transport')}>
            {server.transport}
          </DetailField>
          {catalog?.url && (
            <DetailField label={t('plugins.detail.url')}>{catalog.url}</DetailField>
          )}
          {catalog?.command && (
            <DetailField label={t('plugins.detail.command')}>
              {[catalog.command, ...(catalog.args ?? [])].join(' ')}
            </DetailField>
          )}
          {catalog && catalog.env_refs.length > 0 && (
            <DetailField label={t('plugins.detail.envVars')}>
              <span className="inline-flex items-center gap-1 flex-wrap">
                {catalog.env_refs.map((ref) => (
                  <TagBadge key={ref} soft>
                    {ref}
                  </TagBadge>
                ))}
              </span>
            </DetailField>
          )}
          {catalog && catalog.header_refs.length > 0 && (
            <DetailField label={t('plugins.detail.headers')}>
              <span className="inline-flex items-center gap-1 flex-wrap">
                {catalog.header_refs.map((ref) => (
                  <TagBadge key={ref} soft>
                    {ref}
                  </TagBadge>
                ))}
              </span>
            </DetailField>
          )}
        </div>
      </DetailSection>

      {catalog && (catalog.created_at || catalog.updated_at) && (
        <DetailSection title={t('plugins.detail.info')}>
          <div className="flex flex-col gap-1.5">
            {catalog.created_at && (
              <DetailField label={t('plugins.detail.created')}>
                {formatDate(new Date(catalog.created_at))}
              </DetailField>
            )}
            {catalog.updated_at && (
              <DetailField label={t('plugins.detail.updated')}>
                {formatDate(new Date(catalog.updated_at))}
              </DetailField>
            )}
          </div>
        </DetailSection>
      )}
    </DetailOverlay>
  );
}
