import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import {
  EnabledToggle,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { useTogglePlugin } from '@/hooks/usePlugins';
import { formatApiErrorDetail, type PluginInfo } from '@/pages/ChatAgent/utils/api';

/**
 * One installed plugin, kept to identity + the enabled toggle. The toggle
 * suppresses every component the plugin still owns without touching their own
 * enabled flags. Update / Export / Uninstall and the component list live in
 * the detail view (`PluginDetail`), reached through `onOpen`.
 */

export function PluginCard({
  plugin,
  onOpen,
  selection,
}: {
  plugin: PluginInfo;
  /** Open this plugin's detail view (name button + row-body click). */
  onOpen?: () => void;
  /** ServerRowShell select-mode props, spread through untouched. */
  selection?: { selecting?: boolean; selected?: boolean; onSelectToggle?: () => void };
}) {
  const { t } = useTranslation();
  const toggleMutation = useTogglePlugin();

  const servers = plugin.components.filter((c) => c.kind === 'mcp');
  const skills = plugin.components.filter((c) => c.kind === 'skill');

  async function handleToggle() {
    try {
      await toggleMutation.mutateAsync({
        name: plugin.name,
        enabled: !plugin.enabled,
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  const metaParts = [
    plugin.version ? `v${plugin.version}` : null,
    plugin.source_type === 'zip'
      ? t('plugins.card.sourceZip')
      : t('plugins.card.sourceRemote'),
    t('plugins.card.componentCount', {
      servers: t('plugins.card.serverCount', { count: servers.length }),
      skills: t('plugins.card.skillCount', { count: skills.length }),
    }),
  ].filter(Boolean);

  return (
    <ServerRowShell
      testid={`plugin-card-${plugin.name}`}
      {...(selection ?? {})}
      tile={<IdentityTile name={plugin.name} />}
      onOpen={onOpen}
      main={
        <>
          <ServerNameLine name={plugin.name} onOpen={onOpen} />
          <div className="flex items-center gap-2 flex-wrap">
            <MetaText>{metaParts.join(' · ')}</MetaText>
            {!plugin.enabled && <MetaText>{t('plugins.card.disabledState')}</MetaText>}
          </div>
          {plugin.description && (
            <p
              className="text-[0.6875rem] line-clamp-2"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {plugin.description}
            </p>
          )}
        </>
      }
      actions={
        <EnabledToggle
          enabled={plugin.enabled}
          name={plugin.name}
          disabled={toggleMutation.isPending}
          onToggle={handleToggle}
        />
      }
    />
  );
}
