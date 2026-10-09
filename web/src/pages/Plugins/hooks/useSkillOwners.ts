import { usePlugins } from '@/hooks/usePlugins';
import type { PluginInfo } from '@/pages/ChatAgent/utils/api/plugins';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api/skills';
import { isBundled } from '../utils/pluginSurface';

/**
 * The package each skill row belongs to, for the tile that falls back to its
 * mark.
 *
 * Matched on the tier as well as the name: a platform row is a bundle's and
 * any other an installed plugin's, so an upload that shares a bundle's name
 * cannot lend its mark to the bundle's skills or take theirs.
 */
export function useSkillOwners(): (
  skill: Pick<SkillInfo, 'origin' | 'plugin_name'>,
) => PluginInfo | undefined {
  const { data } = usePlugins();
  const byKey = new Map<string, PluginInfo>();
  for (const plugin of data?.plugins ?? []) {
    byKey.set(`${isBundled(plugin)}:${plugin.name}`, plugin);
  }
  return (skill) =>
    skill.plugin_name
      ? byKey.get(`${skill.origin === 'platform'}:${skill.plugin_name}`)
      : undefined;
}
