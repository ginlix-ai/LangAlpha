import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, Plus } from 'lucide-react';
import {
  HeaderButton,
  ListEmpty,
  ListError,
  ListHeader,
  ListSkeleton,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { usePlugins } from '@/hooks/usePlugins';
import { PluginCard } from './PluginCard';
import { PluginInstallWizard } from './PluginInstallWizard';

/**
 * The Plugins tab body: installed Agent Plugins packages. Each install fans
 * components into the MCP and Skills tabs, where they appear badged with the
 * plugin's name; this list owns identity and lifecycle only.
 */

export function PluginsList() {
  const { t } = useTranslation();
  const { data, isLoading, error } = usePlugins();
  const [wizardOpen, setWizardOpen] = useState(false);

  const plugins = data?.plugins ?? [];
  const maxPlugins = data?.max_plugins ?? 0;
  const atCap = maxPlugins > 0 && plugins.length >= maxPlugins;

  return (
    <div className="flex flex-col gap-3">
      <ListHeader
        icon={Blocks}
        title={t('plugins.list.title')}
        count={plugins.length}
        max={maxPlugins}
      >
        <HeaderButton
          variant="primary"
          icon={Plus}
          onClick={() => setWizardOpen(true)}
          disabled={atCap}
          title={atCap ? t('plugins.list.atCap', { max: maxPlugins }) : undefined}
        >
          {t('plugins.list.install')}
        </HeaderButton>
      </ListHeader>

      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.list.hint')}
      </p>

      {error ? (
        <ListError>
          {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
        </ListError>
      ) : isLoading ? (
        <ListSkeleton />
      ) : plugins.length === 0 ? (
        <ListEmpty>{t('plugins.list.empty')}</ListEmpty>
      ) : (
        <div className="flex flex-col gap-1.5">
          <AnimatePresence initial={false}>
            {plugins.map((plugin) => (
              <PluginCard key={plugin.name} plugin={plugin} />
            ))}
          </AnimatePresence>
        </div>
      )}

      {wizardOpen && <PluginInstallWizard onClose={() => setWizardOpen(false)} />}
    </div>
  );
}
