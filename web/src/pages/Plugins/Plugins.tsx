import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useScrollMemory } from '@/lib/scrollMemory';
import { toast } from '@/components/ui/use-toast';
import { McpServers } from './components/McpServers';
import { SkillsList } from './components/SkillsList';
import { PluginSecrets } from './components/PluginSecrets';
import { PluginsList } from './components/PluginsList';
import './Plugins.css';

/**
 * /plugins — user-level MCP servers, skills and the user vault. An enabled
 * server or skill here reaches every workspace of the user; OAuth-connected
 * servers are bound into sandboxes through the egress relay (credentials
 * never leave the host).
 *
 * Also the landing route of the OAuth connect flow: the backend callback
 * redirects here with `?mcp_connected=<server>` or `?mcp_error=<reason>&server=`
 * — surfaced as a toast, then stripped from the URL.
 */

const TABS = ['plugins', 'mcp', 'skills', 'secrets'] as const;
type Tab = (typeof TABS)[number];

// Explicit key map (not a template literal) so the i18n parity test can see
// every tab label.
const TAB_LABEL_KEYS: Record<Tab, string> = {
  plugins: 'plugins.tabs.plugins',
  mcp: 'plugins.tabs.mcp',
  skills: 'plugins.tabs.skills',
  secrets: 'plugins.tabs.secrets',
};

/** Old deep links: /connectors?tab=servers → the mcp tab. */
function resolveTab(param: string | null): Tab | null {
  if (param === 'servers') return 'mcp';
  return TABS.includes(param as Tab) ? (param as Tab) : null;
}

function Plugins() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { t } = useTranslation();

  const [activeTab, setActiveTab] = useState<Tab>(
    resolveTab(searchParams.get('tab')) ?? 'plugins',
  );
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, 'page:plugins');

  const handleTabChange = (tab: Tab) => {
    setActiveTab(tab);
    setSearchParams({ tab }, { replace: true });
  };

  // Sync from URL on back/forward navigation
  useEffect(() => {
    const urlTab = resolveTab(searchParams.get('tab'));
    if (urlTab && urlTab !== activeTab) {
      setActiveTab(urlTab);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  // OAuth callback landing: toast the outcome once, then strip the params so a
  // refresh doesn't re-announce it.
  const callbackHandled = useRef(false);
  useEffect(() => {
    if (callbackHandled.current) return;
    const connected = searchParams.get('mcp_connected');
    const errorReason = searchParams.get('mcp_error');
    if (!connected && !errorReason) return;
    callbackHandled.current = true;
    if (connected) {
      toast({
        title: t('plugins.oauth.connectedTitle'),
        description: t('plugins.oauth.connectedDesc', { server: connected }),
      });
    } else {
      const server = searchParams.get('server');
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.callbackErrorTitle'),
        description: server ? `${server}: ${errorReason}` : String(errorReason),
      });
    }
    const next = new URLSearchParams(searchParams);
    next.delete('mcp_connected');
    next.delete('mcp_error');
    next.delete('server');
    setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  return (
    <div ref={pageRef} className="plugins-page">
      <div className="plugins-container">
        <h2 className="text-xl font-semibold mb-1" style={{ color: 'var(--color-text-primary)' }}>
          {t('plugins.title')}
        </h2>
        <p className="text-sm mb-6" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.description')}
        </p>
        <div className="flex gap-2 mb-6 border-b overflow-x-auto plugins-tab-bar" style={{ borderColor: 'var(--color-border-muted)' }}>
          {TABS.map((tab) => (
            <button
              key={tab}
              type="button"
              onClick={() => handleTabChange(tab)}
              className="px-4 py-2 text-sm font-medium whitespace-nowrap flex-shrink-0"
              style={{
                color: activeTab === tab ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                borderBottom: activeTab === tab ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
              }}
            >
              {t(TAB_LABEL_KEYS[tab])}
            </button>
          ))}
        </div>

        <div className="plugins-content">
          {activeTab === 'plugins' && <PluginsList />}
          {activeTab === 'mcp' && <McpServers />}
          {activeTab === 'skills' && <SkillsList />}
          {activeTab === 'secrets' && <PluginSecrets />}
        </div>
      </div>
    </div>
  );
}

export default Plugins;
