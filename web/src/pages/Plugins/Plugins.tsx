import { useEffect, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, useReducedMotion } from 'framer-motion';
import { ChevronDown, Plus } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { useScrollMemory } from '@/lib/scrollMemory';
import { toast } from '@/components/ui/use-toast';
import { McpServers } from './components/McpServers';
import { SkillsList } from './components/SkillsList';
import { PluginSecrets } from './components/PluginSecrets';
import { PluginsList } from './components/PluginsList';
import { ADD_INTENT_TAB, ADD_PARAM, type AddIntent } from './utils/addParam';
import { DETAIL_KIND_TAB, parseDetail } from './utils/detailParam';
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
function resolveTab(param: string | null | undefined): Tab | null {
  if (param === 'servers') return 'mcp';
  return TABS.includes(param as Tab) ? (param as Tab) : null;
}

function Plugins() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { t } = useTranslation();
  const reducedMotion = useReducedMotion();

  // The URL is the tab state, not a mirror of it: derived, so back/forward
  // needs no sync effect and cannot briefly disagree with the address bar.
  // A `?detail=` with no tab names its tab through its kind.
  const detailRef = parseDetail(searchParams);
  const activeTab: Tab =
    resolveTab(searchParams.get('tab')) ??
    resolveTab(detailRef && DETAIL_KIND_TAB[detailRef.kind]) ??
    'plugins';
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, 'page:plugins');

  const handleTabChange = (tab: Tab) => {
    setSearchParams({ tab }, { replace: true });
  };

  // The one Add entry point for the whole page: name the tab and the intent in
  // one navigation, and let that tab's list act on the intent and strip it.
  const requestAdd = (intent: AddIntent) => {
    setSearchParams(
      { tab: ADD_INTENT_TAB[intent], [ADD_PARAM]: intent },
      { replace: true },
    );
  };

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
        <div className="flex items-start justify-between gap-3 mb-6">
          <div className="min-w-0">
            <h2 className="text-xl font-semibold mb-1" style={{ color: 'var(--color-text-primary)' }}>
              {t('plugins.title')}
            </h2>
            <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('plugins.description')}
            </p>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-opacity hover:opacity-90 flex-shrink-0"
                style={{
                  color: 'var(--color-btn-primary-text)',
                  backgroundColor: 'var(--color-btn-primary-bg)',
                }}
              >
                <Plus className="h-3 w-3" />
                {t('plugins.addMenu.add')}
                <ChevronDown className="h-3 w-3" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onSelect={() => requestAdd('plugin')}>
                {t('plugins.addMenu.installPlugin')}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => requestAdd('server')}>
                {t('plugins.addMenu.addServer')}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => requestAdd('import')}>
                {t('plugins.addMenu.importServers')}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => requestAdd('skill')}>
                {t('plugins.addMenu.uploadSkill')}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        <div className="flex gap-2 mb-6 border-b overflow-x-auto plugins-tab-bar" style={{ borderColor: 'var(--color-border-muted)' }}>
          {TABS.map((tab) => (
            <button
              key={tab}
              type="button"
              onClick={() => handleTabChange(tab)}
              className="relative px-4 py-2 text-sm font-medium whitespace-nowrap flex-shrink-0 transition-colors"
              style={{
                color: activeTab === tab ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              }}
            >
              {t(TAB_LABEL_KEYS[tab])}
              {activeTab === tab && (
                <motion.span
                  layoutId="plugins-tab-underline"
                  aria-hidden
                  className="absolute inset-x-1 bottom-0 h-0.5 rounded-full"
                  style={{ backgroundColor: 'var(--color-accent-primary)' }}
                  transition={
                    reducedMotion
                      ? { duration: 0 }
                      : { type: 'spring', stiffness: 500, damping: 40 }
                  }
                />
              )}
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
