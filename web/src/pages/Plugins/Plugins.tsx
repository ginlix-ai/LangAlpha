import { lazy, Suspense, useRef } from 'react';
import { Navigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useScrollMemory } from '@/lib/scrollMemory';
import { ListSkeleton } from '@/components/mcp/McpPrimitives';
import { Brokerages } from './components/Brokerages';
import { DetailPageProvider } from './components/DetailPage';
import { McpServers } from './components/McpServers';
import { SkillsList } from './components/SkillsList';
import { SubagentsList } from './components/subagents/SubagentsList';
import { PluginSecrets } from './components/PluginSecrets';
import { PluginsList } from './components/PluginsList';
import { WorkspacesLoadNote } from './components/WorkspacesLoadNote';
import { DiscoverSummary } from './components/discover/DiscoverSummary';
import { PageHeader } from './components/shell/PageHeader';
import { PluginsRail } from './components/shell/PluginsRail';
import { YoursSummary } from './components/shell/YoursSummary';
import {
  hasDiscover,
  readSection,
  readSectionView,
  SCOPED,
  SECTION_LABEL_KEYS,
  type InstallSection,
  type Section,
} from './components/shell/sections';
import { useShellData } from './components/shell/useShellData';
import { useCatalogOn } from './hooks/useCatalogOn';
import { ADD_INTENT_TAB, ADD_PARAM, type AddIntent } from './utils/addParam';
import {
  NEW_SUBAGENT_PARAM,
  parseDetail,
  VIEW_PARAM,
  withDetail,
  type SectionView,
} from './utils/detailParam';
import { SCOPE_TAB, type DiscoverScope } from './utils/marketplace';
import './Plugins.css';

// Its own chunk: the catalog is browsed far less often than the page opens.
const Discover = lazy(() =>
  import('./components/Discover').then((m) => ({ default: m.Discover })),
);

/**
 * /plugins — the account's plugins, connectors (MCP servers), skills,
 * subagents, brokerages and vault. An enabled server or skill here reaches every
 * workspace of the user; OAuth-connected servers are bound into sandboxes
 * through the egress relay (credentials never leave the host).
 *
 * A rail of sections beside one content column. The URL is the whole state:
 * `?tab=` the section, `?view=discover` the catalog side of a Customize
 * section, `?q=` the header search, `?detail=` an item's page.
 *
 * Also where a sign-in that failed or was cancelled puts the user back (the
 * entry's page, the install wizard, the connector form), as the app shell's
 * `ConnectLandingHost` decides on reading the OAuth callback's landing.
 */

const DISCOVER_SCOPE = Object.fromEntries(
  Object.entries(SCOPE_TAB).map(([scope, tab]) => [tab, scope]),
) as Record<InstallSection, DiscoverScope>;

/** Where a link into Discover lands on a server with no catalog: the same
 *  section's own list, keeping the search and any detail but a catalog
 *  entry's, which only Discover has. */
function withoutDiscover(params: URLSearchParams, section: Section): URLSearchParams {
  const next =
    parseDetail(params)?.kind === 'catalog' ? withDetail(params, null) : new URLSearchParams(params);
  next.set('tab', section);
  next.delete(VIEW_PARAM);
  return next;
}

function SectionBody({ section, view }: { section: Section; view: SectionView }) {
  if (view === 'discover' && hasDiscover(section)) {
    return (
      <Suspense fallback={<ListSkeleton />}>
        <Discover scope={DISCOVER_SCOPE[section]} />
      </Suspense>
    );
  }
  switch (section) {
    case 'plugins':
      return <PluginsList />;
    case 'mcp':
      return <McpServers />;
    case 'skills':
      return <SkillsList />;
    case 'subagents':
      return <SubagentsList />;
    case 'brokerages':
      return <Brokerages />;
    case 'secrets':
      return <PluginSecrets />;
  }
}

function Plugins() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { t } = useTranslation();
  const shell = useShellData();
  const catalogOn = useCatalogOn();

  // The URL is the page state, not a mirror of it: derived, so back/forward
  // needs no sync effect and cannot briefly disagree with the address bar.
  // A link into Discover on a server with no catalog is the one rewrite: the
  // page reads it as the section's own list at once, and the address follows.
  const section = readSection(searchParams);
  const strayDiscover =
    catalogOn === false && readSectionView(searchParams, section) === 'discover';
  const shown = strayDiscover ? withoutDiscover(searchParams, section) : searchParams;
  const detailRef = parseDetail(shown);
  // The new-subagent form is a page of its own, like a detail.
  const pageOpen = !!detailRef || (section === 'subagents' && shown.has(NEW_SUBAGENT_PARAM));
  const view = readSectionView(shown, section);
  const query = searchParams.get('q') ?? '';
  const sectionLabel = t(SECTION_LABEL_KEYS[section]);

  // Each section and view keeps its own place, so a short page is not opened
  // halfway down because the long one before it was.
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, `page:plugins:${section}:${view}`);

  // The one Add entry point for the whole page: name the section and the
  // intent in one navigation, and let that section's list act on the intent
  // and strip it.
  const requestAdd = (intent: AddIntent) => {
    setSearchParams(
      { tab: ADD_INTENT_TAB[intent], [ADD_PARAM]: intent },
      { replace: true },
    );
  };

  const setView = (next: SectionView) => {
    const params = withDetail(searchParams, null);
    // The legacy Discover tab names the view itself; it becomes Plugins.
    if (params.get('tab') === 'discover') params.set('tab', 'plugins');
    if (!params.get('tab')) params.set('tab', section);
    if (next === 'discover') params.set(VIEW_PARAM, 'discover');
    else params.delete(VIEW_PARAM);
    setSearchParams(params, { replace: true });
  };

  const setQuery = (next: string) => {
    setSearchParams(
      (prev) => {
        const params = new URLSearchParams(prev);
        if (next) params.set('q', next);
        else params.delete('q');
        return params;
      },
      { replace: true },
    );
  };

  return (
    <div className="plugins-page">
      {strayDiscover && <Navigate replace to={`?${shown.toString()}`} />}
      {/* Doubles as the window titlebar in the desktop shell; inert
          elsewhere. Above the rail and the scroll port rather than inside
          them, so it stays put once the page is scrolled. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div className="plugins-body">
        <PluginsRail section={section} detail={detailRef} data={shell} />
        <div ref={pageRef} className="plugins-scroll">
          <div className="plugins-column">
            {!pageOpen && (
              <PageHeader
                section={section}
                view={view}
                summary={
                  view === 'discover' && hasDiscover(section) ? (
                    <div className="plugins-summary">
                      <DiscoverSummary scope={DISCOVER_SCOPE[section]} />
                    </div>
                  ) : (
                    <YoursSummary section={section} data={shell} />
                  )
                }
                query={query}
                hasCatalog={catalogOn === true}
                onViewChange={setView}
                onQueryChange={setQuery}
                onAdd={requestAdd}
              />
            )}
            <DetailPageProvider value={{ section: sectionLabel }}>
              {/* Keyed so a section switch swaps at once with a short fade
                  rather than sliding: the rail already says where you went. */}
              <div key={`${section}:${view}`} className="plugins-content">
                {SCOPED.has(section) && <WorkspacesLoadNote />}
                <SectionBody section={section} view={view} />
              </div>
            </DetailPageProvider>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Plugins;
