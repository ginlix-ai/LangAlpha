import { useId, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Download, ExternalLink, KeyRound, Plug, RefreshCw } from 'lucide-react';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { useBuiltinMcpServers, useMcpCatalog } from '@/hooks/useMcpServers';
import { useSkills } from '@/hooks/useSkills';
import { useSubagents } from '@/hooks/useSubagents';
import { useLocale } from '@/hooks/useLocale';
import { secretUsage } from '@/hooks/useSecretUsage';
import { useUserVaultSecrets } from '@/hooks/useUserVault';
import { mediumDate } from '@/lib/format';
import type { CatalogServer, McpSignInNeeded, PluginInfo, SkillInfo } from '@/pages/ChatAgent/utils/api';
import { detailHref } from '../utils/detailParam';
import { pluginConnectors } from '../utils/connectorState';
import { hasLifecycle, pluginMark } from '../utils/pluginSurface';
import { secretEditHref, secretSetupHref } from '../utils/secretParam';
import { webLink } from '../utils/webLink';
import { connectBlock } from '../brokerages';
import { useCatalogPlugins } from '../hooks/useCatalogPlugins';
import { connectRequestFor } from '../hooks/useMcpOauthActions';
import { useSectionNav } from '../hooks/useSectionNav';
import { useSignInOffer } from '../hooks/useSignInOffer';
import { useVendorOf } from '../hooks/useVendorOf';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import {
  DetailAsideBlock,
  DetailCallout,
  DetailFacts,
  DetailHeader,
  DetailPage,
  DetailSection,
} from './DetailPage';
import { SurfaceGrid } from './surface/Surface';
import { BuiltinConnectorCell, ConnectorCell } from './connectors/ConnectorCells';
import { PluginBareCell, SkillCell, SubagentCell } from './pluginsPage/componentCells';
import { MetaLine, OnOffControl, PageButton } from './pluginsPage/parts';
import {
  changedSubagents,
  partsLabel,
  pluginStatus,
  reasonLabel,
  sourceLabel,
  storedKeys,
  uninstallLines,
} from './pluginsPage/pluginStatus';
import { partialReachText, pluginReach, pluginReachText } from './pluginsPage/pluginReach';
import { usePluginLifecycle } from './pluginsPage/usePluginLifecycle';

/**
 * A plugin's page: what it is and where it came from, the one problem worth
 * fixing first, then what it carries (connectors, skills and subagents, each
 * opening its own page with its real state), and beside that the keys it
 * reads, its facts and its verbs. The plugin never re-renders what those
 * pages own.
 *
 * A bundle has no lifecycle: Update, Export and Uninstall all answer 404 for a
 * name with no install behind it, so its side column says it can only be
 * switched off.
 */

export function PluginDetail({
  plugin,
  onClose,
  onSignInNeeded,
}: {
  plugin: PluginInfo;
  onClose: () => void;
  /** The switch was refused until these servers are connected. */
  onSignInNeeded?: (servers: McpSignInNeeded[]) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const labelId = useId();
  const navigate = useNavigate();
  const { data: catalog } = useMcpCatalog();
  const { data: builtinData } = useBuiltinMcpServers();
  const { data: skillList } = useSkills({ includeDisabled: true, allScopes: true });
  const { data: subagentList } = useSubagents();
  const vendorOf = useVendorOf();
  const { openDetail } = useSectionNav();
  const { data: vault } = useUserVaultSecrets();
  const { workspaces, loading: workspacesLoading } = useWorkspaceOptions();
  const catalogPlugins = useCatalogPlugins();
  // A sign-in started here comes back to this page.
  const signIn = useSignInOffer(detailHref({ kind: 'plugin', name: plugin.name }));
  const life = usePluginLifecycle(plugin, {
    onSignInNeeded: onSignInNeeded ?? signIn.offer,
    // The plugin row is gone; there is nothing left to detail.
    onUninstalled: onClose,
  });

  const servers = catalog?.servers ?? [];
  const status = pluginStatus(plugin, servers);
  const { problem, fix } = status;
  const own = pluginConnectors(plugin, servers);
  const builtins = (builtinData?.servers ?? []).filter((s) => s.plugin_name === plugin.name);
  const installed = hasLifecycle(plugin);

  const mcpNames = plugin.components.filter((c) => c.kind === 'mcp').map((c) => c.name);
  const skillNames = plugin.components.filter((c) => c.kind === 'skill').map((c) => c.name);
  const subagentNames = plugin.components.filter((c) => c.kind === 'subagent').map((c) => c.name);
  // Only this plugin's: a name it ships may also be the user's own subagent.
  const pluginSubagents = (subagentList?.subagents ?? []).filter(
    (v) => v.source === 'plugin' && v.plugin.name === plugin.name,
  );
  const skillByName = new Map<string, SkillInfo>();
  for (const skill of skillList ?? []) {
    // The plugin's own row wins over a same-named skill elsewhere.
    const held = skillByName.get(skill.name);
    if (!held || (skill.plugin_name === plugin.name && held.plugin_name !== plugin.name)) {
      skillByName.set(skill.name, skill);
    }
  }
  const toolTotal = own.reduce((sum, s) => sum + (s.tool_count ?? 0), 0);
  const usage = secretUsage(servers);
  const storedNames = vault ? new Set(vault.secrets.map((s) => s.name)) : null;
  const rejectedBy = fix?.kind === 'replaceKey' ? fix.by : [];

  // Where it reaches, once every list it is read from has answered.
  const connectorParts = installed ? own : builtins;
  const skillParts = skillNames
    .map((name) => skillByName.get(name))
    .filter((s): s is SkillInfo => !!s && s.plugin_name === plugin.name);
  const partsRead =
    !workspacesLoading &&
    (mcpNames.length === 0 || (installed ? !!catalog : !!builtinData)) &&
    (skillNames.length === 0 || !!skillList);
  const reach = partsRead ? pluginReach(connectorParts, skillParts, workspaces) : null;
  const partReach = (part: CatalogServer | SkillInfo | (typeof builtins)[number]) =>
    workspacesLoading ? null : partialReachText(t, part, workspaces);

  function connect(server: CatalogServer) {
    signIn.oauth.connect(connectRequestFor(server));
  }

  /** The same gate the row's own Connect holds: a registry that has not
   *  answered, or a vendor's flow this build cannot finish, holds it. */
  const canConnect = (server: CatalogServer) => connectBlock(vendorOf(server.url)) === null;

  /** A part's own page, on the section that lists it. */
  const openComponent = (kind: 'mcp' | 'skill' | 'subagent', name: string) =>
    openDetail({ kind: kind === 'mcp' ? 'server' : kind, name });

  function openSecrets(name?: string) {
    navigate(name ? secretSetupHref(name) : '/plugins?tab=secrets');
  }

  // ── The callout ─────────────────────────────────────────────────────────
  let callout: ReactNode = null;
  const server = status.server;
  if (problem && fix && server) {
    const connectButton = (variant: 'primary' | 'outline') => (
      <PageButton
        variant={variant}
        icon={<Plug />}
        onClick={() => connect(server)}
        disabled={signIn.oauth.connectingName === server.name || !canConnect(server)}
      >
        {t('plugins.pluginsPage.callout.connect')}
      </PageButton>
    );
    if (fix.kind === 'replaceKey') {
      const count = fix.by.length;
      // "The others" only when this plugin has others to keep working.
      const others = own.length > count;
      callout = (
        <DetailCallout
          title={
            count === 1
              ? t('plugins.pluginsPage.callout.rejected', { server: server.name })
              : t('plugins.pluginsPage.callout.rejectedCount', { n: count })
          }
          action={
            <PageButton
              variant="primary"
              icon={<KeyRound />}
              onClick={() => navigate(fix.key ? secretEditHref(fix.key) : '/plugins?tab=secrets')}
            >
              {t('plugins.pluginsPage.callout.replaceKey')}
            </PageButton>
          }
        >
          {count === 1
            ? others
              ? t('plugins.pluginsPage.callout.rejectedBody')
              : t('plugins.pluginsPage.callout.rejectedBodyOne')
            : others
              ? t('plugins.pluginsPage.callout.rejectedBodyCount')
              : t('plugins.pluginsPage.callout.rejectedBodyAll')}
        </DetailCallout>
      );
    } else if (fix.kind === 'addKey') {
      callout = (
        <DetailCallout
          title={t('plugins.pluginsPage.callout.missing', { key: fix.key })}
          action={
            <div className="pp-callout-actions">
              {problem.kind === 'connect' && connectButton('outline')}
              <PageButton variant="primary" icon={<KeyRound />} onClick={() => openSecrets(fix.key)}>
                {t('plugins.pluginsPage.callout.addKey')}
              </PageButton>
            </div>
          }
        >
          {t('plugins.pluginsPage.callout.missingBody')}
        </DetailCallout>
      );
    } else if (fix.kind === 'open' && problem.kind === 'failing') {
      callout = (
        <DetailCallout
          title={t('plugins.pluginsPage.callout.failing', {
            reason: reasonLabel(t, problem.noteKey),
            server: server.name,
          })}
          action={
            <PageButton variant="primary" onClick={() => openComponent('mcp', server.name)}>
              {t('plugins.pluginsPage.callout.open', { server: server.name })}
            </PageButton>
          }
        >
          {t('plugins.pluginsPage.callout.failingBody')}
        </DetailCallout>
      );
    } else {
      callout = (
        <DetailCallout
          title={
            problem.count === 1
              ? t('plugins.pluginsPage.callout.signIn', { server: server.name })
              : t('plugins.pluginsPage.callout.signInCount', { n: problem.count })
          }
          action={connectButton('primary')}
        >
          {problem.count === 1
            ? t('plugins.pluginsPage.callout.signInBodyOne')
            : t('plugins.pluginsPage.callout.signInBody')}
        </DetailCallout>
      );
    }
  }

  // ── What it carries ─────────────────────────────────────────────────────
  // Rows the lists hold, in the plugin's order; a component the lists do not
  // hold yet still opens its page, which says why.
  const connectorCells = installed
    ? mcpNames.map((name) => {
        const row = own.find((s) => s.name === name) ?? servers.find((s) => s.name === name);
        if (!row) return <PluginBareCell key={name} name={name} onOpen={() => openComponent('mcp', name)} />;
        return (
          <ConnectorCell
            key={name}
            server={row}
            vendor={vendorOf(row.url)}
            workspaces={workspaces}
            workspacesLoading={workspacesLoading}
            connecting={signIn.oauth.connectingName === name}
            onConnect={() => connect(row)}
            onOpen={() => openComponent('mcp', name)}
          />
        );
      })
    : (builtins.length > 0 ? builtins.map((s) => s.name) : mcpNames).map((name) => {
        const row = builtins.find((s) => s.name === name);
        return row ? (
          <BuiltinConnectorCell
            key={name}
            server={row}
            workspaces={workspaces}
            workspacesLoading={workspacesLoading}
            onOpen={() => openComponent('mcp', name)}
          />
        ) : (
          <PluginBareCell key={name} name={name} onOpen={() => openComponent('mcp', name)} />
        );
      });
  const skillCells = [...skillNames]
    .sort((a, b) => a.localeCompare(b))
    .map((name) => {
      const skill = skillByName.get(name) ?? null;
      return (
        <SkillCell
          key={name}
          name={name}
          skill={skill}
          meta={skill ? partReach(skill) : null}
          onOpen={() => openComponent('skill', name)}
        />
      );
    });
  const subagentCells = [...subagentNames]
    .sort((a, b) => a.localeCompare(b))
    .map((name) => (
      <SubagentCell
        key={name}
        name={name}
        view={pluginSubagents.find((v) => v.name === name) ?? null}
        onOpen={() => openComponent('subagent', name)}
      />
    ));

  // ── Side column ─────────────────────────────────────────────────────────
  const keyBlock =
    status.keys.length > 0 ? (
      <DetailAsideBlock title={t('plugins.pluginsPage.keys')}>
        <div className="pp-keys">
          {status.keys.map((key) => {
            // Every connector that reads it, this plugin's or not: changing
            // the key changes all of them.
            const users = usage.get(key) ?? [];
            const missing =
              own.some((s) => s.probe?.missing_secrets?.includes(key)) ||
              (storedNames !== null && !storedNames.has(key));
            const rejecter = users.find((s) => rejectedBy.includes(s));
            let state: string;
            let warn = false;
            if (missing) {
              state = t('plugins.pluginsPage.keyMissingState');
              warn = true;
            } else if (rejecter) {
              state = t('plugins.pluginsPage.keyRejectedBy', { server: rejecter.name });
              warn = true;
            } else {
              state = t('plugins.pluginsPage.keyUsedBy', { count: users.length });
            }
            return (
              <div key={key} className="pp-key">
                {/* A stored key opens on its own row to replace; a missing one
                    opens the add form already named. */}
                <Link to={missing ? secretSetupHref(key) : secretEditHref(key)} className="pp-key-name pp-key-link">
                  <KeyRound className="h-3.5 w-3.5" aria-hidden />
                  <span>{key}</span>
                </Link>
                <div className={warn ? 'pp-key-state is-warn' : 'pp-key-state'}>{state}</div>
              </div>
            );
          })}
        </div>
        <p className="pp-note">
          <button type="button" className="pp-note-link" onClick={() => openSecrets()}>
            {t('plugins.pluginsPage.openSecrets')} ›
          </button>
        </p>
      </DetailAsideBlock>
    ) : null;

  const homepage = webLink(plugin.homepage);
  const repository = webLink(plugin.repository);
  const contains = [
    partsLabel(t, plugin),
    toolTotal > 0 ? t('mcp.row.toolCount', { count: toolTotal }) : null,
  ]
    .filter(Boolean)
    .join(' · ');
  const homepageLink = homepage ? <FactLink href={homepage} /> : null;
  const sourceHref = webLink(plugin.source_ref);
  const sourceFact = sourceHref
    ? <FactLink href={sourceHref} />
    : plugin.source_ref || sourceLabel(t, plugin, catalogPlugins);
  const repositoryLink = repository ? <FactLink href={repository} /> : null;
  // A plugin has no scope of its own: each part is scoped on its own page. One
  // part links there; several each carry their reach on their cell, which
  // opens that page.
  const onlyPart =
    connectorParts.length + skillParts.length === 1
      ? connectorParts[0]
        ? { kind: 'mcp' as const, name: connectorParts[0].name }
        : { kind: 'skill' as const, name: skillParts[0].name }
      : null;
  const reachFact = !reach ? null : onlyPart ? (
    <button
      type="button"
      className="pp-note-link pp-fact-button"
      onClick={() => openComponent(onlyPart.kind, onlyPart.name)}
    >
      {pluginReachText(t, reach)}
    </button>
  ) : (
    pluginReachText(t, reach)
  );
  const facts = (
    <DetailAsideBlock title={t('plugins.detail.info')}>
      <DetailFacts
        items={[
          [t('plugins.pluginsPage.contains'), contains],
          [t('plugins.pluginsPage.factWorkspaces'), reachFact],
          [t('plugins.detail.version'), plugin.version],
          [t('plugins.detail.author'), plugin.author],
          [t('plugins.detail.homepage'), homepageLink],
          [t('plugins.detail.repository'), repositoryLink],
          [t('plugins.detail.license'), plugin.license ?? null],
          [t('plugins.detail.sourceLabel'), sourceFact],
          [t('plugins.detail.installed'), plugin.installed_at ? mediumDate(new Date(plugin.installed_at), locale) : null],
          [t('plugins.detail.updated'), plugin.updated_at ? mediumDate(new Date(plugin.updated_at), locale) : null],
        ]}
      />
    </DetailAsideBlock>
  );

  const uninstallNote = uninstallLines(
    t,
    {
      connectors: mcpNames.length,
      skills: skillNames.length,
      subagents: subagentNames.length,
      changedSubagents: changedSubagents(plugin, subagentList?.subagents),
    },
    storedKeys(status.keys, storedNames),
    'before',
  );
  // The note stays under the confirm too: what goes matters most while the
  // user is deciding.
  const footer = !installed ? (
    <p className="pp-note">{t('plugins.pluginsPage.builtinNote')}</p>
  ) : (
    <div className="pp-verbs">
      {life.confirmingUninstall ? (
        <ConfirmStrip
          message={t('plugins.pluginsPage.uninstallAsk', { plugin: plugin.name })}
          confirmLabel={life.uninstalling ? t('common.loading') : t('plugins.card.uninstallConfirmYes')}
          cancelLabel={t('plugins.card.uninstallConfirmNo')}
          pending={life.uninstalling}
          onConfirm={() => void life.uninstall()}
          onCancel={life.cancelUninstall}
        />
      ) : (
        <>
          <div className="pp-verbs-row">
            <PageButton
              variant={life.updateAvailable ? 'primary' : 'outline'}
              icon={<RefreshCw />}
              onClick={life.update}
              disabled={life.busy}
            >
              {t('plugins.card.update')}
            </PageButton>
            <PageButton icon={<Download />} onClick={() => void life.exportPackage()} disabled={life.exporting}>
              {t('plugins.card.export')}
            </PageButton>
          </div>
          <PageButton onClick={life.requestUninstall} disabled={life.uninstalling}>
            {t('plugins.pluginsPage.uninstallNamed', { plugin: plugin.name })}
          </PageButton>
        </>
      )}
      {uninstallNote.length > 0 && (
        <p className="pp-note">
          {uninstallNote.map((line) => (
            <span key={line} className="block">{line}</span>
          ))}
        </p>
      )}
    </div>
  );

  const metaParts = [
    <span key="source">{sourceLabel(t, plugin, catalogPlugins)}</span>,
    plugin.version ? <code key="version" className="pp-meta-mono">v{plugin.version}</code> : null,
    life.updateAvailable ? <span key="update">{t('plugins.pluginsPage.updateAvailable')}</span> : null,
    plugin.author ? <span key="author">{t('plugins.pluginsPage.by', { author: plugin.author })}</span> : null,
  ];

  return (
    <>
      <DetailPage
        labelId={labelId}
        onClose={onClose}
        crumb={plugin.name}
        header={
          <DetailHeader
            name={plugin.name}
            labelId={labelId}
            {...pluginMark(plugin)}
            meta={<MetaLine parts={metaParts} />}
            description={plugin.description || undefined}
            controls={
              <OnOffControl
                enabled={plugin.enabled}
                name={plugin.name}
                disabled={life.busy}
                onToggle={() => void life.toggle()}
              />
            }
          />
        }
        aside={
          <>
            {keyBlock}
            {facts}
          </>
        }
        footer={footer}
      >
        {!plugin.enabled && <p className="pp-empty-line">{t('plugins.pluginsPage.offNote')}</p>}
        {callout}
        {connectorCells.length > 0 && (
          <DetailSection title={t('plugins.pluginsPage.connectors')} count={connectorCells.length}>
            <SurfaceGrid className="is-wide">{connectorCells}</SurfaceGrid>
          </DetailSection>
        )}
        {skillCells.length > 0 && (
          <DetailSection title={t('plugins.pluginsPage.skills')} count={skillCells.length}>
            <SurfaceGrid className="is-wide">{skillCells}</SurfaceGrid>
          </DetailSection>
        )}
        {subagentCells.length > 0 && (
          <DetailSection title={t('plugins.pluginsPage.subagents')} count={subagentCells.length}>
            <SurfaceGrid className="is-wide">{subagentCells}</SurfaceGrid>
          </DetailSection>
        )}
        {connectorCells.length === 0 && skillCells.length === 0 && subagentCells.length === 0 && (
          <p className="pp-empty-line">{t('plugins.pluginsPage.nothingInside')}</p>
        )}
      </DetailPage>
      {life.dialogs}
    </>
  );
}

/** A manifest link as a fact: the address without its scheme. */
function FactLink({ href }: { href: string }) {
  const shown = href.replace(/^https?:\/\//i, '').replace(/\/$/, '').replace(/\.git$/, '');
  return (
    <a href={href} target="_blank" rel="noopener noreferrer" className="pp-fact-link">
      <span>{shown}</span>
      <ExternalLink className="h-3 w-3" aria-hidden />
    </a>
  );
}
