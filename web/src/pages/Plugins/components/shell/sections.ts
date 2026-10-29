import type { ComponentType } from 'react';
import { FilePlus, KeyRound, Landmark, MessageSquarePlus, Package, Plug, SquareSlash } from 'lucide-react';
import { SubagentsRings } from '@/components/ui/subagents-rings';
import type { AddIntent } from '../../utils/addParam';
import { DETAIL_KIND_TAB, parseDetail, readView, type SectionView } from '../../utils/detailParam';

/**
 * The Plugins page's sections, keyed by the `?tab=` values links already
 * carry (`mcp` is Connectors). Customize holds what the agent can use, most
 * with a Yours and a Discover view; Accounts holds what it signs in with.
 */
export type Section = 'plugins' | 'mcp' | 'skills' | 'subagents' | 'brokerages' | 'secrets';

export const CUSTOMIZE: readonly Section[] = ['plugins', 'mcp', 'skills', 'subagents'];
export const ACCOUNTS: readonly Section[] = ['brokerages', 'secrets'];
const ALL: readonly Section[] = [...CUSTOMIZE, ...ACCOUNTS];

// Bare literals (not a template) so the locale parity test sees every label.
export const SECTION_LABEL_KEYS: Record<Section, string> = {
  plugins: 'plugins.shell.section.plugins',
  mcp: 'plugins.shell.section.mcp',
  skills: 'plugins.shell.section.skills',
  subagents: 'plugins.shell.section.subagents',
  brokerages: 'plugins.shell.section.brokerages',
  secrets: 'plugins.shell.section.secrets',
};

// What each section holds, in a line under its title: a first visit cannot
// tell a plugin from a connector from a skill by their names alone.
export const SECTION_DEFINITION_KEYS: Record<Section, string> = {
  plugins: 'plugins.shell.definition.plugins',
  mcp: 'plugins.shell.definition.mcp',
  skills: 'plugins.shell.definition.skills',
  subagents: 'plugins.shell.definition.subagents',
  brokerages: 'plugins.shell.definition.brokerages',
  secrets: 'plugins.shell.definition.secrets',
};

export const SECTION_ICONS: Record<Section, ComponentType<{ className?: string }>> = {
  plugins: Package,
  mcp: Plug,
  skills: SquareSlash,
  subagents: SubagentsRings,
  brokerages: Landmark,
  secrets: KeyRound,
};

/** The sections that list what can be installed, each with a catalog side. */
export type InstallSection = 'plugins' | 'mcp' | 'skills';
const INSTALLS: readonly Section[] = ['plugins', 'mcp', 'skills'];

/** The sections with a header search and an Add menu: everything the agent
 *  uses, whether or not the catalog carries it yet. */
export type AddSection = InstallSection | 'subagents';

/**
 * The ways in besides the catalog, per section. Each section adds only its
 * own kind: a skill upload offered on Connectors lands on a section the user
 * did not ask for. `hintKey` is the item's second line.
 */
export interface AddRoute {
  intent: AddIntent;
  key: string;
  hintKey?: string;
  icon?: ComponentType<{ className?: string }>;
}

export const ADD_ROUTES: Record<AddSection, AddRoute[]> = {
  plugins: [{ intent: 'plugin', key: 'plugins.addMenu.installPlugin' }],
  mcp: [
    { intent: 'server', key: 'plugins.addMenu.addServer' },
    { intent: 'import', key: 'plugins.addMenu.importServers' },
  ],
  skills: [{ intent: 'skill', key: 'plugins.addMenu.uploadSkill' }],
  subagents: [
    {
      intent: 'subagent',
      key: 'plugins.addMenu.newSubagent',
      hintKey: 'plugins.addMenu.newSubagentHint',
      icon: FilePlus,
    },
    {
      intent: 'subagent-chat',
      icon: MessageSquarePlus,
      key: 'plugins.addMenu.createWithAgent',
      hintKey: 'plugins.addMenu.createWithAgentHint',
    },
  ],
};

/** Sections whose rows carry a per-workspace scope. */
export const SCOPED: ReadonlySet<Section> = new Set<Section>(['brokerages', 'mcp', 'skills']);

/** A section with a Yours and a Discover view, the catalog on the other side. */
export function hasDiscover(section: Section): section is InstallSection {
  return INSTALLS.includes(section);
}

/** A section with a header search and an Add menu: every Customize one. */
export function hasAdd(section: Section): section is AddSection {
  return CUSTOMIZE.includes(section);
}

function asSection(param: string | null | undefined): Section | null {
  // Old deep links: /connectors?tab=servers, and the Discover tab, which is
  // now Plugins with Discover showing (`readView` says so).
  if (param === 'servers') return 'mcp';
  if (param === 'discover') return 'plugins';
  return ALL.includes(param as Section) ? (param as Section) : null;
}

/**
 * The section the URL names. A `?detail=` with no tab is still unambiguous
 * about where it is listed, and only that section mounts to resolve it.
 */
export function readSection(params: URLSearchParams): Section {
  const detail = parseDetail(params);
  return (
    asSection(params.get('tab')) ??
    asSection(detail && DETAIL_KIND_TAB[detail.kind]) ??
    'plugins'
  );
}

/** Yours or Discover, only where the section has both. */
export function readSectionView(params: URLSearchParams, section: Section): SectionView {
  return hasDiscover(section) ? readView(params) : 'yours';
}
