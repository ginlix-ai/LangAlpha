import type { ComponentType } from 'react';
import {
  Bot,
  CandlestickChart,
  CircleUser,
  Cpu,
  CreditCard,
  FlaskConical,
  MessageSquare,
  Palette,
  PieChart,
  Plug,
  SlidersHorizontal,
} from 'lucide-react';
import { isPlatformMode } from '@/config/hostMode';
import { TRADING_PERMISSION_ANCHOR } from '@/lib/tradingPermission';

interface SectionMeta {
  labelKey: string;
  /** What the section holds, in a line under its title. */
  descKey: string;
  icon: ComponentType<{ className?: string }>;
  /** Points into the hosted account console, which a self-hosted build has none of. */
  platformOnly?: boolean;
}

/**
 * The Settings page's sections, keyed by the `?tab=` values links already
 * carry: `model` and `preferences` (the investing profile) kept their names
 * when the page was regrouped, so every link written before still lands.
 */
export const SECTIONS = {
  account: { labelKey: 'settings.section.account', descKey: 'settings.definition.account', icon: CircleUser },
  preferences: { labelKey: 'settings.section.preferences', descKey: 'settings.definition.preferences', icon: PieChart },
  plan: { labelKey: 'settings.section.plan', descKey: 'settings.definition.plan', icon: CreditCard, platformOnly: true },
  general: { labelKey: 'settings.section.general', descKey: 'settings.definition.general', icon: SlidersHorizontal },
  appearance: { labelKey: 'settings.section.appearance', descKey: 'settings.definition.appearance', icon: Palette },
  chat: { labelKey: 'settings.section.chat', descKey: 'settings.definition.chat', icon: MessageSquare },
  agent: { labelKey: 'settings.section.agent', descKey: 'settings.definition.agent', icon: Bot },
  model: { labelKey: 'settings.section.model', descKey: 'settings.definition.model', icon: Cpu },
  integrations: { labelKey: 'settings.section.integrations', descKey: 'settings.definition.integrations', icon: Plug, platformOnly: true },
  trading: { labelKey: 'settings.section.trading', descKey: 'settings.definition.trading', icon: CandlestickChart },
  experiments: { labelKey: 'settings.section.experiments', descKey: 'settings.definition.experiments', icon: FlaskConical },
} satisfies Record<string, SectionMeta>;

export type Section = keyof typeof SECTIONS;

/** Whether this build draws the section. */
export function sectionShown(section: Section): boolean {
  const meta: SectionMeta = SECTIONS[section];
  return isPlatformMode || !meta.platformOnly;
}

export const GROUPS: readonly { labelKey: string; sections: readonly Section[] }[] = [
  { labelKey: 'settings.group.account', sections: ['account', 'preferences', 'plan'] },
  { labelKey: 'settings.group.app', sections: ['general', 'appearance', 'chat'] },
  { labelKey: 'settings.group.agent', sections: ['agent', 'model', 'integrations', 'trading'] },
  { labelKey: 'settings.group.labs', sections: ['experiments'] },
];

/**
 * One setting: the section it lives in, the anchor its row carries, and the
 * name and line it is drawn with. The rows take their copy from here, so the
 * search finds a setting by exactly the words its row shows.
 */
export interface SettingEntry {
  section: Section;
  anchor: string;
  labelKey: string;
  descKey?: string;
  /** Drawn only in the desktop app, so only found there. */
  desktopOnly?: boolean;
  /** Drawn only in the Models section's advanced view, which opening it turns on. */
  advanced?: boolean;
  /** Names of rows drawn only some of the time beside this one, which a
   *  search finds here so it never opens on a row that is not there. */
  alsoKeys?: readonly string[];
}

export const SETTINGS = {
  photo: { section: 'account', anchor: 'photo', labelKey: 'settings.photo', descKey: 'settings.photoDesc' },
  name: { section: 'account', anchor: 'name', labelKey: 'common.name', descKey: 'settings.nameDesc' },
  email: { section: 'account', anchor: 'email', labelKey: 'common.email', descKey: 'settings.emailCannotBeChanged' },
  logout: { section: 'account', anchor: 'sign-out', labelKey: 'settings.logout', descKey: 'settings.logoutDesc' },
  investingAnswers: { section: 'preferences', anchor: 'investing-profile', labelKey: 'settings.investingAnswers', descKey: 'settings.preferencesDesc' },
  resetPreferences: { section: 'preferences', anchor: 'reset-preferences', labelKey: 'settings.resetPreferences', descKey: 'settings.resetPreferencesDesc' },
  plan: { section: 'plan', anchor: 'plan', labelKey: 'settings.planUsage.plan', descKey: 'settings.planUsage.planDesc' },
  usage: { section: 'plan', anchor: 'usage', labelKey: 'settings.planUsage.usage', descKey: 'settings.planUsage.usageDesc' },
  billing: { section: 'plan', anchor: 'billing', labelKey: 'settings.planUsage.billing', descKey: 'settings.planUsage.billingDesc' },
  language: { section: 'general', anchor: 'language', labelKey: 'settings.locale', descKey: 'settings.localeDesc' },
  timezone: { section: 'general', anchor: 'timezone', labelKey: 'settings.timezone', descKey: 'settings.timezoneDesc' },
  linkTarget: { section: 'general', anchor: 'link-target', labelKey: 'settings.shareLinkOpen', descKey: 'settings.shareLinkOpenDesc', desktopOnly: true },
  guides: { section: 'general', anchor: 'guides', labelKey: 'onboarding.settings.sectionTitle', descKey: 'onboarding.settings.description' },
  theme: { section: 'appearance', anchor: 'theme', labelKey: 'settings.theme', descKey: 'settings.themeDesc' },
  fontSize: { section: 'appearance', anchor: 'font-size', labelKey: 'settings.fontSize', descKey: 'settings.fontSizeDesc' },
  cardStyle: { section: 'appearance', anchor: 'card-style', labelKey: 'settings.cardStyle', descKey: 'settings.cardStyleDesc' },
  turnDisplay: { section: 'chat', anchor: 'stream-reasoning', labelKey: 'settings.turnDisplay', descKey: 'settings.turnDisplayDesc' },
  streamingMode: { section: 'chat', anchor: 'response-delivery', labelKey: 'settings.streamingMode', descKey: 'settings.streamingModeDesc' },
  toolCallLayout: { section: 'chat', anchor: 'tool-calls', labelKey: 'settings.toolCallLayout', descKey: 'settings.toolCallLayoutDesc' },
  turnEndScroll: { section: 'chat', anchor: 'turn-end', labelKey: 'settings.turnEndScroll', descKey: 'settings.turnEndScrollDesc' },
  voiceInput: { section: 'chat', anchor: 'voice-input', labelKey: 'settings.voiceInput', descKey: 'settings.voiceInputDesc' },
  outputFormat: { section: 'agent', anchor: 'output-format', labelKey: 'settings.outputFormat', descKey: 'settings.outputFormatDesc' },
  researchDepth: { section: 'agent', anchor: 'research-depth', labelKey: 'settings.researchDepth', descKey: 'settings.researchDepthDesc' },
  reachingOut: { section: 'agent', anchor: 'reaching-out', labelKey: 'settings.reachingOut', descKey: 'settings.reachingOutDesc' },
  subagents: { section: 'agent', anchor: 'subagents', labelKey: 'settings.subagentsDefault', descKey: 'settings.subagentsDefaultDesc' },
  approveHandoffs: { section: 'agent', anchor: 'approve-handoffs', labelKey: 'settings.autoApproveHandoffs', descKey: 'settings.autoApproveHandoffsDesc' },
  approveWorkspaces: { section: 'agent', anchor: 'approve-workspaces', labelKey: 'settings.autoApproveWorkspaceCreation', descKey: 'settings.autoApproveWorkspaceCreationDesc' },
  searchProvider: { section: 'agent', anchor: 'search-provider', labelKey: 'settings.searchProvider', descKey: 'settings.searchProviderDesc', alsoKeys: ['settings.searchDepth'] },
  modelAccess: { section: 'model', anchor: 'model-access', labelKey: 'modelAccess.title', descKey: 'modelAccess.description' },
  defaultModels: { section: 'model', anchor: 'default-models', labelKey: 'settings.defaultModels', descKey: 'settings.defaultModelsDesc' },
  defaultModelScope: { section: 'model', anchor: 'default-model-scope', labelKey: 'settings.defaultModelChange.scopeLabel', descKey: 'settings.defaultModelChange.scopeDesc' },
  quickAccess: { section: 'model', anchor: 'quick-access', labelKey: 'settings.starredModels', descKey: 'settings.starredModelsDesc' },
  modelDetail: { section: 'model', anchor: 'model-detail', labelKey: 'settings.modelTuning.showAdvanced', descKey: 'settings.modelTuning.showAdvancedDesc' },
  accountDefaults: { section: 'model', anchor: 'account-defaults', labelKey: 'settings.modelTuning.sectionDefaults', advanced: true },
  perModel: { section: 'model', anchor: 'per-model', labelKey: 'settings.modelTuning.perModel', descKey: 'settings.modelTuning.perModelDesc', advanced: true },
  routing: { section: 'model', anchor: 'routing', labelKey: 'settings.modelTuning.sectionRouting', descKey: 'settings.fallbackModels', advanced: true },
  chatApps: { section: 'integrations', anchor: 'chat-apps', labelKey: 'settings.integrations.chatApps', descKey: 'settings.integrations.chatAppsDesc' },
  connectedApps: { section: 'integrations', anchor: 'connected-apps', labelKey: 'settings.integrations.connectedApps', descKey: 'settings.integrations.connectedAppsDesc' },
  tradingPermission: { section: 'trading', anchor: TRADING_PERMISSION_ANCHOR, labelKey: 'settings.tradingPermission.title', descKey: 'settings.tradingPermission.desc' },
  experiments: { section: 'experiments', anchor: 'experiments', labelKey: 'settings.experiments', descKey: 'settings.experimentsIntro' },
} satisfies Record<string, SettingEntry>;

const ENTRIES: readonly SettingEntry[] = Object.values(SETTINGS);

/** The setting a URL hash names, if any. */
export function settingAt(hash: string): SettingEntry | undefined {
  const anchor = hash.replace(/^#/, '');
  return anchor ? ENTRIES.find((entry) => entry.anchor === anchor) : undefined;
}

/** Every setting the search can find on this surface. */
export function searchableSettings(desktop: boolean): SettingEntry[] {
  return ENTRIES.filter((entry) => (desktop || !entry.desktopOnly) && sectionShown(entry.section));
}

function asSection(param: string | null): Section | null {
  // The page before the regroup called the profile tab `userInfo`.
  if (param === 'userInfo') return 'account';
  return param !== null && Object.hasOwn(SECTIONS, param) ? (param as Section) : null;
}

/**
 * The section the URL names. A hash naming a setting wins over the tab, so a
 * link written when that setting lived elsewhere (Trading permission under
 * `?tab=preferences`) still opens on it.
 */
export function readSection(params: URLSearchParams, hash: string): Section {
  return [settingAt(hash)?.section, asSection(params.get('tab'))].find((s) => s && sectionShown(s)) ?? 'account';
}
