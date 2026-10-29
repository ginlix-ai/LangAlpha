import type { TFunction } from 'i18next';
import * as z from 'zod/mini';
import { rankingFrom, readOutputFormats, type OutputFormat } from '@/lib/outputFormat';
import type { WelcomeStepId } from './paths';

/**
 * The investing profile the welcome flow asks for, in the words the server
 * and the agent's skills read it in. The values are a contract shared with
 * both, so they are spelled out here once and every screen and the save take
 * them from this module.
 */

export const INVESTOR_TYPES = ['individual', 'professional', 'student'] as const;
export type InvestorType = (typeof INVESTOR_TYPES)[number];

/** Years spent investing or trading. */
export const EXPERIENCE_LEVELS = ['under_1y', '1_3y', '3_10y', 'over_10y'] as const;
export type ExperienceLevel = (typeof EXPERIENCE_LEVELS)[number];

/** What the user wants the agent's help with. */
export const HELP_AREAS = ['ideas', 'research', 'portfolio', 'news', 'trading', 'learning'] as const;
export type HelpArea = (typeof HELP_AREAS)[number];

export const RISK_LEVELS = ['conservative', 'moderate', 'growth', 'aggressive'] as const;
export type RiskLevel = (typeof RISK_LEVELS)[number];

export const HORIZONS = ['intraday', 'days', 'months', 'years'] as const;
export type Horizon = (typeof HORIZONS)[number];

export const MARKETS = ['us', 'hk', 'cn', 'jp', 'eu'] as const;
export type Market = (typeof MARKETS)[number];

/** What can be traded. The server also keeps what the user has traded, on the
 *  same list, from when the flow asked it; nothing here reads or writes it. */
export const INSTRUMENTS = [
  'stock',
  'etf',
  'index',
  'option',
  'future',
  'bond',
  'fund',
  'forex',
  'crypto',
  'commodity',
] as const;
export type Instrument = (typeof INSTRUMENTS)[number];

/** How the user reaches a decision. */
export const APPROACHES = ['fundamental', 'macro', 'quantitative', 'technical', 'event_driven'] as const;
export type Approach = (typeof APPROACHES)[number];

export const STYLES = ['growth', 'value', 'income', 'trading', 'index'] as const;
export type InvestingStyle = (typeof STYLES)[number];

export const SECTORS = [
  'technology',
  'semiconductors',
  'healthcare',
  'financials',
  'energy',
  'consumer',
  'industrials',
  'real_estate',
  'utilities',
  'materials',
] as const;
export type Sector = (typeof SECTORS)[number];

/** The brokerages the user holds accounts with, asked when none is connected:
 *  `other` for one the list lacks (named in the kickoff, not here), `none`
 *  for no account yet, which stands alone. */
export const BROKERAGES = [
  'fidelity',
  'schwab',
  'vanguard',
  'etrade',
  'robinhood',
  'ibkr',
  'webull',
  'moomoo',
  'futu',
  'tiger',
  'longbridge',
  'tastytrade',
  'other',
  'none',
] as const;
export type BrokerageKey = (typeof BROKERAGES)[number];

/** How much work a question gets by default, which is most of the time and
 *  credits an answer costs: a quick answer with more on request, the agent's
 *  judgment, or full research before every answer. */
export const RESEARCH_DEPTHS = ['light', 'adaptive', 'thorough'] as const;
export type ResearchDepth = (typeof RESEARCH_DEPTHS)[number];

/** What an answer usually is, as the question offers it, from the quickest
 *  to the richest. The answer is the first of the ranking Settings' output
 *  format keeps, so the two cannot disagree. */
export const DELIVERABLES = ['text', 'markdown', 'visual'] as const satisfies readonly OutputFormat[];
export type Deliverable = OutputFormat;

/** When the agent may come to the user unasked. */
export const PROACTIVITY = ['proactive', 'when_needed', 'on_request'] as const;
export type Proactivity = (typeof PROACTIVITY)[number];

/** How the holdings reach the agent without a connected brokerage: as
 *  screenshots it reads, or added one row at a time by search. Asked beside
 *  the brokerages but kept with the draft, not the profile: it only shapes the
 *  opening message. */
export const HOLDINGS_METHODS = ['screenshot', 'search'] as const;
export type HoldingsMethod = (typeof HOLDINGS_METHODS)[number];

/** Whether `value` is one of `values`, and so typed as one. */
export function isOneOf<T extends readonly string[]>(values: T, value: unknown): value is T[number] {
  return (values as readonly unknown[]).includes(value);
}

export function isListedSector(value: string): value is Sector {
  return isOneOf(SECTORS, value);
}

export interface InvestingProfile {
  investorType: InvestorType | null;
  experience: ExperienceLevel | null;
  /** In the order picked: the first is what the agent offers first. */
  helpWith: HelpArea[];
  /** The user's own word: there is no second question to check it against. */
  riskLevel: RiskLevel | null;
  horizon: Horizon | null;
  /** In the order picked: the first is the market starter tasks are about. */
  markets: Market[];
  /** What the user wants to trade. */
  instruments: Instrument[];
  /** In the order picked, as `styles` is: the two take turns leading the
   *  starter tasks. */
  approaches: Approach[];
  /** In the order picked, which is the order starter tasks are offered in. */
  styles: InvestingStyle[];
  /** The listed sectors by key, and any other as it was written: sectors are
   *  the user's own words, and the agent may save one the list lacks. */
  sectors: string[];
  /** Where the user's accounts are, when none is connected. */
  brokerages: BrokerageKey[];
  /** How the agent works with them by default: how much research a question
   *  gets, what the answer usually is, and when it reaches out. */
  depth: ResearchDepth | null;
  deliverable: Deliverable | null;
  proactivity: Proactivity | null;
}

// Per-field recovery, as the onboarding prefs schema does: one value a newer
// build wrote, or a hand edit, drops only itself, and only from the screens:
// a save sends just the answers the user changed. The free-text answers the
// old interview saved sit beside these keys and are left alone.
const oneOf = <T extends readonly [string, ...string[]]>(values: T) => z.catch(z.nullable(z.enum(values)), null);

// A listed value spelled another way (" US") reads as that value, folded the
// way the server folds it before checking it.
const someOf = <T extends readonly [string, ...string[]]>(values: T) =>
  z.pipe(
    z.catch(z.array(z.unknown()), []),
    z.transform((list) => [
      ...new Set(
        list.flatMap((v) => {
          const folded = typeof v === 'string' ? v.trim().toLowerCase() : v;
          return isOneOf(values, folded) ? [folded] : [];
        }),
      ),
    ]),
  );

// A spelling of a listed sector ("Real Estate", the free-form value an older
// build saved) reads as that sector, folded the way the server folds it; any
// other is kept as written, so a save cannot drop a sector this list lacks.
const sectorList = z.pipe(
  z.catch(z.union([z.array(z.unknown()), z.string()]), []),
  z.transform((value) => {
    const list = typeof value === 'string' ? value.split(',') : value;
    const sectors = list.flatMap((v) => {
      if (typeof v !== 'string' || !v.trim()) return [];
      const slug = v.trim().toLowerCase().split(/\s+/).join('_');
      return [isListedSector(slug) ? slug : v.trim()];
    });
    return [...new Set(sectors)];
  }),
);

type ProfileSection = 'risk_preference' | 'investment_preference' | 'agent_preference';
type ProfileKey = keyof InvestingProfile;
/** One value of an answer: the answer itself, or one pick of a list. */
type Pick1<V> = V extends readonly (infer I)[] ? I : NonNullable<V>;

/** One answer of the profile: where the server keeps it, the question that
 *  asks it, and the words its card prints. */
interface Field<V> {
  section: ProfileSection;
  /** Its key in that section, in the server's and the skills' words. */
  key: string;
  /** The screen that asks it; null for one asked on a turn of its own. */
  step: WelcomeStepId | null;
  /** Left out of what the screen needs before its Continue lights up. */
  optional?: true;
  /** The stored value as the profile holds it. Never throws: a value it
   *  cannot read comes back unanswered. */
  read: { parse(value: unknown): V };
  /** One value in the words its card prints, which is also how the user's
   *  answer reads back to them. */
  label: (t: TFunction, value: Pick1<V>) => string;
}

/**
 * Every answer the profile holds, in the order the screens ask them: the
 * order a question's answer reads back in. What the user wants to trade, what
 * they want help with and the sectors are optional.
 */
export const PROFILE_FIELDS: { [K in ProfileKey]: Field<InvestingProfile[K]> } = {
  investorType: {
    section: 'investment_preference',
    key: 'investor_type',
    step: 'experience',
    read: oneOf(INVESTOR_TYPES),
    label: (t, v) => t(`onboarding.welcome.experience.types.${v}.title`),
  },
  experience: {
    section: 'investment_preference',
    key: 'experience',
    step: 'experience',
    read: oneOf(EXPERIENCE_LEVELS),
    label: (t, v) => t(`onboarding.welcome.experience.years.${v}`),
  },
  instruments: {
    section: 'investment_preference',
    key: 'asset_classes',
    step: 'experience',
    optional: true,
    read: someOf(INSTRUMENTS),
    label: (t, v) => t(`onboarding.welcome.instruments.${v}`),
  },
  riskLevel: {
    section: 'risk_preference',
    key: 'risk_level',
    step: 'risk',
    read: oneOf(RISK_LEVELS),
    label: (t, v) => t(`onboarding.welcome.risk.levels.${v}.title`),
  },
  horizon: {
    section: 'investment_preference',
    key: 'horizon',
    step: 'risk',
    read: oneOf(HORIZONS),
    label: (t, v) => t(`onboarding.welcome.risk.horizons.${v}`),
  },
  markets: {
    section: 'investment_preference',
    key: 'markets',
    step: 'markets',
    read: someOf(MARKETS),
    label: (t, v) => t(`onboarding.welcome.markets.names.${v}`),
  },
  helpWith: {
    section: 'investment_preference',
    key: 'help_with',
    step: 'markets',
    optional: true,
    read: someOf(HELP_AREAS),
    label: (t, v) => t(`onboarding.welcome.markets.help.${v}`),
  },
  approaches: {
    section: 'investment_preference',
    key: 'approaches',
    step: 'style',
    read: someOf(APPROACHES),
    label: (t, v) => t(`onboarding.welcome.style.approaches.${v}`),
  },
  styles: {
    section: 'investment_preference',
    key: 'styles',
    step: 'style',
    read: someOf(STYLES),
    label: (t, v) => t(`onboarding.welcome.style.styles.${v}`),
  },
  sectors: {
    section: 'investment_preference',
    key: 'sectors',
    step: 'style',
    optional: true,
    read: sectorList,
    label: (t, v) => sectorLabel(t, v),
  },
  // Asked on the brokerage screen's second half, which reads them back with
  // the broker typed under "other" and how the holdings come in.
  brokerages: {
    section: 'investment_preference',
    key: 'brokerages',
    step: null,
    optional: true,
    read: someOf(BROKERAGES),
    label: (t, v) => t(`onboarding.welcome.elsewhere.brokerages.${v}`),
  },
  depth: {
    section: 'agent_preference',
    key: 'research_depth',
    step: 'agent',
    read: oneOf(RESEARCH_DEPTHS),
    label: (t, v) => t(`onboarding.welcome.agent.depth.${v}.title`),
  },
  // The first of the `output_format` ranking, which Settings orders whole
  // (see readProfile and profileRow).
  deliverable: {
    section: 'agent_preference',
    key: 'output_format',
    step: 'agent',
    read: { parse: (value) => readOutputFormats({ output_format: value })?.[0] ?? null },
    label: (t, v) => t(`onboarding.welcome.agent.deliverable.${v}.title`),
  },
  proactivity: {
    section: 'agent_preference',
    key: 'proactivity',
    step: 'agent',
    read: oneOf(PROACTIVITY),
    label: (t, v) => t(`onboarding.welcome.agent.proactivity.${v}.title`),
  },
};

const PROFILE_KEYS = Object.keys(PROFILE_FIELDS) as ProfileKey[];

/** A profile built one answer at a time, each from its own key. */
function eachField(value: <K extends ProfileKey>(key: K) => InvestingProfile[K]): InvestingProfile {
  const profile: Partial<Record<ProfileKey, unknown>> = {};
  for (const key of PROFILE_KEYS) profile[key] = value(key);
  // Every key is set above; the type cannot follow a loop over them.
  return profile as InvestingProfile;
}

const asObject = (value: unknown): Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

/**
 * The profile a preferences row (or a draft written in its shape) holds.
 * Never throws: anything unreadable comes back as an unanswered question.
 */
export function readProfile(preferences: unknown): InvestingProfile {
  const row = asObject(preferences);
  const profile = eachField((key) => {
    const { section, key: stored, read } = PROFILE_FIELDS[key];
    return read.parse(asObject(row[section])[stored]);
  });
  // Read off the whole section, so a row from before the ranking (a draft, a
  // cached read) answers as the server's migration reads it.
  profile.deliverable = readOutputFormats(row.agent_preference)?.[0] ?? null;
  return profile;
}

export function emptyProfile(): InvestingProfile {
  return readProfile({});
}

/** The profile in the shape the server stores it, section by section. */
export type ProfileRow = Record<ProfileSection, Record<string, unknown>>;

/** Every answer in the shape the server stores it, as the draft keeps it. */
export function profileRow(profile: InvestingProfile): ProfileRow {
  const row: ProfileRow = { risk_preference: {}, investment_preference: {}, agent_preference: {} };
  for (const key of PROFILE_KEYS) {
    const { section, key: stored } = PROFILE_FIELDS[key];
    row[section][stored] = profile[key];
  }
  // The answer leads the ranking, the rest after it as the server orders
  // them; null leaves it to the agent, as Settings does. An unchanged answer
  // writes nothing, so an order set in Settings survives a pass through the
  // questions.
  row.agent_preference.output_format = profile.deliverable ? rankingFrom(profile.deliverable) : null;
  return row;
}

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/**
 * The one preferences write the flow makes, when the user leaves the last
 * question: only the keys whose answer differs from `saved`, the row the flow
 * opened on, or null when none does. The server merges each section key by
 * key, so a key left out keeps what the row holds (a value the agent wrote
 * since, or one this build cannot read), and a list the user cleared is sent
 * empty.
 */
export function profilePatch(profile: InvestingProfile, saved: InvestingProfile): Partial<ProfileRow> | null {
  const next = profileRow(profile);
  const before = profileRow(saved);
  const patch: Partial<ProfileRow> = {};
  for (const section of Object.keys(next) as ProfileSection[]) {
    const changed = Object.entries(next[section]).filter(([key, value]) => !same(value, before[section][key]));
    if (changed.length > 0) patch[section] = Object.fromEntries(changed);
  }
  return Object.keys(patch).length > 0 ? patch : null;
}

/**
 * A draft's answers, given on top of `base` (the saved profile they were
 * seeded from), moved onto `current` (the saved profile now). A question
 * whose answer differs from its base was answered: it keeps the answer, and
 * the base too, so it still reads as answered on the next resume. Any other
 * takes the current value, which the agent may have changed since; a resumed
 * draft must not write back a value it only showed.
 */
export function resumeAnswers(
  answers: InvestingProfile,
  base: InvestingProfile,
  current: InvestingProfile,
): { profile: InvestingProfile; base: InvestingProfile } {
  const given = (key: ProfileKey) => !same(answers[key], base[key]);
  return {
    profile: eachField((key) => (given(key) ? answers[key] : current[key])),
    base: eachField((key) => (given(key) ? base[key] : current[key])),
  };
}

/** The answers a screen asks, in the order its card asks them. */
export function stepFields(step: WelcomeStepId): ProfileKey[] {
  return PROFILE_KEYS.filter((key) => PROFILE_FIELDS[key].step === step);
}

/** What a screen needs before Continue lights up: each answer it requires.
 *  True for a screen that asks for none. */
export function answered(step: WelcomeStepId, profile: InvestingProfile): boolean {
  return stepFields(step).every((key) => {
    const value: unknown = profile[key];
    return PROFILE_FIELDS[key].optional || (Array.isArray(value) ? value.length > 0 : value !== null);
  });
}

/** An answer in the words its card prints, one per value picked. */
export function answerLabels<K extends ProfileKey>(t: TFunction, key: K, value: InvestingProfile[K]): string[] {
  const values = (Array.isArray(value) ? value : value === null ? [] : [value]) as Pick1<InvestingProfile[K]>[];
  return values.map((v) => PROFILE_FIELDS[key].label(t, v));
}

/** The sectors in `lists` the screen does not offer, each once, in order. */
export function unlistedSectors(...lists: readonly string[][]): string[] {
  return [...new Set(lists.flat().filter((sector) => !isListedSector(sector)))];
}

/** A sector as the user reads it: a listed one in their language, any other
 *  as it was written, a slug's underscores read as spaces. */
export function sectorLabel(t: TFunction, sector: string): string {
  return isListedSector(sector) ? t(`onboarding.welcome.style.sectors.${sector}`) : sector.replaceAll('_', ' ');
}

/** The brokerages as picked: "No account yet" stands alone, so picking it
 *  clears the rest, and picking any other clears it. */
export function pickBrokerages(picked: readonly BrokerageKey[], next: BrokerageKey[]): BrokerageKey[] {
  const added = next.find((key) => !picked.includes(key));
  return added === 'none' ? ['none'] : next.filter((key) => key !== 'none');
}
