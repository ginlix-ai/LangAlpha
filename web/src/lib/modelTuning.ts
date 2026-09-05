/**
 * Names and orders for the four settings a model profile can override.
 *
 * The composer menu and the Settings matrix both render these, and a level that
 * reads "High" in one place and "high" in the other looks like two different
 * settings — so the vocabulary lives here rather than beside either surface.
 */
import type { CompactionProfileName } from '@/hooks/useAllModels';

export const EFFORT_LABELS: Record<string, string> = {
  none: 'chat.modelSelector.effortNone',
  minimal: 'chat.modelSelector.effortMinimal',
  low: 'chat.modelSelector.effortLow',
  medium: 'chat.modelSelector.effortMedium',
  high: 'chat.modelSelector.effortHigh',
  xhigh: 'chat.modelSelector.effortXhigh',
  max: 'chat.modelSelector.effortMax',
};

/** Weakest first — the order a ladder is drawn in, whatever subset a model honors. */
export const EFFORT_ORDER: readonly string[] = [
  'none',
  'minimal',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
];

/** The trigger pill and its measure twin must render the same string, so both
 *  resolve the label through here rather than building the key by hand. */
export function effortLabelFor(t: (key: string) => string, level: string | null): string | null {
  return level && EFFORT_LABELS[level] ? t(EFFORT_LABELS[level]) : null;
}

/**
 * The level a model actually runs when asked for one it does not offer.
 *
 * Mirrors `LLM.resolve_reasoning_effort`: step down to the nearest level the
 * model honors, or up to its lowest when the request is under its floor. Both
 * the composer pill and the settings matrix name the inherited level, and a
 * label that disagrees with what the server runs is worse than none.
 */
export function resolveEffort(requested: string, offered: readonly string[]): string | null {
  if (offered.length === 0) return null;
  if (offered.includes(requested)) return requested;
  const ceiling = EFFORT_ORDER.indexOf(requested);
  if (ceiling < 0) return null;
  const ladder = EFFORT_ORDER.filter((lv) => offered.includes(lv));
  const atOrBelow = ladder.filter((lv) => EFFORT_ORDER.indexOf(lv) <= ceiling);
  return atOrBelow.length > 0 ? atOrBelow[atOrBelow.length - 1] : ladder[0];
}

export const COMPACTION_PROFILE_ORDER: CompactionProfileName[] = [
  'aggressive',
  'moderate',
  'extended',
  'relaxed',
];

export const GUIDANCE_LEVELS = ['lean', 'detailed'] as const;

export type GuidanceLevel = (typeof GUIDANCE_LEVELS)[number];

/**
 * Wire values stay `lean`/`detailed`; the UI says Concise/Thorough.
 *
 * The setting shapes how much scaffolding the system prompt carries, and
 * "detailed" on a control next to the composer reads as a promise about reply
 * length — which it is not.
 */
export const GUIDANCE_LABELS: Record<GuidanceLevel, string> = {
  lean: 'settings.modelTuning.guidanceLean',
  detailed: 'settings.modelTuning.guidanceDetailed',
};

export function isGuidanceLevel(value: unknown): value is GuidanceLevel {
  return value === 'lean' || value === 'detailed';
}

/**
 * The four settings a user may set per account or per model.
 *
 * The typed twin of `Tuning` in `src/llms/preferences.py`. `null` is the wire
 * value that deletes an override, so every field admits it.
 */
export interface ModelProfile {
  prompt_guidance?: GuidanceLevel | null;
  compaction_profile?: CompactionProfileName | null;
  reasoning_effort?: string | null;
  fast_mode?: boolean | null;
}

export type TuningField = keyof ModelProfile;

/** Same order as `TUNING_FIELDS` on the Python side. */
export const TUNING_FIELDS: readonly TuningField[] = [
  'prompt_guidance',
  'compaction_profile',
  'reasoning_effort',
  'fast_mode',
];

/** What the server runs when neither the account nor the model declares a
 *  level. Mirrors `DEFAULT_GUIDANCE`; lean is a strict subset, so the fail-safe
 *  is the wider one. */
export const DEFAULT_GUIDANCE: GuidanceLevel = 'detailed';

/** Speed reads the same words in the composer menu and the settings matrix, so
 *  the two resolve one vocabulary rather than each naming the keys by hand. */
export const SPEED_LABELS: Record<'standard' | 'fast', string> = {
  standard: 'chat.modelSelector.speedStandard',
  fast: 'chat.modelSelector.speedFast',
};

/** What a field falls back to when the profile is silent. Never null for the
 *  two settings that always have an answer. */
export interface InheritedTuning {
  prompt_guidance: GuidanceLevel;
  compaction_profile: string | null;
  reasoning_effort: string | null;
  fast_mode: boolean;
}

export interface EffectiveTuning {
  /** What this model overrides. Empty when it inherits everything. */
  profile: ModelProfile;
  /** Levels this model offers that the UI can name, weakest first. A level the
   *  vocabulary has no label for would render a blank option. */
  efforts: string[];
  /** What an untouched control resolves to: the account value (effort clamped
   *  into `efforts`), else what the model itself declares. */
  inherited: InheritedTuning;
  /** What the turn actually runs. */
  effective: InheritedTuning;
}

/** An empty string is how a cleared select reports itself, and it must read as
 *  "no value" rather than as a level nothing offers. */
function text(value: string | null | undefined): string | null {
  return typeof value === 'string' && value !== '' ? value : null;
}

/**
 * Per-model override, else the account-wide value.
 *
 * The twin of `resolve_tuning_field` in `src/llms/preferences.py`, which owns
 * this precedence for the turns this UI never starts (schedules, automations,
 * subagent role models). Presence, not truthiness: a field the profile carries
 * wins even when its value is empty, because clearing a field deletes the key
 * rather than storing a blank. The two are pinned together by
 * `tests/fixtures/tuning_precedence.json`.
 */
export function resolveTuningField<K extends TuningField>(
  account: ModelProfile,
  profile: ModelProfile,
  field: K,
): ModelProfile[K] {
  return field in profile ? profile[field] : account[field];
}

/**
 * Every tuning field for one model, at all three layers.
 *
 * The twin of `resolve_tuning` in `src/llms/preferences.py`, which owns the
 * same precedence for the turns this UI never starts (schedules, automations,
 * subagent role models). The two are pinned together by
 * `tests/fixtures/tuning_precedence.json`. `inherited` is the extra layer the
 * server applies at resolve time rather than storing, so a control can name
 * what "Default" will actually do instead of saying only "Default".
 *
 * `pinnedGuidance` is the deployment's own `prompt.guidance`, served as
 * `system_defaults.prompt_guidance` and null on the `auto` default. It outranks
 * what the model declares, so without it a pinned deployment labels "Default"
 * with a level no turn will ever run.
 */
export function resolveTuning(
  account: ModelProfile,
  profile: ModelProfile,
  meta?: {
    reasoning_efforts?: string[];
    reasoning_effort_default?: string;
    prompt_guidance?: string;
    compaction_profile?: string;
  } | null,
  pinnedGuidance?: string | null,
): EffectiveTuning {
  const efforts = EFFORT_ORDER.filter(
    (lv) => (meta?.reasoning_efforts ?? []).includes(lv) && EFFORT_LABELS[lv],
  );
  const declaredGuidance = isGuidanceLevel(pinnedGuidance)
    ? pinnedGuidance
    : meta?.prompt_guidance;
  const accountEffort = text(account.reasoning_effort);
  const inherited: InheritedTuning = {
    prompt_guidance:
      account.prompt_guidance ||
      (isGuidanceLevel(declaredGuidance) ? declaredGuidance : DEFAULT_GUIDANCE),
    compaction_profile: account.compaction_profile || meta?.compaction_profile || null,
    reasoning_effort: accountEffort
      ? resolveEffort(accountEffort, efforts)
      : meta?.reasoning_effort_default ?? null,
    fast_mode: account.fast_mode ?? false,
  };
  const guidance = resolveTuningField(account, profile, 'prompt_guidance');
  const compaction = resolveTuningField(account, profile, 'compaction_profile');
  const effort = text(resolveTuningField(account, profile, 'reasoning_effort'));
  const fast = resolveTuningField(account, profile, 'fast_mode');
  return {
    profile,
    efforts,
    inherited,
    effective: {
      prompt_guidance: guidance || inherited.prompt_guidance,
      compaction_profile: compaction || inherited.compaction_profile,
      reasoning_effort: effort ?? inherited.reasoning_effort,
      fast_mode: fast ?? inherited.fast_mode,
    },
  };
}
