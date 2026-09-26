import type {
  Automation,
  AutomationPayload,
  AutomationUpdatePayload,
  PriceCondition,
  PriceTriggerConfig,
  TriggerType,
} from '@/types/automation';
import { deviceTimezone } from '@/lib/deviceTimezone';
import { DEFAULT_CRON } from './cron';
import { rezone } from './moments';
import { MIN_COOLDOWN_MINUTES, isIndexSymbol, isPctCondition, isValidSymbol, normalizeSymbol } from './price';

// ── Form State ─────────────────────────────────────────────

type Retrigger = PriceTriggerConfig['retrigger']['mode'];

/** The form as it is edited. Numbers a field may hold empty, the price level
 *  and the cooldown, stay strings until the payload reads them. */
export interface FormState {
  name: string;
  description: string;
  trigger_type: TriggerType;
  cron_expression: string;
  timezone: string;
  next_run_at: string;
  agent_mode: Automation['agent_mode'];
  workspace_id: string;
  instruction: string;
  thread_strategy: Automation['thread_strategy'];
  max_failures: number;
  /** Every channel the automation delivers to. The form offers one of Slack
   *  or Discord, but the agent can set others, and an edit keeps them. */
  delivery_methods: string[];
  price_symbol: string;
  price_condition_type: PriceCondition['type'];
  price_value: string;
  price_reference: NonNullable<PriceCondition['reference']>;
  price_retrigger_mode: Retrigger;
  price_cooldown_minutes: string;
}

export type FormPatch = <K extends keyof FormState>(key: K, value: FormState[K]) => void;

export const INITIAL_FORM: FormState = {
  name: '',
  description: '',
  trigger_type: 'cron',
  // The builder's own starting schedule, so a form nobody touched reads as
  // untouched rather than as changed by the builder filling it in.
  cron_expression: DEFAULT_CRON,
  timezone: deviceTimezone(),
  next_run_at: '',
  agent_mode: 'flash',
  workspace_id: '',
  instruction: '',
  thread_strategy: 'continue',
  max_failures: 3,
  delivery_methods: [],
  price_symbol: '',
  price_condition_type: 'price_above',
  price_value: '',
  price_reference: 'previous_close',
  price_retrigger_mode: 'one_shot',
  price_cooldown_minutes: '',
};

export function automationToFormState(automation: Automation, timezone = deviceTimezone()): FormState {
  const tc = automation.trigger_config;
  const condition = tc?.conditions?.[0];
  // A config written before retrigger modes were renamed still says 'cooldown'.
  const mode: string | undefined = tc?.retrigger?.mode;
  const cooldown = tc?.retrigger?.cooldown_seconds;
  return {
    name: automation.name,
    description: automation.description ?? '',
    trigger_type: automation.trigger_type,
    cron_expression: automation.cron_expression ?? '',
    timezone: automation.timezone || timezone,
    // The instant as the server sent it. Cutting it to a wall-clock string
    // read UTC digits back as local time and moved the run by the offset.
    next_run_at: automation.next_run_at ?? '',
    agent_mode: automation.agent_mode,
    workspace_id: automation.workspace_id ?? '',
    instruction: automation.instruction,
    thread_strategy: automation.thread_strategy,
    max_failures: automation.max_failures,
    delivery_methods: automation.delivery_config?.methods ?? [],
    price_symbol: tc?.symbol ?? '',
    price_condition_type: condition?.type ?? 'price_above',
    price_value: condition?.value != null ? String(condition.value) : '',
    price_reference: condition?.reference ?? 'previous_close',
    price_retrigger_mode: mode === 'recurring' || mode === 'cooldown' ? 'recurring' : 'one_shot',
    price_cooldown_minutes: cooldown ? String(Math.round(cooldown / 60)) : '',
  };
}

/** A new zone for the form. A one-time run keeps the time it reads: 2:15 PM
 *  stays 2:15 PM in the new zone, so the moment itself moves. */
export function withZone(form: FormState, timezone: string): FormState {
  const at = form.trigger_type === 'once' && form.next_run_at ? new Date(form.next_run_at) : null;
  const moved = at && !Number.isNaN(at.getTime()) ? rezone(at, form.timezone, timezone).toISOString() : form.next_run_at;
  return { ...form, timezone, next_run_at: moved };
}

/** Whether the form differs from what it opened with, which is what makes
 *  leaving it a loss worth asking about. */
export function isFormChanged(initial: FormState, form: FormState): boolean {
  return (Object.keys(initial) as (keyof FormState)[]).some((key) => {
    const a = initial[key];
    const b = form[key];
    if (Array.isArray(a) && Array.isArray(b)) return a.length !== b.length || a.some((v, i) => v !== b[i]);
    return a !== b;
  });
}

// ── Validation ─────────────────────────────────────────────

/** Where a form that cannot be saved goes wrong: the message to show, and
 *  the part of the form holding the field, so a folded part can open. */
export interface FormProblem {
  messageKey: string;
  section: 'when' | 'instruction' | 'more';
}

/** How a template's instruction names its ticker. Only a price trigger has
 *  a symbol to fill it with. */
const SYMBOL_PLACEHOLDER = '{symbol}';

function fillSymbol(form: FormState): string {
  const symbol = normalizeSymbol(form.price_symbol);
  return symbol ? form.instruction.replaceAll(SYMBOL_PLACEHOLDER, symbol) : form.instruction;
}

/** The cooldown as whole minutes, or null when none was typed. The validator
 *  and the serializer both read it here, because a number field hands over
 *  `1e3` as typed and `parseInt` reads that as 1. */
function cooldownMinutes(text: string): number | null {
  const trimmed = text.trim();
  const mins = Number(trimmed);
  return trimmed && Number.isInteger(mins) ? mins : null;
}

/**
 * The server's own rules, checked before the round trip so the reader gets
 * a sentence and the field instead of a 422. `initial` is what an edit
 * opened with: a one-time moment left alone is not the edit's to judge, so
 * a finished run can still be renamed.
 */
export function validateForm(form: FormState, now = Date.now(), initial?: FormState): FormProblem | null {
  if (form.trigger_type === 'once' && form.next_run_at !== initial?.next_run_at) {
    const at = Date.parse(form.next_run_at);
    if (Number.isNaN(at)) return { messageKey: 'automation.onceHint', section: 'when' };
    if (at <= now) return { messageKey: 'automation.oncePassed', section: 'when' };
  }
  if (form.trigger_type === 'price') {
    if (!isValidSymbol(form.price_symbol)) return { messageKey: 'automation.priceSymbolInvalid', section: 'when' };
    const level = Number(form.price_value);
    if (!form.price_value.trim() || !Number.isFinite(level) || level <= 0) {
      return { messageKey: 'automation.priceValuePositive', section: 'when' };
    }
    if (form.price_retrigger_mode === 'recurring' && form.price_cooldown_minutes.trim()) {
      const mins = cooldownMinutes(form.price_cooldown_minutes);
      if (mins === null || mins < MIN_COOLDOWN_MINUTES) {
        return { messageKey: 'automation.cooldownTooShort', section: 'more' };
      }
    }
  }
  // Saved unfilled, the agent would be asked about "{symbol}" itself.
  if (fillSymbol(form).includes(SYMBOL_PLACEHOLDER)) {
    return { messageKey: 'automation.instructionSymbolUnfilled', section: 'instruction' };
  }
  if (form.agent_mode === 'ptc' && !form.workspace_id) {
    return { messageKey: 'automation.workspaceRequired', section: 'more' };
  }
  return null;
}

// ── Payloads ───────────────────────────────────────────────

/** The price fields of the form as the trigger config the server stores, so
 *  the form can preview exactly what it will save. */
export function formToPriceConfig(form: FormState): PriceTriggerConfig {
  const type = form.price_condition_type;
  const condition: PriceCondition = { type, value: parseFloat(form.price_value) || 0 };
  if (isPctCondition(type)) condition.reference = form.price_reference;

  const retrigger: PriceTriggerConfig['retrigger'] = { mode: form.price_retrigger_mode };
  if (retrigger.mode === 'recurring') {
    const mins = cooldownMinutes(form.price_cooldown_minutes);
    if (mins !== null && mins >= MIN_COOLDOWN_MINUTES) retrigger.cooldown_seconds = mins * 60;
  }

  const symbol = normalizeSymbol(form.price_symbol);
  return {
    symbol,
    ...(isIndexSymbol(symbol) ? { market: 'index' as const } : {}),
    conditions: [condition],
    retrigger,
  };
}

type Settings = Pick<
  AutomationPayload,
  'name' | 'agent_mode' | 'instruction' | 'thread_strategy' | 'max_failures' | 'delivery_config' | 'workspace_id'
>;
type Trigger = Pick<AutomationUpdatePayload, 'cron_expression' | 'next_run_at' | 'timezone' | 'trigger_config'>;

/** What every save states, whatever starts the automation. */
function settingsPayload(form: FormState): Settings {
  const settings: Settings = {
    name: form.name,
    agent_mode: form.agent_mode,
    instruction: fillSymbol(form),
    thread_strategy: form.thread_strategy,
    max_failures: form.max_failures,
    delivery_config: { methods: form.delivery_methods },
  };
  if (form.agent_mode === 'ptc') settings.workspace_id = form.workspace_id;
  return settings;
}

/** What starts the automation. A schedule and a moment are read in their
 *  zone, so the zone goes with them. */
function triggerPayload(form: FormState): Trigger {
  switch (form.trigger_type) {
    case 'cron':
      return { cron_expression: form.cron_expression, timezone: form.timezone };
    case 'once':
      return form.next_run_at ? { next_run_at: new Date(form.next_run_at).toISOString(), timezone: form.timezone } : {};
    case 'price':
      return { trigger_config: formToPriceConfig(form) };
  }
}

export function formStateToPayload(form: FormState): AutomationPayload {
  return {
    ...settingsPayload(form),
    ...(form.description ? { description: form.description } : {}),
    trigger_type: form.trigger_type,
    timezone: form.timezone,
    ...triggerPayload(form),
  };
}

const CONDITION_FIELDS = ['price_condition_type', 'price_value', 'price_reference'] as const;
const RETRIGGER_FIELDS = ['price_retrigger_mode', 'price_cooldown_minutes'] as const;

/** The fields each kind of trigger is built from. */
const TRIGGER_FIELDS: Record<TriggerType, readonly (keyof FormState)[]> = {
  cron: ['cron_expression', 'timezone'],
  once: ['next_run_at', 'timezone'],
  price: ['price_symbol', ...CONDITION_FIELDS, ...RETRIGGER_FIELDS],
};

/**
 * The saved trigger with the form's changes laid over it. The form edits one
 * condition and rounds the cooldown to minutes, so the config rebuilt from
 * the form would drop any further condition the agent wrote, an explicit
 * market, and a cooldown that is not whole minutes. Only the parts the reader
 * changed are taken from it.
 */
function mergePriceConfig(
  saved: PriceTriggerConfig,
  edited: PriceTriggerConfig,
  form: FormState,
  initial: FormState,
): PriceTriggerConfig {
  const out: PriceTriggerConfig = { ...saved, conditions: [...saved.conditions] };
  if (form.price_symbol !== initial.price_symbol) {
    out.symbol = edited.symbol;
    if (edited.market) out.market = edited.market;
    else delete out.market;
  }
  if (CONDITION_FIELDS.some((k) => form[k] !== initial[k])) out.conditions[0] = edited.conditions[0];
  if (RETRIGGER_FIELDS.some((k) => form[k] !== initial[k])) out.retrigger = edited.retrigger;
  return out;
}

/**
 * An edit as the patch the server applies: the settings every save restates,
 * and the trigger only where it changed. A schedule restated unchanged would
 * still make the server recompute the next run, and a price config restated
 * from the form would lose what the form cannot show (see mergePriceConfig).
 * The description always goes, since an emptied one is a change to clear it.
 */
export function formStateToUpdatePayload(form: FormState, initial: FormState, original: Automation): AutomationUpdatePayload {
  const payload: AutomationUpdatePayload = { ...settingsPayload(form), description: form.description };
  if (!TRIGGER_FIELDS[form.trigger_type].some((k) => form[k] !== initial[k])) return payload;
  const trigger = triggerPayload(form);
  const saved = original.trigger_config;
  if (trigger.trigger_config && saved) trigger.trigger_config = mergePriceConfig(saved, trigger.trigger_config, form, initial);
  return { ...payload, ...trigger };
}
