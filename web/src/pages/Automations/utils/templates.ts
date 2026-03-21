import {
  TrendingUp,
  Sun,
  BarChart3,
  CalendarSearch,
  Plus,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import type { Automation } from '@/types/automation';
import { isIndexSymbol } from '../components/AutomationInlineForm';

// ── Constants ──────────────────────────────────────────────

export const COMMON_TIMEZONES = [
  'UTC',
  'America/New_York',
  'America/Chicago',
  'America/Denver',
  'America/Los_Angeles',
  'Europe/London',
  'Europe/Paris',
  'Europe/Berlin',
  'Asia/Tokyo',
  'Asia/Shanghai',
  'Asia/Singapore',
  'Australia/Sydney',
];

export const PRICE_CONDITION_TYPES = [
  { value: 'price_above', labelKey: 'automation.priceConditionAbove' },
  { value: 'price_below', labelKey: 'automation.priceConditionBelow' },
  { value: 'pct_change_above', labelKey: 'automation.pctChangeAbove' },
  { value: 'pct_change_below', labelKey: 'automation.pctChangeBelow' },
] as const;

export const PRICE_REFERENCE_OPTIONS = [
  { value: 'previous_close', labelKey: 'automation.refPreviousClose' },
  { value: 'day_open', labelKey: 'automation.refDayOpen' },
] as const;

export const RETRIGGER_MODES = [
  { value: 'one_shot', labelKey: 'automation.retriggerOneShot' },
  { value: 'recurring', labelKey: 'automation.retriggerRecurring' },
] as const;

export function isPctCondition(type: string): boolean {
  return type === 'pct_change_above' || type === 'pct_change_below';
}

function detectTimezone(): string {
  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (tz) return tz;
  } catch {
    // Intl not available
  }
  return 'America/New_York';
}

// ── Form State ─────────────────────────────────────────────

export interface FormState {
  name: string;
  description: string;
  trigger_type: string;
  cron_expression: string;
  timezone: string;
  next_run_at: string;
  agent_mode: string;
  workspace_id: string;
  instruction: string;
  thread_strategy: string;
  max_failures: number | string;
  delivery_method: string;
  price_symbol: string;
  price_condition_type: string;
  price_value: string;
  price_reference: string;
  price_retrigger_mode: string;
  price_cooldown_minutes: number | string;
}

export const INITIAL_FORM: FormState = {
  name: '',
  description: '',
  trigger_type: 'cron',
  cron_expression: '',
  timezone: detectTimezone(),
  next_run_at: '',
  agent_mode: 'flash',
  workspace_id: '',
  instruction: '',
  thread_strategy: 'new',
  max_failures: 3,
  delivery_method: '',
  price_symbol: '',
  price_condition_type: 'price_above',
  price_value: '',
  price_reference: 'previous_close',
  price_retrigger_mode: 'one_shot',
  price_cooldown_minutes: '',
};

// ── Templates ──────────────────────────────────────────────

export type TemplateId = 'price_alert' | 'morning_briefing' | 'weekly_review' | 'earnings_watch' | 'custom';

export interface AutomationTemplate {
  id: TemplateId;
  nameKey: string;
  descriptionKey: string;
  icon: LucideIcon;
  defaults: Partial<FormState>;
}

export const AUTOMATION_TEMPLATES: AutomationTemplate[] = [
  {
    id: 'custom',
    nameKey: 'automation.templateCustom',
    descriptionKey: 'automation.templateCustomDesc',
    icon: Plus,
    defaults: {},
  },
  {
    id: 'price_alert',
    nameKey: 'automation.templatePriceAlert',
    descriptionKey: 'automation.templatePriceAlertDesc',
    icon: TrendingUp,
    defaults: {
      name: 'Price Alert + Analysis',
      trigger_type: 'price',
      agent_mode: 'flash',
      instruction:
        'Analyze recent news and market sentiment for {symbol} to explain the price movement. Include key catalysts, analyst reactions, and short-term outlook.',
    },
  },
  {
    id: 'morning_briefing',
    nameKey: 'automation.templateMorningBriefing',
    descriptionKey: 'automation.templateMorningBriefingDesc',
    icon: Sun,
    defaults: {
      name: 'Morning Market Briefing',
      trigger_type: 'cron',
      cron_expression: '0 7 * * 1-5',
      timezone: 'America/New_York',
      agent_mode: 'flash',
      instruction:
        'Provide a pre-market briefing covering: overnight market moves, key economic data releases today, notable earnings reports, and sector trends to watch.',
    },
  },
  {
    id: 'weekly_review',
    nameKey: 'automation.templateWeeklyReview',
    descriptionKey: 'automation.templateWeeklyReviewDesc',
    icon: BarChart3,
    defaults: {
      name: 'Weekly Portfolio Review',
      trigger_type: 'cron',
      cron_expression: '0 22 * * 5',
      timezone: 'America/New_York',
      agent_mode: 'ptc',
      instruction:
        "Analyze my portfolio's weekly performance. Create charts showing returns vs benchmarks, sector allocation, and risk metrics. Identify positions that need attention and suggest rebalancing actions.",
    },
  },
  {
    id: 'earnings_watch',
    nameKey: 'automation.templateEarningsWatch',
    descriptionKey: 'automation.templateEarningsWatchDesc',
    icon: CalendarSearch,
    defaults: {
      name: 'Earnings Watch',
      trigger_type: 'once',
      agent_mode: 'ptc',
      instruction:
        'Run a deep earnings analysis for {symbol}: review recent financial statements, analyst estimates, historical earnings surprises, options activity, and key metrics to watch. Create a comprehensive pre-earnings report.',
    },
  },
];

export function applyTemplate(templateId: TemplateId): FormState {
  const template = AUTOMATION_TEMPLATES.find((t) => t.id === templateId);
  if (!template) return { ...INITIAL_FORM };
  return { ...INITIAL_FORM, ...template.defaults };
}

// ── Conversion helpers ─────────────────────────────────────

export function automationToFormState(automation: Automation): FormState {
  const tc = automation.trigger_config;
  const condition = tc?.conditions?.[0];
  return {
    name: (automation.name as string) || '',
    description: (automation.description as string) || '',
    trigger_type: (automation.trigger_type as string) || 'cron',
    cron_expression: (automation.cron_expression as string) || '',
    timezone: (automation.timezone as string) || detectTimezone(),
    next_run_at: automation.next_run_at ? (automation.next_run_at as string).slice(0, 16) : '',
    agent_mode: (automation.agent_mode as string) || 'flash',
    workspace_id: (automation.workspace_id as string) || '',
    instruction: (automation.instruction as string) || '',
    thread_strategy: (automation.thread_strategy as string) || 'new',
    max_failures: (automation.max_failures as number) ?? 3,
    delivery_method: automation.delivery_config?.methods?.[0] || '',
    price_symbol: tc?.symbol || '',
    price_condition_type: condition?.type || 'price_above',
    price_value: condition?.value != null ? String(condition.value) : '',
    price_reference: condition?.reference || 'previous_close',
    price_retrigger_mode:
      (tc?.retrigger?.mode as string) === 'cooldown'
        ? 'recurring'
        : (tc?.retrigger?.mode || 'one_shot'),
    price_cooldown_minutes: tc?.retrigger?.cooldown_seconds
      ? Math.round(tc.retrigger.cooldown_seconds / 60)
      : '',
  };
}

export function formStateToPayload(form: FormState): Record<string, unknown> {
  const payload: Record<string, unknown> = { ...form };

  // Replace {symbol} placeholder in instruction with actual symbol
  if (form.price_symbol && typeof payload.instruction === 'string') {
    payload.instruction = (payload.instruction as string).replaceAll(
      '{symbol}',
      form.price_symbol.toUpperCase().trim(),
    );
  }

  // Clean up price trigger form fields — they get packed into trigger_config
  delete payload.price_symbol;
  delete payload.price_condition_type;
  delete payload.price_value;
  delete payload.price_reference;
  delete payload.price_retrigger_mode;
  delete payload.price_cooldown_minutes;

  if (payload.trigger_type === 'price') {
    delete payload.cron_expression;
    delete payload.next_run_at;

    const conditionType = form.price_condition_type;
    const condition: Record<string, unknown> = {
      type: conditionType,
      value: parseFloat(form.price_value) || 0,
    };
    if (isPctCondition(conditionType)) {
      condition.reference = form.price_reference;
    }

    const retrigger: Record<string, unknown> = { mode: form.price_retrigger_mode };
    if (form.price_retrigger_mode === 'recurring' && form.price_cooldown_minutes) {
      const mins = parseInt(String(form.price_cooldown_minutes), 10);
      if (mins && mins >= 240) {
        retrigger.cooldown_seconds = mins * 60;
      }
    }

    const sym = form.price_symbol.toUpperCase().trim();
    payload.trigger_config = {
      symbol: sym,
      ...(isIndexSymbol(sym) ? { market: 'index' } : {}),
      conditions: [condition],
      retrigger,
    };
  } else if (payload.trigger_type === 'cron') {
    delete payload.next_run_at;
  } else {
    delete payload.cron_expression;
    if (payload.next_run_at) {
      payload.next_run_at = new Date(payload.next_run_at as string).toISOString();
    }
  }

  if (payload.agent_mode !== 'ptc') {
    delete payload.workspace_id;
  }

  if (!payload.description) delete payload.description;

  payload.max_failures = parseInt(String(payload.max_failures), 10) || 3;

  payload.delivery_config = payload.delivery_method
    ? { methods: [payload.delivery_method] }
    : { methods: [] };
  delete payload.delivery_method;

  return payload;
}
