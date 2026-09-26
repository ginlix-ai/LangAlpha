import { describe, it, expect } from 'vitest';
import {
  automationToFormState,
  formStateToPayload,
  formStateToUpdatePayload,
  isFormChanged,
  validateForm,
  withZone,
  INITIAL_FORM,
  type FormState,
} from '../form';
import { applyTemplate, type TemplateId } from '../templates';
import { timeIn } from '../moments';
import type { Automation } from '@/types/automation';

function makeAutomation(overrides: Partial<Automation> = {}): Automation {
  return {
    automation_id: 'auto-1',
    user_id: 'user-1',
    name: 'Automation',
    description: null,
    trigger_type: 'cron',
    cron_expression: '0 9 * * *',
    timezone: 'UTC',
    trigger_config: null,
    next_run_at: null,
    last_run_at: null,
    agent_mode: 'flash',
    instruction: 'Summarize the market.',
    workspace_id: null,
    llm_model: null,
    thread_strategy: 'new',
    conversation_thread_id: null,
    status: 'active',
    max_failures: 3,
    failure_count: 0,
    delivery_config: null,
    created_at: '2026-01-01T00:00:00+00:00',
    updated_at: '2026-01-01T00:00:00+00:00',
    last_execution: null,
    ...overrides,
  };
}

const priceForm = (overrides: Partial<FormState>): FormState => ({
  ...INITIAL_FORM,
  trigger_type: 'price',
  price_condition_type: 'price_above',
  price_retrigger_mode: 'one_shot',
  ...overrides,
});

describe('formStateToPayload', () => {
  it('builds trigger_config for price trigger (stock, default)', () => {
    const payload = formStateToPayload(priceForm({ price_symbol: ' aapl ', price_value: '250' }));
    expect(payload.trigger_config).toEqual({
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 250 }],
      retrigger: { mode: 'one_shot' },
    });
    expect(payload).not.toHaveProperty('price_symbol');
    expect(payload).not.toHaveProperty('cron_expression');
    expect(payload).not.toHaveProperty('next_run_at');
  });

  it('auto-detects market=index for index symbols', () => {
    const tc = formStateToPayload(priceForm({ price_symbol: 'SPX', price_value: '5000' })).trigger_config;
    expect(tc?.symbol).toBe('SPX');
    expect(tc?.market).toBe('index');
  });

  it('omits market field for stock symbols', () => {
    const tc = formStateToPayload(priceForm({ price_symbol: 'AAPL', price_value: '250' })).trigger_config;
    expect(tc).not.toHaveProperty('market');
  });

  it('includes reference for pct_change condition', () => {
    const tc = formStateToPayload(
      priceForm({ price_symbol: 'TSLA', price_condition_type: 'pct_change_above', price_value: '5', price_reference: 'day_open' }),
    ).trigger_config;
    expect(tc?.conditions[0].reference).toBe('day_open');
  });

  it('includes cooldown_seconds for recurring mode', () => {
    const tc = formStateToPayload(
      priceForm({
        price_symbol: 'MSFT',
        price_condition_type: 'price_below',
        price_value: '100',
        price_retrigger_mode: 'recurring',
        price_cooldown_minutes: '480',
      }),
    ).trigger_config;
    expect(tc?.retrigger).toEqual({ mode: 'recurring', cooldown_seconds: 28800 });
  });

  it('drops next_run_at for cron trigger', () => {
    const payload = formStateToPayload({
      ...INITIAL_FORM,
      trigger_type: 'cron',
      cron_expression: '0 9 * * *',
      next_run_at: '2026-01-01T09:00:00Z',
    });
    expect(payload.cron_expression).toBe('0 9 * * *');
    expect(payload).not.toHaveProperty('next_run_at');
  });

  it('converts next_run_at to ISO for once trigger', () => {
    const payload = formStateToPayload({ ...INITIAL_FORM, trigger_type: 'once', next_run_at: '2026-03-20T14:30:00-04:00' });
    expect(payload.next_run_at).toBe('2026-03-20T18:30:00.000Z');
    expect(payload).not.toHaveProperty('cron_expression');
  });

  it('strips workspace_id when agent_mode is not ptc', () => {
    const payload = formStateToPayload({ ...INITIAL_FORM, agent_mode: 'flash', workspace_id: 'ws-123' });
    expect(payload).not.toHaveProperty('workspace_id');
  });

  it('keeps workspace_id when agent_mode is ptc', () => {
    const payload = formStateToPayload({ ...INITIAL_FORM, agent_mode: 'ptc', workspace_id: 'ws-123' });
    expect(payload.workspace_id).toBe('ws-123');
  });

  it('packs delivery_methods into delivery_config', () => {
    const payload = formStateToPayload({ ...INITIAL_FORM, delivery_methods: ['slack'] });
    expect(payload.delivery_config).toEqual({ methods: ['slack'] });
    expect(payload).not.toHaveProperty('delivery_methods');
    expect(formStateToPayload(INITIAL_FORM).delivery_config).toEqual({ methods: [] });
  });

  it('strips empty description', () => {
    expect(formStateToPayload({ ...INITIAL_FORM, description: '' })).not.toHaveProperty('description');
  });

  it('carries only the fields the server reads', () => {
    expect(Object.keys(formStateToPayload(INITIAL_FORM)).sort()).toEqual([
      'agent_mode',
      'cron_expression',
      'delivery_config',
      'instruction',
      'max_failures',
      'name',
      'thread_strategy',
      'timezone',
      'trigger_type',
    ]);
  });

  it('replaces {symbol} placeholder in instruction with price_symbol', () => {
    const payload = formStateToPayload(
      priceForm({ price_symbol: ' nvda ', price_value: '180', instruction: 'Analyze {symbol} price movement and {symbol} outlook.' }),
    );
    expect(payload.instruction).toBe('Analyze NVDA price movement and NVDA outlook.');
  });
});

describe('automationToFormState', () => {
  it('maps a full automation to form state', () => {
    const form = automationToFormState(
      makeAutomation({
        name: 'Test Alert',
        description: 'test desc',
        trigger_type: 'price',
        max_failures: 5,
        delivery_config: { methods: ['slack'] },
        trigger_config: {
          symbol: 'AAPL',
          conditions: [{ type: 'price_above', value: 250 }],
          retrigger: { mode: 'recurring', cooldown_seconds: 28800 },
        },
      }),
    );
    expect(form.name).toBe('Test Alert');
    expect(form.price_symbol).toBe('AAPL');
    expect(form.price_condition_type).toBe('price_above');
    expect(form.price_value).toBe('250');
    expect(form.price_retrigger_mode).toBe('recurring');
    expect(form.price_cooldown_minutes).toBe('480');
    expect(form.delivery_methods).toEqual(['slack']);
    expect(form.max_failures).toBe(5);
  });

  it('keeps a channel the form does not offer through an edit', () => {
    const form = automationToFormState(makeAutomation({ delivery_config: { methods: ['telegram', 'slack'] } }));
    expect(formStateToPayload(form).delivery_config).toEqual({ methods: ['telegram', 'slack'] });
  });

  it('loads index automation symbol correctly', () => {
    const form = automationToFormState(
      makeAutomation({
        trigger_type: 'price',
        trigger_config: {
          symbol: 'SPX',
          market: 'index',
          conditions: [{ type: 'price_above', value: 5000 }],
          retrigger: { mode: 'one_shot' },
        },
      }),
    );
    expect(form.price_symbol).toBe('SPX');
  });

  it('handles missing trigger_config gracefully', () => {
    const form = automationToFormState(
      makeAutomation({ trigger_type: 'cron', cron_expression: '0 9 * * *', timezone: 'America/New_York' }),
    );
    expect(form.trigger_type).toBe('cron');
    expect(form.cron_expression).toBe('0 9 * * *');
    expect(form.timezone).toBe('America/New_York');
    expect(form.price_symbol).toBe('');
    expect(form.price_condition_type).toBe('price_above');
  });

  it('normalizes cooldown retrigger mode alias', () => {
    const legacy = { mode: 'cooldown', cooldown_seconds: 14400 } as unknown as { mode: 'recurring' };
    const form = automationToFormState(
      makeAutomation({
        trigger_type: 'price',
        trigger_config: { symbol: 'TSLA', conditions: [{ type: 'price_below', value: 100 }], retrigger: legacy },
      }),
    );
    expect(form.price_retrigger_mode).toBe('recurring');
  });

  it('saves a one-time run back at the instant it was loaded with', () => {
    // Regression: the form cut the instant to its UTC wall-clock digits and
    // read them back as local time, so an untouched edit moved the run by the
    // reader's offset. The zone is off UTC on purpose: in UTC that cut read
    // back unchanged.
    const form = automationToFormState(
      makeAutomation({ trigger_type: 'once', timezone: 'America/New_York', next_run_at: '2026-10-28T18:15:00+00:00' }),
    );
    expect(timeIn(new Date(form.next_run_at), 'America/New_York')).toEqual({ hour: 14, minute: 15 });
    expect(formStateToPayload(form).next_run_at).toBe('2026-10-28T18:15:00.000Z');
  });
});

describe('withZone', () => {
  it('keeps the wall-clock time of a one-time run in the new zone', () => {
    const form: FormState = {
      ...INITIAL_FORM,
      trigger_type: 'once',
      timezone: 'America/New_York',
      next_run_at: '2026-10-28T18:15:00.000Z',
    };
    const moved = withZone(form, 'Asia/Tokyo');
    expect(moved.timezone).toBe('Asia/Tokyo');
    expect(timeIn(new Date(moved.next_run_at), 'Asia/Tokyo')).toEqual({ hour: 14, minute: 15 });
  });

  it('leaves a schedule alone', () => {
    const form: FormState = { ...INITIAL_FORM, timezone: 'UTC', next_run_at: '2026-10-28T18:15:00.000Z' };
    expect(withZone(form, 'Asia/Tokyo').next_run_at).toBe(form.next_run_at);
  });
});

describe('validateForm', () => {
  const now = Date.parse('2026-10-28T12:00:00Z');

  it('asks for a moment, then for one still ahead', () => {
    const once: FormState = { ...INITIAL_FORM, trigger_type: 'once' };
    expect(validateForm(once, now)).toEqual({ messageKey: 'automation.onceHint', section: 'when' });
    expect(validateForm({ ...once, next_run_at: '2026-10-28T11:00:00Z' }, now)).toEqual({
      messageKey: 'automation.oncePassed',
      section: 'when',
    });
    expect(validateForm({ ...once, next_run_at: '2026-10-28T13:00:00Z' }, now)).toBeNull();
  });

  it('points a sandbox run without a workspace at the folded options', () => {
    expect(validateForm({ ...INITIAL_FORM, agent_mode: 'ptc' }, now)).toEqual({
      messageKey: 'automation.workspaceRequired',
      section: 'more',
    });
  });

  it("leaves an edit's untouched one-time moment alone, even a past one", () => {
    const ran: FormState = { ...INITIAL_FORM, trigger_type: 'once', next_run_at: '' };
    expect(validateForm({ ...ran, name: 'Renamed' }, now, ran)).toBeNull();
    expect(validateForm({ ...ran, next_run_at: '2026-10-28T11:00:00Z' }, now, ran)?.messageKey).toBe('automation.oncePassed');
  });

  it('holds a price trigger to what the server accepts', () => {
    const ok = priceForm({ price_symbol: 'AAPL', price_value: '150' });
    expect(validateForm(ok, now)).toBeNull();
    expect(validateForm({ ...ok, price_symbol: '^spx' }, now)).toBeNull();
    expect(validateForm({ ...ok, price_symbol: '' }, now)?.messageKey).toBe('automation.priceSymbolInvalid');
    expect(validateForm({ ...ok, price_symbol: 'TOOLONGSYMB' }, now)?.messageKey).toBe('automation.priceSymbolInvalid');
    expect(validateForm({ ...ok, price_value: '0' }, now)?.messageKey).toBe('automation.priceValuePositive');
    expect(validateForm({ ...ok, price_value: '-5' }, now)?.messageKey).toBe('automation.priceValuePositive');
    expect(validateForm({ ...ok, price_value: '' }, now)?.messageKey).toBe('automation.priceValuePositive');
  });

  it('refuses an instruction whose {symbol} nothing fills', () => {
    const unfilled = { messageKey: 'automation.instructionSymbolUnfilled', section: 'instruction' };
    // The earnings template runs once, and a one-time trigger has no symbol.
    const earnings = { ...applyTemplate('earnings_watch'), workspace_id: 'ws-1', next_run_at: '2026-10-28T13:00:00Z' };
    expect(validateForm(earnings, now)).toEqual(unfilled);
    expect(validateForm({ ...earnings, instruction: earnings.instruction.replace('{symbol}', 'NVDA') }, now)).toBeNull();
    // A price template moved onto a schedule keeps the placeholder too.
    expect(validateForm({ ...applyTemplate('price_alert'), trigger_type: 'cron' }, now)).toEqual(unfilled);
    // A price trigger's symbol fills it.
    expect(validateForm(priceForm({ price_symbol: 'AAPL', price_value: '150', instruction: 'Why did {symbol} move?' }), now)).toBeNull();
  });

  it('opens the folded options for a cooldown the server would refuse', () => {
    const recurring = priceForm({ price_symbol: 'AAPL', price_value: '150', price_retrigger_mode: 'recurring' });
    expect(validateForm({ ...recurring, price_cooldown_minutes: '' }, now)).toBeNull();
    expect(validateForm({ ...recurring, price_cooldown_minutes: '240' }, now)).toBeNull();
    expect(validateForm({ ...recurring, price_cooldown_minutes: '60' }, now)).toEqual({
      messageKey: 'automation.cooldownTooShort',
      section: 'more',
    });
  });

  it('reads the cooldown the same way to check it and to save it', () => {
    // Regression: the field hands over "1e3" as typed, which passed the check
    // and then saved as parseInt read it, 1 minute, so the cooldown was dropped.
    const recurring = priceForm({ price_symbol: 'AAPL', price_value: '150', price_retrigger_mode: 'recurring' });
    const withCooldown = (text: string) => ({ ...recurring, price_cooldown_minutes: text });
    const saved = (text: string) => formStateToPayload(withCooldown(text)).trigger_config?.retrigger;
    expect(validateForm(withCooldown('1e3'), now)).toBeNull();
    expect(saved('1e3')).toEqual({ mode: 'recurring', cooldown_seconds: 60_000 });
    expect(validateForm(withCooldown(' 300 '), now)).toBeNull();
    expect(saved(' 300 ')).toEqual({ mode: 'recurring', cooldown_seconds: 18_000 });
    expect(validateForm(withCooldown('239'), now)?.messageKey).toBe('automation.cooldownTooShort');
    expect(validateForm(withCooldown('240.7'), now)?.messageKey).toBe('automation.cooldownTooShort');
  });
});

describe('isFormChanged', () => {
  it('compares every field, delivery channels by content', () => {
    const initial: FormState = { ...INITIAL_FORM, delivery_methods: ['slack'] };
    expect(isFormChanged(initial, { ...initial, delivery_methods: ['slack'] })).toBe(false);
    expect(isFormChanged(initial, { ...initial, delivery_methods: [] })).toBe(true);
    expect(isFormChanged(initial, { ...initial, name: 'x' })).toBe(true);
  });

  it('reads a blank schedule form as untouched, since the builder starts on its default', () => {
    const blank = applyTemplate('custom', 'UTC');
    expect(blank.cron_expression).toBe('0 9 * * *');
  });
});

describe('formStateToUpdatePayload', () => {
  const edit = (automation: Automation, change: (f: FormState) => FormState) => {
    const initial = automationToFormState(automation, 'UTC');
    return formStateToUpdatePayload(change(initial), initial, automation);
  };

  const priceAutomation = makeAutomation({
    trigger_type: 'price',
    cron_expression: null,
    trigger_config: {
      symbol: 'SPX',
      market: 'index',
      conditions: [
        { type: 'price_above', value: 6000 },
        { type: 'pct_change_below', value: 2, reference: 'day_open' },
      ],
      retrigger: { mode: 'recurring', cooldown_seconds: 15_000 },
    },
  });

  it('never sends the kind of trigger, and always sends the description', () => {
    const payload = edit(makeAutomation({ description: 'old' }), (f) => ({ ...f, description: '' }));
    expect(payload).not.toHaveProperty('trigger_type');
    expect(payload.description).toBe('');
  });

  it('leaves a price trigger alone when no price field changed', () => {
    const payload = edit(priceAutomation, (f) => ({ ...f, name: 'Renamed' }));
    expect(payload).not.toHaveProperty('trigger_config');
    expect(payload.name).toBe('Renamed');
  });

  it('lays a changed level over the saved trigger, keeping what the form cannot show', () => {
    const payload = edit(priceAutomation, (f) => ({ ...f, price_value: '6100' }));
    expect(payload.trigger_config).toEqual({
      symbol: 'SPX',
      market: 'index',
      conditions: [
        { type: 'price_above', value: 6100 },
        { type: 'pct_change_below', value: 2, reference: 'day_open' },
      ],
      retrigger: { mode: 'recurring', cooldown_seconds: 15_000 },
    });
  });

  it('rebuilds only the part that changed', () => {
    const symbol = edit(priceAutomation, (f) => ({ ...f, price_symbol: 'aapl' })).trigger_config;
    expect(symbol?.symbol).toBe('AAPL');
    expect(symbol).not.toHaveProperty('market');
    expect(symbol?.retrigger.cooldown_seconds).toBe(15_000);

    const cooldown = edit(priceAutomation, (f) => ({ ...f, price_cooldown_minutes: '300' })).trigger_config;
    expect(cooldown?.retrigger).toEqual({ mode: 'recurring', cooldown_seconds: 18_000 });
    expect(cooldown?.conditions).toHaveLength(2);
  });

  it('restates a schedule only when it or its zone changed', () => {
    const cron = makeAutomation({ cron_expression: '0 9 * * 1-5', timezone: 'America/New_York' });
    expect(edit(cron, (f) => ({ ...f, name: 'x' }))).not.toHaveProperty('cron_expression');
    expect(edit(cron, (f) => ({ ...f, name: 'x' }))).not.toHaveProperty('timezone');
    expect(edit(cron, (f) => ({ ...f, cron_expression: '0 10 * * 1-5' }))).toMatchObject({
      cron_expression: '0 10 * * 1-5',
      timezone: 'America/New_York',
    });
    expect(edit(cron, (f) => ({ ...f, timezone: 'Asia/Tokyo' }))).toMatchObject({
      cron_expression: '0 9 * * 1-5',
      timezone: 'Asia/Tokyo',
    });
  });

  it('restates a one-time moment only when it moved', () => {
    const once = makeAutomation({ trigger_type: 'once', cron_expression: null, next_run_at: '2026-10-28T18:15:00+00:00' });
    expect(edit(once, (f) => ({ ...f, name: 'x' }))).not.toHaveProperty('next_run_at');
    expect(edit(once, (f) => ({ ...f, next_run_at: '2026-10-29T18:15:00.000Z' })).next_run_at).toBe('2026-10-29T18:15:00.000Z');
  });
});

describe('an edit of each template', () => {
  // Each template filled in the way a reader would, saved, and reopened.
  const filled = (id: TemplateId): FormState => ({
    ...applyTemplate(id, 'UTC'),
    workspace_id: 'ws-1',
    price_symbol: 'NVDA',
    price_value: '180',
    next_run_at: '2026-10-28T18:15:00.000Z',
  });
  const reopened = (form: FormState) => {
    const p = formStateToPayload(form);
    const automation = makeAutomation({
      ...p,
      description: p.description ?? null,
      cron_expression: p.cron_expression ?? null,
      next_run_at: p.next_run_at ?? null,
      trigger_config: p.trigger_config ?? null,
      workspace_id: p.workspace_id ?? null,
    });
    return { automation, initial: automationToFormState(automation, 'UTC') };
  };
  const pick = (payload: object, keys: string[]) =>
    Object.fromEntries(Object.entries(payload).filter(([k]) => keys.includes(k)));

  const cases: [TemplateId, (f: FormState) => FormState, string[]][] = [
    ['custom', (f) => ({ ...f, cron_expression: '0 10 * * *' }), ['cron_expression', 'timezone']],
    ['morning_briefing', (f) => ({ ...f, timezone: 'Asia/Tokyo' }), ['cron_expression', 'timezone']],
    ['weekly_review', (f) => ({ ...f, cron_expression: '0 21 * * 5' }), ['cron_expression', 'timezone']],
    ['earnings_watch', (f) => ({ ...f, next_run_at: '2026-10-29T18:15:00.000Z' }), ['next_run_at', 'timezone']],
    ['price_alert', (f) => ({ ...f, price_value: '190' }), ['trigger_config']],
  ];

  it.each(cases)('%s restates the settings a create states, and its trigger only once changed', (id, change, triggerKeys) => {
    const { automation, initial } = reopened(filled(id));
    const settings = pick(formStateToPayload(initial), [
      'name', 'agent_mode', 'instruction', 'thread_strategy', 'max_failures', 'delivery_config', 'workspace_id',
    ]);
    const untouched = formStateToUpdatePayload(initial, initial, automation);
    expect(untouched).toEqual({ ...settings, description: initial.description });

    const changed = change(initial);
    expect(formStateToUpdatePayload(changed, initial, automation)).toEqual({
      ...untouched,
      ...pick(formStateToPayload(changed), triggerKeys),
    });
  });
});
