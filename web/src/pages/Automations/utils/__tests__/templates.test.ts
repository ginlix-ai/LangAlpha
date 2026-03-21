import { describe, it, expect } from 'vitest';
import {
  applyTemplate,
  automationToFormState,
  formStateToPayload,
  INITIAL_FORM,
  AUTOMATION_TEMPLATES,
} from '../templates';
import type { Automation } from '@/types/automation';

describe('applyTemplate', () => {
  it('returns price trigger defaults for price_alert', () => {
    const form = applyTemplate('price_alert');
    expect(form.trigger_type).toBe('price');
    expect(form.agent_mode).toBe('flash');
    expect(form.name).toBe('Price Alert + Analysis');
    expect(form.instruction).toContain('sentiment');
  });

  it('returns cron defaults for morning_briefing', () => {
    const form = applyTemplate('morning_briefing');
    expect(form.trigger_type).toBe('cron');
    expect(form.cron_expression).toBe('0 7 * * 1-5');
    expect(form.timezone).toBe('America/New_York');
    expect(form.agent_mode).toBe('flash');
  });

  it('returns cron PTC defaults for weekly_review', () => {
    const form = applyTemplate('weekly_review');
    expect(form.trigger_type).toBe('cron');
    expect(form.cron_expression).toBe('0 22 * * 5');
    expect(form.agent_mode).toBe('ptc');
  });

  it('returns once PTC defaults for earnings_watch', () => {
    const form = applyTemplate('earnings_watch');
    expect(form.trigger_type).toBe('once');
    expect(form.agent_mode).toBe('ptc');
  });

  it('returns INITIAL_FORM for custom', () => {
    const form = applyTemplate('custom');
    expect(form).toEqual(INITIAL_FORM);
  });

  it('returns INITIAL_FORM for unknown id', () => {
    const form = applyTemplate('nonexistent' as any);
    expect(form).toEqual(INITIAL_FORM);
  });

  it('defines exactly 5 templates', () => {
    expect(AUTOMATION_TEMPLATES).toHaveLength(5);
  });
});

describe('formStateToPayload', () => {
  it('builds trigger_config for price trigger (stock, default)', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: ' aapl ',
      price_condition_type: 'price_above',
      price_value: '250',
      price_retrigger_mode: 'one_shot',
    };
    const payload = formStateToPayload(form);
    expect(payload.trigger_config).toEqual({
      symbol: 'AAPL',
      conditions: [{ type: 'price_above', value: 250 }],
      retrigger: { mode: 'one_shot' },
    });
    expect(payload.price_symbol).toBeUndefined();
    expect(payload.cron_expression).toBeUndefined();
    expect(payload.next_run_at).toBeUndefined();
  });

  it('auto-detects market=index for index symbols', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: 'SPX',
      price_condition_type: 'price_above',
      price_value: '5000',
      price_retrigger_mode: 'one_shot',
    };
    const payload = formStateToPayload(form);
    const tc = payload.trigger_config as any;
    expect(tc.symbol).toBe('SPX');
    expect(tc.market).toBe('index');
  });

  it('omits market field for stock symbols', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: 'AAPL',
      price_condition_type: 'price_above',
      price_value: '250',
      price_retrigger_mode: 'one_shot',
    };
    const payload = formStateToPayload(form);
    const tc = payload.trigger_config as any;
    expect(tc.market).toBeUndefined();
  });

  it('includes reference for pct_change condition', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: 'TSLA',
      price_condition_type: 'pct_change_above',
      price_value: '5',
      price_reference: 'day_open',
      price_retrigger_mode: 'one_shot',
    };
    const payload = formStateToPayload(form);
    const tc = payload.trigger_config as any;
    expect(tc.conditions[0].reference).toBe('day_open');
  });

  it('includes cooldown_seconds for recurring mode', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: 'MSFT',
      price_condition_type: 'price_below',
      price_value: '100',
      price_retrigger_mode: 'recurring',
      price_cooldown_minutes: 480,
    };
    const payload = formStateToPayload(form);
    const tc = payload.trigger_config as any;
    expect(tc.retrigger.mode).toBe('recurring');
    expect(tc.retrigger.cooldown_seconds).toBe(28800);
  });

  it('drops next_run_at for cron trigger', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'cron',
      cron_expression: '0 9 * * *',
      next_run_at: '2026-01-01T09:00',
    };
    const payload = formStateToPayload(form);
    expect(payload.cron_expression).toBe('0 9 * * *');
    expect(payload.next_run_at).toBeUndefined();
  });

  it('converts next_run_at to ISO for once trigger', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'once',
      next_run_at: '2026-03-20T14:30',
    };
    const payload = formStateToPayload(form);
    expect(payload.next_run_at).toBe(new Date('2026-03-20T14:30').toISOString());
    expect(payload.cron_expression).toBeUndefined();
  });

  it('strips workspace_id when agent_mode is not ptc', () => {
    const form = {
      ...INITIAL_FORM,
      agent_mode: 'flash',
      workspace_id: 'ws-123',
    };
    const payload = formStateToPayload(form);
    expect(payload.workspace_id).toBeUndefined();
  });

  it('keeps workspace_id when agent_mode is ptc', () => {
    const form = {
      ...INITIAL_FORM,
      agent_mode: 'ptc',
      workspace_id: 'ws-123',
    };
    const payload = formStateToPayload(form);
    expect(payload.workspace_id).toBe('ws-123');
  });

  it('packs delivery_method into delivery_config', () => {
    const form = { ...INITIAL_FORM, delivery_method: 'slack' };
    const payload = formStateToPayload(form);
    expect(payload.delivery_config).toEqual({ methods: ['slack'] });
    expect(payload.delivery_method).toBeUndefined();
  });

  it('packs empty delivery_method into empty methods array', () => {
    const form = { ...INITIAL_FORM, delivery_method: '' };
    const payload = formStateToPayload(form);
    expect(payload.delivery_config).toEqual({ methods: [] });
  });

  it('strips empty description', () => {
    const form = { ...INITIAL_FORM, description: '' };
    const payload = formStateToPayload(form);
    expect(payload.description).toBeUndefined();
  });

  it('replaces {symbol} placeholder in instruction with price_symbol', () => {
    const form = {
      ...INITIAL_FORM,
      trigger_type: 'price',
      price_symbol: ' nvda ',
      price_condition_type: 'price_above',
      price_value: '180',
      instruction: 'Analyze {symbol} price movement and {symbol} outlook.',
    };
    const payload = formStateToPayload(form);
    expect(payload.instruction).toBe('Analyze NVDA price movement and NVDA outlook.');
  });
});

describe('automationToFormState', () => {
  it('maps a full automation to form state', () => {
    const automation: Automation = {
      id: '123',
      automation_id: '123',
      name: 'Test Alert',
      description: 'test desc',
      trigger_type: 'price',
      status: 'active',
      agent_mode: 'flash',
      instruction: 'do something',
      workspace_id: '',
      thread_strategy: 'new',
      max_failures: 5,
      delivery_config: { methods: ['slack'] },
      trigger_config: {
        symbol: 'AAPL',
        conditions: [{ type: 'price_above', value: 250 }],
        retrigger: { mode: 'recurring', cooldown_seconds: 28800 },
      },
    };
    const form = automationToFormState(automation);
    expect(form.name).toBe('Test Alert');
    expect(form.price_symbol).toBe('AAPL');
    expect(form.price_condition_type).toBe('price_above');
    expect(form.price_value).toBe('250');
    expect(form.price_retrigger_mode).toBe('recurring');
    expect(form.price_cooldown_minutes).toBe(480);
    expect(form.delivery_method).toBe('slack');
    expect(form.max_failures).toBe(5);
  });

  it('loads index automation symbol correctly', () => {
    const automation: Automation = {
      id: '999',
      automation_id: '999',
      name: 'Index Alert',
      status: 'active',
      trigger_type: 'price',
      trigger_config: {
        symbol: 'SPX',
        market: 'index',
        conditions: [{ type: 'price_above', value: 5000 }],
        retrigger: { mode: 'one_shot' },
      },
    };
    const form = automationToFormState(automation);
    expect(form.price_symbol).toBe('SPX');
  });

  it('handles missing trigger_config gracefully', () => {
    const automation: Automation = {
      id: '456',
      automation_id: '456',
      name: 'Cron Job',
      status: 'active',
      trigger_type: 'cron',
      cron_expression: '0 9 * * *',
      timezone: 'America/New_York',
    };
    const form = automationToFormState(automation);
    expect(form.trigger_type).toBe('cron');
    expect(form.cron_expression).toBe('0 9 * * *');
    expect(form.timezone).toBe('America/New_York');
    expect(form.price_symbol).toBe('');
    expect(form.price_condition_type).toBe('price_above');
  });

  it('normalizes cooldown retrigger mode alias', () => {
    const automation: Automation = {
      id: '789',
      automation_id: '789',
      name: 'Old Style',
      status: 'active',
      trigger_type: 'price',
      trigger_config: {
        symbol: 'TSLA',
        conditions: [{ type: 'price_below', value: 100 }],
        retrigger: { mode: 'cooldown' as any, cooldown_seconds: 14400 },
      },
    };
    const form = automationToFormState(automation);
    expect(form.price_retrigger_mode).toBe('recurring');
  });
});
