// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { TFunction } from 'i18next';
import type { DeliveryAttempt, DeliveryOptions } from '@/types/automation';
import {
  deliveryAttemptLabel,
  deliveryAttemptName,
  deliveryDefaultError,
  deliveryEntryName,
  deliveryMethodName,
  deliveryNames,
  deliveryOutcome,
  deliveryProblems,
} from '../delivery';

const LABELS: Record<string, string> = {
  'automation.deliverToSlack': 'Slack',
  'automation.deliverToDiscord': 'Discord',
  'automation.deliveryDefaultBusy': 'Busy',
  'automation.deliveryDefaultUnavailable': 'Unavailable',
  'automation.deliveryDefaultFailed': 'Failed',
};

type Opts = { app?: string; name?: string; method?: string };
const TEMPLATES: Record<string, (o: Opts) => string> = {
  'automation.deliverToDm': (o) => `${o.app} DM`,
  'automation.deliverAppLandsIn': (o) => `${o.app} (${o.name})`,
  'automation.deliveredVia': (o) => `sent to ${o.method}`,
  'automation.deliveredFallbackVia': (o) => `final answer posted to ${o.method}`,
  'automation.deliveredNoticeVia': (o) => `notice sent to ${o.method}`,
  'automation.deliveryFailedVia': (o) => `${o.method} delivery failed`,
};

const t = ((key: string, opts: Opts = {}) => TEMPLATES[key]?.(opts) ?? LABELS[key] ?? key) as unknown as TFunction;

/** A refusal as the API client rejects one: the body on `response`. */
function refused(status: number, body: unknown) {
  return Object.assign(new Error(`Request failed with status code ${status}`), { response: { status, data: body } });
}

const OPTIONS: DeliveryOptions = {
  enabled: true,
  apps: {
    slack: {
      chats: [
        { address: 'slack:T1', name: 'Your Slack DM (Acme)', kind: 'dm' },
        { address: 'slack:T1/C1', name: '#demo', kind: 'channel' },
      ],
      default: { address: 'slack:T1/C1', name: '#demo', via: 'workspace' },
      error: null,
    },
    discord: { chats: [], default: null, error: 'Discord is down.' },
  },
};

describe('deliveryMethodName', () => {
  it('names the form’s own options by their labels', () => {
    expect(deliveryMethodName('slack', t)).toBe('Slack');
    expect(deliveryMethodName('discord', t)).toBe('Discord');
  });

  it('names a bare app the form does not offer', () => {
    expect(deliveryMethodName('telegram', t)).toBe('Telegram');
    expect(deliveryMethodName('imessage', t)).toBe('iMessage');
  });

  it.each([
    ['discord:@me', 'Discord DM'],
    ['telegram:@me', 'Telegram DM'],
    ['imessage:@me', 'iMessage DM'],
    ['slack:T0123', 'Slack DM'],
  ])('reads the DM address %s as "%s"', (method, name) => {
    expect(deliveryMethodName(method, t)).toBe(name);
  });

  it.each([
    ['slack:T0123/C0456', 'Slack:T0123/C0456'],
    ['discord:1/2', 'Discord:1/2'],
    ['telegram:-100123', 'Telegram:-100123'],
  ])('leaves the chat address %s as it was', (method, name) => {
    expect(deliveryMethodName(method, t)).toBe(name);
  });
});

describe('deliveryNames', () => {
  it('takes the apps’ current names over a run’s, and a newer run’s over an older one’s', () => {
    const names = deliveryNames(OPTIONS, [
      { method: 'slack:T1/C1', success: true, address: 'slack:T1/C1', name: '#demo-old' },
      { method: 'slack:T1/C9', success: true, address: 'slack:T1/C9', name: '#ops' },
      { method: 'slack:T1/C9', success: false, address: 'slack:T1/C9', name: '#ops-older' },
    ]);
    expect(names.get('slack:T1/C1')).toBe('#demo');
    expect(names.get('slack:T1/C9')).toBe('#ops');
  });

  it('never names a bare app by where one run landed', () => {
    const names = deliveryNames(null, [{ method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo' }]);
    expect(names.has('slack')).toBe(false);
    expect(names.get('slack:T1/C1')).toBe('#demo');
  });
});

describe('deliveryEntryName', () => {
  const names = deliveryNames(OPTIONS);

  it('names a bare app with where it lands now', () => {
    expect(deliveryEntryName('slack', t, OPTIONS, names)).toBe('Slack (#demo)');
  });

  it('names a bare app alone where nothing says where it lands', () => {
    expect(deliveryEntryName('discord', t, OPTIONS, names)).toBe('Discord');
    expect(deliveryEntryName('telegram', t, OPTIONS, names)).toBe('Telegram');
    expect(deliveryEntryName('slack', t)).toBe('Slack');
  });

  it('names a chat by its name, else as deliveryMethodName spells it', () => {
    expect(deliveryEntryName('slack:T1/C1', t, OPTIONS, names)).toBe('#demo');
    expect(deliveryEntryName('slack:T1', t, OPTIONS, names)).toBe('Your Slack DM (Acme)');
    expect(deliveryEntryName('discord:@me', t, OPTIONS, names)).toBe('Discord DM');
    expect(deliveryEntryName('slack:T1/C_GONE', t, OPTIONS, names)).toBe('Slack:T1/C_GONE');
  });
});

describe('a run’s delivery', () => {
  const base: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo' };

  it.each([
    ['agent', true, 'sent', 'sent to #demo'],
    ['fallback', true, 'fallback', 'final answer posted to #demo'],
    ['notice', true, 'notice', 'notice sent to #demo'],
    [null, true, 'sent', 'sent to #demo'],
    ['notice', false, 'failed', '#demo delivery failed'],
    [null, false, 'failed', '#demo delivery failed'],
  ] as const)('via %s, success %s, reads as %s', (via, success, outcome, text) => {
    const attempt = { ...base, via, success };
    expect(deliveryOutcome(attempt)).toBe(outcome);
    expect(deliveryAttemptLabel(attempt, t)).toBe(text);
  });

  it('reads a run from before by its app, as it always has', () => {
    expect(deliveryAttemptLabel({ method: 'slack', success: true }, t)).toBe('sent to Slack');
    expect(deliveryAttemptLabel({ method: 'discord', success: false }, t)).toBe('Discord delivery failed');
  });

  it('names a landing the run did not name from what else is known', () => {
    const names = deliveryNames(OPTIONS);
    expect(deliveryAttemptName({ method: 'slack', success: true, address: 'slack:T1/C1' }, t, names)).toBe('#demo');
    expect(deliveryAttemptName({ method: 'slack:T1', success: true }, t, names)).toBe('Your Slack DM (Acme)');
    expect(deliveryAttemptName({ method: 'slack', success: true, address: 'slack:T1/C9' }, t, names)).toBe('Slack');
  });
});

describe('deliveryProblems', () => {
  it('reads each refused entry from the detail', () => {
    const err = refused(409, {
      detail: {
        message: "'slack:T1/C9': the bot isn't in the channel; 'telegram': not linked",
        problems: [
          { entry: 'slack:T1/C9', message: "the bot isn't in the channel" },
          { entry: 'telegram', message: 'not linked' },
          { entry: 'discord', message: '' },
          'junk',
        ],
      },
    });
    expect(deliveryProblems(err)).toEqual([
      { entry: 'slack:T1/C9', message: "the bot isn't in the channel" },
      { entry: 'telegram', message: 'not linked' },
    ]);
  });

  it('reads them beside the detail too', () => {
    const err = refused(400, { detail: 'Not saved.', problems: [{ entry: 'slack', message: 'not linked' }] });
    expect(deliveryProblems(err)).toEqual([{ entry: 'slack', message: 'not linked' }]);
  });

  it('finds none in a refusal that is only a sentence', () => {
    expect(deliveryProblems(refused(409, { detail: "'slack:T1/C9': the bot isn't in the channel" }))).toEqual([]);
    expect(deliveryProblems(new Error('offline'))).toEqual([]);
  });
});

describe('deliveryDefaultError', () => {
  it('says busy and unavailable in its own words', () => {
    expect(deliveryDefaultError(refused(409, { detail: 'conflict' }), t)).toBe('Busy');
    expect(deliveryDefaultError(refused(503, { detail: 'down' }), t)).toBe('Unavailable');
  });

  it('reads a refusal’s problems, else its sentence, else falls back', () => {
    const problems = [{ field: 'automation_output', message: 'The bot is not in #ops.' }, { field: 'x', message: 'Pick a chat.' }];
    expect(deliveryDefaultError(refused(400, { detail: 'Not saved.', problems }), t)).toBe('The bot is not in #ops. Pick a chat.');
    expect(deliveryDefaultError(refused(400, { detail: 'Not saved.' }), t)).toBe('Not saved.');
    expect(deliveryDefaultError(new Error(''), t)).toBe('Failed');
  });
});
