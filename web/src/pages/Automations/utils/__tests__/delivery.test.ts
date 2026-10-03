// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { TFunction } from 'i18next';
import { deliveryMethodName } from '../delivery';

const LABELS: Record<string, string> = {
  'automation.deliverToSlack': 'Slack',
  'automation.deliverToDiscord': 'Discord',
};

const t = ((key: string, opts?: { app?: string }) =>
  key === 'automation.deliverToDm' ? `${opts?.app} DM` : (LABELS[key] ?? key)) as unknown as TFunction;

describe('deliveryMethodName', () => {
  it('names the form’s own options by their labels', () => {
    expect(deliveryMethodName('slack', t)).toBe('Slack');
    expect(deliveryMethodName('discord', t)).toBe('Discord');
  });

  it('capitalizes a bare app the form does not offer', () => {
    expect(deliveryMethodName('telegram', t)).toBe('Telegram');
    expect(deliveryMethodName('imessage', t)).toBe('Imessage');
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
