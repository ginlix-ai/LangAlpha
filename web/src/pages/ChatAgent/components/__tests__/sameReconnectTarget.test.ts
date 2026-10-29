// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { sameReconnectTarget } from '../startFailure';

const sent = { host: 'box', port: 22, username: 'dev', tunnel_id: 'tun-9', host_key: 'ssh-ed25519 AAAAnew' };

describe('sameReconnectTarget', () => {
  it('holds a refusal to the login it was sent for', () => {
    expect(sameReconnectTarget(sent, { ...sent })).toBe(true);
  });

  it.each([
    ['another user', { username: 'ops' }],
    ['another port', { port: 2222 }],
    ['another tunnel', { tunnel_id: 'tun-10' }],
    ['no tunnel', { tunnel_id: null }],
    ['another key', { host_key: 'ssh-ed25519 AAAAother' }],
  ])('asks afresh for %s on the same host', (_name, change) => {
    expect(sameReconnectTarget(sent, { ...sent, ...change })).toBe(false);
  });

  it('reads an absent tunnel as none', () => {
    const direct = { host: 'box', port: 22, username: 'dev', host_key: 'k' };
    expect(sameReconnectTarget(direct, { ...direct, tunnel_id: null })).toBe(true);
  });
});
