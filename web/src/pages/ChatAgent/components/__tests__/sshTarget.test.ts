// @vitest-environment node
import { describe, expect, it } from 'vitest';

import type { SshProbeResult } from '@/types/api';

import {
  formatSshAddress,
  formatSshHostPort,
  parseSshAddress,
  sshCreateBody,
  sshDefaultName,
  type SshReady,
} from '../sshTarget';

describe('parseSshAddress', () => {
  const ok = (username: string | null, host: string, port = 22) => ({ ok: true, username, host, port });

  it.each([
    ['ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['  ubuntu@203.0.113.10  ', ok('ubuntu', '203.0.113.10')],
    ['ubuntu@box.example.com:2222', ok('ubuntu', 'box.example.com', 2222)],
    ['ubuntu@[2001:db8::1]:2222', ok('ubuntu', '2001:db8::1', 2222)],
    ['ubuntu@[2001:db8::1]', ok('ubuntu', '2001:db8::1')],
    ['ubuntu@2001:db8::1', ok('ubuntu', '2001:db8::1')],
    ['ssh://ubuntu@box.example.com:2222', ok('ubuntu', 'box.example.com', 2222)],
  ])('reads the address %j', (raw, expected) => {
    expect(parseSshAddress(raw)).toEqual(expected);
  });

  it.each([
    ['ssh ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['ssh -p 2222 ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10', 2222)],
    ['ssh -p2222 ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10', 2222)],
    ['ssh ubuntu@203.0.113.10 -p 2222', ok('ubuntu', '203.0.113.10', 2222)],
    ['ssh -l ubuntu 203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['ssh -lubuntu 203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['ssh 203.0.113.10 -l ubuntu -p 2222', ok('ubuntu', '203.0.113.10', 2222)],
    ['ssh -vp 2222 ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10', 2222)],
    ['ssh -i ~/.ssh/id_ed25519 ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['ssh -o StrictHostKeyChecking=no -A ubuntu@203.0.113.10', ok('ubuntu', '203.0.113.10')],
    ['ssh -4 -t ubuntu@203.0.113.10 uptime -p', ok('ubuntu', '203.0.113.10')],
    ['ssh ubuntu@203.0.113.10 -- -p 99', ok('ubuntu', '203.0.113.10')],
  ])('reads the ssh command %j', (raw, expected) => {
    expect(parseSshAddress(raw)).toEqual(expected);
  });

  it('lets -l and -p win over the destination, as ssh does', () => {
    expect(parseSshAddress('ssh -l root -p 2200 ubuntu@box:2222')).toEqual(ok('root', 'box', 2200));
  });

  it('reads an address with no login, for the caller to ask for one', () => {
    expect(parseSshAddress('203.0.113.10')).toEqual(ok(null, '203.0.113.10'));
    expect(parseSshAddress('ssh box.example.com -p 2222')).toEqual(ok(null, 'box.example.com', 2222));
    expect(parseSshAddress('@box')).toEqual(ok(null, 'box'));
  });

  it('says nothing is there for empty input', () => {
    expect(parseSshAddress('')).toEqual({ ok: false, error: 'empty' });
    expect(parseSshAddress('   ')).toEqual({ ok: false, error: 'empty' });
    expect(parseSshAddress('ssh')).toEqual({ ok: false, error: 'empty' });
  });

  it.each([
    'ubuntu@',
    'ssh -p 2222',
    'https://box.example.com',
    'ubuntu@[2001:db8::1',
    'ubuntu@[2001:db8::1]2222',
  ])('refuses %j as no host', (raw) => {
    expect(parseSshAddress(raw)).toEqual({ ok: false, error: 'host' });
  });

  it.each([
    'ubuntu@box:0',
    'ubuntu@box:65536',
    'ubuntu@box:',
    'ubuntu@box:ssh',
    'ssh -p abc ubuntu@box',
    'ssh ubuntu@box -p',
  ])('refuses %j for its port', (raw) => {
    expect(parseSshAddress(raw)).toEqual({ ok: false, error: 'port' });
  });
});

describe('formatSshAddress', () => {
  it('leaves the default port out', () => {
    expect(formatSshAddress({ username: 'me', host: 'box', port: 22 })).toBe('me@box');
  });
  it('appends another port, bracketing an IPv6 host so it reads apart', () => {
    expect(formatSshAddress({ username: 'me', host: 'box', port: 2222 })).toBe('me@box:2222');
    expect(formatSshAddress({ username: 'me', host: '2001:db8::1', port: 2222 })).toBe('me@[2001:db8::1]:2222');
    expect(formatSshAddress({ username: 'me', host: '2001:db8::1', port: 22 })).toBe('me@2001:db8::1');
  });
  it('reads back as the same target', () => {
    for (const target of [
      { username: 'me', host: 'box', port: 22 },
      { username: 'me', host: '2001:db8::1', port: 2222 },
      { username: 'me', host: '2001:db8::1', port: 22 },
    ]) {
      expect(parseSshAddress(formatSshAddress(target))).toEqual({ ok: true, ...target });
    }
  });
  it('writes host and port the same way', () => {
    expect(formatSshHostPort('box', 22)).toBe('box:22');
    expect(formatSshHostPort('2001:db8::1', 22)).toBe('[2001:db8::1]:22');
  });
});

const probe = (over: Partial<SshProbeResult> = {}): SshProbeResult => ({
  host_key: 'ssh-ed25519 AAAA', fingerprint: 'SHA256:x', os: 'linux', arch: 'x86_64', distro: 'Ubuntu',
  home: '/home/me', shell: '/bin/bash', is_root: false, has_sudo: true, disk_free_bytes: null,
  supported: true, problems: [], suggested_root_dir: '/home/me/langalpha', hostname: 'box', helper: false, ...over,
});
const ready = (over: Partial<SshProbeResult> = {}): SshReady => ({
  target: { host: 'box.example.com', port: 22, username: 'me' },
  probe: probe(over),
});

describe('sshCreateBody', () => {
  it('stays closed until a machine is in', () => {
    expect(sshCreateBody(null, '')).toBeNull();
  });
  it('pins the host key the server connected with', () => {
    expect(sshCreateBody(ready(), '')).toEqual({
      host: 'box.example.com', port: 22, username: 'me', host_key: 'ssh-ed25519 AAAA',
    });
  });
  it('forwards the tunnel id of a tunneled target', () => {
    const tunneled = { ...ready(), target: { host: 'box', port: 22, username: 'me', tunnel_id: 't1' } };
    expect(sshCreateBody(tunneled, '')?.tunnel_id).toBe('t1');
    expect(sshCreateBody(ready(), '')).not.toHaveProperty('tunnel_id');
  });
  it('sends the folder only when set', () => {
    expect(sshCreateBody(ready(), ' /srv/w ')?.root_dir).toBe('/srv/w');
    expect(sshCreateBody(ready(), '   ')).not.toHaveProperty('root_dir');
  });
  it('reads a ~ folder against the home the probe found, since the server takes only absolute ones', () => {
    expect(sshCreateBody(ready(), '~/work')?.root_dir).toBe('/home/me/work');
    expect(sshCreateBody(ready({ home: '/home/me/' }), '~')?.root_dir).toBe('/home/me');
    expect(sshCreateBody(ready(), '~other/work')?.root_dir).toBe('~other/work');
    expect(sshCreateBody(ready({ home: '' }), '~/work')?.root_dir).toBe('~/work');
  });
  it('sends only the target fields, whatever else the server put on it', () => {
    const extra = { ...ready(), target: { host: 'h', port: 2222, username: 'u', note: 'x' } as SshReady['target'] };
    expect(sshCreateBody(extra, '')).toEqual({ host: 'h', port: 2222, username: 'u', host_key: 'ssh-ed25519 AAAA' });
  });
  it('stays closed for an unsupported machine', () => {
    expect(sshCreateBody(ready({ supported: false }), '/srv/w')).toBeNull();
  });
});

describe('sshDefaultName', () => {
  it("is the machine's own name, else its host", () => {
    expect(sshDefaultName(ready())).toBe('box');
    expect(sshDefaultName(ready({ hostname: '' }))).toBe('box.example.com');
    expect(sshDefaultName(ready({ hostname: '  ' }))).toBe('box.example.com');
  });
});
