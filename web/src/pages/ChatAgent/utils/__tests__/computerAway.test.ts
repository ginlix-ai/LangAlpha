// @vitest-environment node
/**
 * A workspace's computer that did not answer reads in this app's words, read
 * from the connect code the server sends, never from the English sentence
 * beside it, and the machine is called what its owner calls it.
 */
import { describe, expect, it } from 'vitest';

import type { Computer } from '@/types/api';

import {
  awayMachineOf, awayRetryMessage, backupCode, backupShownText, unreachableCode, unreachableTurnCopy,
} from '../computerAway';

// Only the named keys print the name, as the catalogs do.
const t = ((key: string, opts?: Record<string, unknown>) => (
  key.endsWith('Named') ? `${key}:${String(opts?.name)}` : key
)) as unknown as Parameters<typeof unreachableTurnCopy>[0];

function refused(status: number, headers: Record<string, string> = {}) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, headers, data: { detail: 'Sandbox is not reachable: work-mac is not connected.' } },
  });
}

const ssh = (over: Record<string, unknown>) => ({
  host: 'h', port: 22, username: 'u', fingerprint: null, tunnel: true, tunnel_connected: false, helper: false, ...over,
}) as Computer['ssh'];

const remoteMac = awayMachineOf({ kind: 'ssh', name: 'work-mac', ssh: ssh({ helper: true }) });
const sshMac = awayMachineOf({ kind: 'ssh', name: 'work-mac', ssh: ssh({ os: 'Darwin' }) });
const linux = awayMachineOf({ kind: 'ssh', name: 'lab-box', ssh: ssh({ os: 'Linux' }) });

describe('computerAway', () => {
  it('reads the code only off a 503 that carries it', () => {
    expect(unreachableCode(refused(503, { 'x-computer-unreachable': 'not_connected' }))).toBe('not_connected');
    expect(unreachableCode(refused(503))).toBeNull();
    expect(unreachableCode(refused(409, { 'x-computer-unreachable': 'not_connected' }))).toBeNull();
    expect(unreachableCode(null)).toBeNull();
  });

  it('reads a backup as an away machine\'s only when it names the code', () => {
    expect(backupCode({ kind: 'backup', unreachable: 'timeout' })).toBe('timeout');
    expect(backupCode({ kind: 'backup', unreachable: null })).toBeNull();
    expect(backupCode({ kind: 'live' })).toBeNull();
  });

  it('calls a Mac "your Mac" and any other machine by its name', () => {
    expect(remoteMac).toEqual({ mac: true, remoteControl: true, name: 'work-mac' });
    expect(sshMac).toEqual({ mac: true, remoteControl: false, name: 'work-mac' });
    expect(linux).toEqual({ mac: false, remoteControl: false, name: 'lab-box' });
    expect(awayMachineOf({ kind: 'daytona', name: 'cloud', ssh: null }).mac).toBe(false);
    expect(awayMachineOf(null)).toEqual({ mac: false, remoteControl: false, name: null });
  });

  it('words a refused change for the machine, and blames the service for its own gateway', () => {
    const away = refused(503, { 'x-computer-unreachable': 'not_connected' });
    expect(awayRetryMessage(away, t, sshMac)).toBe('filePanel:away.retryMac');
    expect(awayRetryMessage(away, t, linux)).toBe('filePanel:away.retryNamed:lab-box');
    expect(awayRetryMessage(away, t, awayMachineOf(null))).toBe('filePanel:away.retry');
    expect(awayRetryMessage(refused(503, { 'x-computer-unreachable': 'gateway_unreachable' }), t, sshMac))
      .toBe('filePanel:away.retryService');
    expect(awayRetryMessage(refused(500), t, sshMac)).toBeNull();
  });

  it('says the files shown are the backup', () => {
    expect(backupShownText(t, 'not_connected', sshMac)).toBe('filePanel:away.backupMac');
    expect(backupShownText(t, 'timeout', linux)).toBe('filePanel:away.backupNamed:lab-box');
    expect(backupShownText(t, 'gateway_unreachable', linux)).toBe('filePanel:away.backupService');
  });

  it('tells a turn how to bring its machine back', () => {
    expect(unreachableTurnCopy(t, 'not_connected', remoteMac)).toEqual({
      title: 'chat.computerUnreachable.titleMac', detail: 'chat.computerUnreachable.remoteControl',
    });
    expect(unreachableTurnCopy(t, 'timeout', sshMac).detail).toBe('chat.computerUnreachable.checkMac');
    expect(unreachableTurnCopy(t, 'unreachable', linux)).toEqual({
      title: 'chat.computerUnreachable.titleNamed:lab-box', detail: 'chat.computerUnreachable.checkNamed:lab-box',
    });
    expect(unreachableTurnCopy(t, 'gateway_unreachable', remoteMac)).toEqual({
      title: 'chat.computerUnreachable.titleService', detail: 'chat.computerUnreachable.checkService',
    });
  });
});
