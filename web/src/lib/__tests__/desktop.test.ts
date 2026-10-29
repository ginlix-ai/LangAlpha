/**
 * The bridge's one auth-shaped call, and the question asked ahead of it.
 *
 * Both are pinned because both fail quietly. The capability must key off the
 * method and not the shell's presence, or a shell too old to know the channel
 * claims one it does not have; and every way of not getting a URI has to arrive
 * as the same `undefined`, because the caller's fallback is the browser path
 * and a thrown channel would take the whole connect down with it.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

const bridge = { version: '0.1.3', platform: 'darwin' } as Record<string, unknown>;
vi.stubGlobal('window', Object.assign(globalThis.window, { langalphaDesktop: bridge }));

const {
  beginMcpOAuth,
  bindMcpOAuth,
  cancelMcpOAuth,
  canBeginMcpOAuth,
  canRemoteControl,
  disableRemoteControl,
  enableRemoteControl,
  getRemoteControl,
} = await import('../desktop');

afterEach(() => {
  delete bridge.beginMcpOAuth;
  delete bridge.bindMcpOAuth;
  delete bridge.cancelMcpOAuth;
  delete bridge.getRemoteControl;
  delete bridge.enableRemoteControl;
  delete bridge.disableRemoteControl;
});

describe('canBeginMcpOAuth', () => {
  it('is false in a shell without the method', () => {
    expect(canBeginMcpOAuth()).toBe(false);
  });

  it('is true once the shell exposes it', () => {
    bridge.beginMcpOAuth = vi.fn();
    expect(canBeginMcpOAuth()).toBe(true);
  });
});

describe('beginMcpOAuth', () => {
  const offered = { redirectUri: 'http://127.0.0.1:8788/mcp/callback', flowId: 'f-1' };

  it('hands back the loopback URI the shell offers, and the flow that owns it', async () => {
    bridge.beginMcpOAuth = vi.fn().mockResolvedValue(offered);
    await expect(beginMcpOAuth('https://app.example.test/cb')).resolves.toEqual(offered);
    expect(bridge.beginMcpOAuth).toHaveBeenCalledWith('https://app.example.test/cb');
  });

  // Half an answer is no answer. Without the id nothing can be bound or stood
  // down afterwards, so a flow that arrived without one could never complete;
  // reading that as 'no listener' puts the caller on the browser path, which
  // still works.
  it.each([
    ['no flow id', { redirectUri: 'http://127.0.0.1:8788/mcp/callback' }],
    ['no URI', { flowId: 'f-1' }],
  ])('is undefined on %s', async (_label, half) => {
    bridge.beginMcpOAuth = vi.fn().mockResolvedValue(half);
    await expect(beginMcpOAuth('https://app.example.test/cb')).resolves.toBeUndefined();
  });

  // Every way there is nothing on offer answers the same, because the caller
  // has exactly one fallback for all of them and normalizing here is what lets
  // it get away with a single guard.
  it('is undefined with no method at all', async () => {
    await expect(beginMcpOAuth('https://app.example.test/cb')).resolves.toBeUndefined();
  });

  it.each([
    ['a null answer', null],
    ['an empty answer', ''],
  ])('is undefined on %s', async (_label, answer: unknown) => {
    bridge.beginMcpOAuth = vi.fn().mockResolvedValue(answer);
    await expect(beginMcpOAuth('https://app.example.test/cb')).resolves.toBeUndefined();
  });

  // A shell too old to know the channel rejects rather than answering, and that
  // must not take the connect down with it.
  it('is undefined on a throw', async () => {
    bridge.beginMcpOAuth = vi.fn().mockRejectedValue(new Error('no handler'));
    await expect(beginMcpOAuth('https://app.example.test/cb')).resolves.toBeUndefined();
  });
});

// Both of these exist so a listener is never left holding a flow that cannot
// happen. They part company on what a failure means: standing down is advice
// the page has no use for either way, while binding is the step that decides
// whether a callback can be received at all, so its answer has to survive the
// wrapper. A shell too old to know the channel armed nothing in the first
// place, and reports the same refusal.
describe('binding and standing down a flow', () => {
  it('names the flow it means', async () => {
    bridge.bindMcpOAuth = vi.fn().mockResolvedValue(true);
    bridge.cancelMcpOAuth = vi.fn().mockResolvedValue(true);
    await bindMcpOAuth('f-1', 'st-1');
    await cancelMcpOAuth('f-1');
    expect(bridge.bindMcpOAuth).toHaveBeenCalledWith('f-1', 'st-1');
    expect(bridge.cancelMcpOAuth).toHaveBeenCalledWith('f-1');
  });

  it('cancelMcpOAuth resolves with no method at all', async () => {
    await expect(cancelMcpOAuth('f-1')).resolves.toBeUndefined();
  });

  it('cancelMcpOAuth resolves on a throw', async () => {
    bridge.cancelMcpOAuth = vi.fn().mockRejectedValue(new Error('no handler'));
    await expect(cancelMcpOAuth('f-1')).resolves.toBeUndefined();
  });

  // Fail closed, all three ways. The caller uses this to decide whether to send
  // the user to a consent screen, so anything short of the shell saying yes has
  // to read as no -- an optimistic default here is a flow launched at a
  // listener that will refuse its callback, which looks like a hang.
  it.each([
    ['no method at all', undefined],
    ['a shell that says no', vi.fn().mockResolvedValue(false)],
    ['a channel that throws', vi.fn().mockRejectedValue(new Error('no handler'))],
  ])('bindMcpOAuth answers false on %s', async (_label, impl) => {
    if (impl) bridge.bindMcpOAuth = impl;
    await expect(bindMcpOAuth('f-1', 'st-1')).resolves.toBe(false);
  });

  it('bindMcpOAuth answers true only when the shell confirms it', async () => {
    bridge.bindMcpOAuth = vi.fn().mockResolvedValue(true);
    await expect(bindMcpOAuth('f-1', 'st-1')).resolves.toBe(true);
  });
});

describe('remote control', () => {
  const status = {
    supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: 'SHA256:k', account: 'alex',
  };

  // A switch the shell could not turn back off is not offered at all.
  it('is offered only with all three methods', () => {
    bridge.getRemoteControl = vi.fn();
    bridge.enableRemoteControl = vi.fn();
    expect(canRemoteControl()).toBe(false);
    bridge.disableRemoteControl = vi.fn();
    expect(canRemoteControl()).toBe(true);
  });

  // The shell answers null to a page it does not count as the app's.
  const WITHOUT_AN_ANSWER = [
    ['no method at all', undefined],
    ['a channel that throws', vi.fn().mockRejectedValue(new Error('no handler'))],
    ['a shell that answers null', vi.fn().mockResolvedValue(null)],
  ] as const;

  it.each(WITHOUT_AN_ANSWER)('reads and turns off as null on %s', async (_label, impl) => {
    if (impl) {
      bridge.getRemoteControl = impl;
      bridge.disableRemoteControl = impl;
    }
    await expect(getRemoteControl()).resolves.toBeNull();
    await expect(disableRemoteControl()).resolves.toBeNull();
  });

  it("hands back the shell's answer as it is", async () => {
    const refused = { ok: false, error: 'needs_approval', status };
    bridge.getRemoteControl = vi.fn().mockResolvedValue(status);
    bridge.enableRemoteControl = vi.fn().mockResolvedValue(refused);
    await expect(getRemoteControl()).resolves.toEqual(status);
    await expect(enableRemoteControl('https://app.example.test/pair.sh')).resolves.toEqual(refused);
    expect(bridge.enableRemoteControl).toHaveBeenCalledWith('https://app.example.test/pair.sh');
  });

  // Turning on is the one call whose failure the user has to see, so it
  // fails as an answer the setup words, never as a rejection.
  it.each(WITHOUT_AN_ANSWER)('fails turning on as unavailable on %s', async (_label, impl) => {
    if (impl) bridge.enableRemoteControl = impl;
    await expect(enableRemoteControl('https://app.example.test/pair.sh')).resolves.toEqual({
      ok: false, error: 'unavailable', status: null,
    });
  });
});
