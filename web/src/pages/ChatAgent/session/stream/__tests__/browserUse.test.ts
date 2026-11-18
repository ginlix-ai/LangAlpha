// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { ToolCallData } from '@/types/sse';
import { BrowserUseWatch } from '../browserUse';

const bash = (command: string, extra: Record<string, unknown> = {}, id = 'c1'): ToolCallData[] => (
  [{ id, name: 'Bash', args: { command, ...extra } }]
);
const usesBrowser = (toolCalls: ToolCallData[] | undefined) =>
  new BrowserUseWatch().seen('tool_calls', { tool_calls: toolCalls }) !== null;

describe('BrowserUseWatch', () => {
  it.each([
    'agent-browser open https://example.com',
    'cd /tmp && agent-browser snapshot -i',
    '(agent-browser close)',
    'x; agent-browser click @e1',
  ])('sees the browser in %s', (command) => {
    expect(usesBrowser(bash(command))).toBe(true);
  });

  it.each(['ls agent-browser-notes', 'echo my-agent-browser', 'cat agent-browser.log'])('is not fooled by %s', (command) => {
    expect(usesBrowser(bash(command))).toBe(false);
  });

  it('reads only the shell tool, and a call with no command or no calls at all', () => {
    expect(usesBrowser([{ id: 'c1', name: 'Read', args: { command: 'agent-browser open x' } }])).toBe(false);
    expect(usesBrowser([{ id: 'c1', name: 'Bash' }])).toBe(false);
    expect(usesBrowser(undefined)).toBe(false);
  });

  it('leaves out a call run on another of the user computers, whose browser this is not', () => {
    expect(usesBrowser(bash('agent-browser open x', { computer: 'Mac mini' }))).toBe(false);
    expect(usesBrowser(bash('agent-browser open x', { computer: '  ' }))).toBe(true);
    expect(usesBrowser(bash('agent-browser open x', { computer: null }))).toBe(true);
  });

  it('names the call and its command as it starts, and again once when its result comes back', () => {
    const watch = new BrowserUseWatch();
    expect(watch.seen('tool_calls', { tool_calls: [...bash('agent-browser open x', {}, 'b1'), ...bash('ls', {}, 'l1')] }))
      .toEqual({ id: 'b1', command: 'agent-browser open x', done: false, running: 'agent-browser open x' });
    expect(watch.seen('tool_call_result', { tool_call_id: 'l1' })).toBeNull();
    expect(watch.seen('tool_call_result', { tool_call_id: 'b1' }))
      .toEqual({ id: 'b1', command: 'agent-browser open x', done: true, running: null });
    expect(watch.seen('tool_call_result', { tool_call_id: 'b1' })).toBeNull();
  });

  it('names the latest call still running with each one, which is what the view captions', () => {
    const watch = new BrowserUseWatch();
    watch.seen('tool_calls', { tool_calls: bash('agent-browser open x', {}, 'a') });
    expect(watch.seen('tool_calls', { tool_calls: bash('agent-browser snapshot', {}, 'b') })?.running).toBe('agent-browser snapshot');
    expect(watch.seen('tool_call_result', { tool_call_id: 'b' })?.running).toBe('agent-browser open x');
    expect(watch.seen('tool_call_result', { tool_call_id: 'a' })?.running).toBeNull();
  });

  it('still names a call that came with no id, though its end cannot be heard', () => {
    const use = new BrowserUseWatch().seen('tool_calls', { tool_calls: [{ id: '', name: 'Bash', args: { command: 'agent-browser click @e1' } }] });
    expect(use?.running).toBe('agent-browser click @e1');
  });

  it('does not see the result of a call made on another computer', () => {
    const watch = new BrowserUseWatch();
    expect(watch.seen('tool_calls', { tool_calls: bash('agent-browser open x', { computer: 'Mac mini' }, 'b1') })).toBeNull();
    expect(watch.seen('tool_call_result', { tool_call_id: 'b1' })).toBeNull();
  });
});
