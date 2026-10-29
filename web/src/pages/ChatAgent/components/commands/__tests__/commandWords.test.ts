/**
 * How a background command reads: by its description or else its tool's
 * name, where it stands in the approved words, and which of them the tray
 * lists.
 */
import { afterEach, describe, expect, it } from 'vitest';
import i18n from '@/i18n';
import { switchLocale } from '@/lib/locale';
import type { CommandRun } from '@/types/commands';
import { commandName, commandStateWords, isCommandTool, runningCommands } from '../commandWords';

const t = i18n.t.bind(i18n);
const M = 60_000;
const START = Date.parse('2030-01-01T10:00:00Z');
const at = (minutes: number) => new Date(START + minutes * M).toISOString();

function command(over: Partial<CommandRun>): CommandRun {
  return {
    command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest', status: 'running',
    exit_code: null, failure_reason: null, tool_call_id: 'call_1', log_path: '.agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log',
    started_at: at(0), handed_off_at: at(5), ended_at: null,
    ...over,
  } as CommandRun;
}

afterEach(async () => {
  await switchLocale(i18n, 'en-US');
});

describe('commandStateWords', () => {
  it('counts a running command from its start, the wait included', () => {
    expect(commandStateWords(command({}), START + 17 * M + 20_000, t)).toBe('In background 17m');
  });

  it.each<[Partial<CommandRun>, string]>([
    [{ status: 'completed', exit_code: 0, ended_at: at(41) }, 'Completed in 41m'],
    [{ status: 'failed', exit_code: 1, failure_reason: 'exit', ended_at: at(7) }, 'Failed after 7m'],
    [{ status: 'failed', failure_reason: 'lost', ended_at: at(7) }, 'Failed after 7m'],
    [{ status: 'failed', failure_reason: 'after_turn_limit', ended_at: at(112) }, 'Stopped early after 1h 52m'],
    [{ status: 'stopped', ended_at: at(30) }, 'Stopped after 30m'],
  ])('says how an ended command ended and how long it took: %o', (over, words) => {
    expect(commandStateWords(command(over), START + 999 * M, t)).toBe(words);
  });

  it('says how it ended alone when its end reads before its start', () => {
    expect(commandStateWords(command({ status: 'completed', ended_at: at(-1) }), START, t)).toBe('Completed');
    expect(commandStateWords(command({ status: 'failed', failure_reason: 'after_turn_limit', ended_at: at(-1) }), START, t))
      .toBe('Stopped early');
  });

  it('reads in Chinese with the duration words the rest of the chat uses', async () => {
    await switchLocale(i18n, 'zh-CN');
    expect(commandStateWords(command({ status: 'completed', ended_at: at(41) }), START, t)).toBe('已完成，用时 41 分钟');
    expect(commandStateWords(command({}), START + 17 * M, t)).toBe('后台运行 17 分钟');
  });
});

describe('commandName', () => {
  it('names a command by the first line of its description', () => {
    expect(commandName('Run 10-year backtest\nthen chart it', 'Bash', t)).toBe('Run 10-year backtest');
  });

  it("falls back to the tool's own name, never the command's id", () => {
    expect(commandName(null, 'Bash', t)).toBe('Bash');
    expect(commandName('', 'ExecuteCode', t)).toBe('Execute Code');
  });
});

describe('runningCommands', () => {
  it('keeps the running ones, oldest first', () => {
    const later = command({ command_id: 'bg-b', started_at: at(10) });
    const earlier = command({ command_id: 'bg-a', started_at: at(2) });
    const ended = command({ command_id: 'bg-c', status: 'completed', ended_at: at(3) });
    expect(runningCommands([later, ended, earlier]).map((c) => c.command_id)).toEqual(['bg-a', 'bg-b']);
  });
});

describe('isCommandTool', () => {
  it('is only the two tools that hand off', () => {
    expect(isCommandTool('Bash')).toBe(true);
    expect(isCommandTool('ExecuteCode')).toBe(true);
    expect(isCommandTool('BashOutput')).toBe(false);
    expect(isCommandTool(undefined)).toBe(false);
  });
});
