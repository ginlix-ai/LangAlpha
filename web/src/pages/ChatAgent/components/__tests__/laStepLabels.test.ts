/**
 * An `la` step's row: named for what it did only once it is known to have
 * done it, its object named only from the command or the app's own names.
 */
import { describe, expect, it } from 'vitest';
import i18n from '@/i18n';
import type { TFn } from '@/lib/elapsed';
import { getActiveLabel, getCompletedRowTitle, getCompletedSummary, type ToolCallOutcome } from '../toolDisplayConfig';
import { completedToolSummary, completedToolTitle } from '../messageList/activitySummary';
import type { ToolActivityItem } from '../messageList/activityTypes';
import type { LaNames } from '../laStepLabels';

const en = i18n.getFixedT('en-US') as TFn;
const zh = i18n.getFixedT('zh-CN') as TFn;
const ID = '346bee3b-8f31-43d3-b8a3-f6489e03d7aa';
const WS = '0f1e2d3c-4b5a-4968-8776-655443322110';
const names: LaNames = new Map([[`thread:${ID}`, 'NVDA fair value'], [`workspace:${WS}`, 'Semis']]);

const bash = (command: string) => ({ args: { command, description: 'Run la' } });
// What a title reads of a result: its artifact, and whether Bash's text says it failed.
const ok = { record: { toolCallResult: {} }, names };

describe('a settled la step', () => {
  it('reads as what it did, with the object it did it to', () => {
    const call = bash(`la threads output ${ID}`);
    expect(getCompletedRowTitle('Bash', call, en, ok)).toBe('Read thread');
    expect(getCompletedSummary('Bash', call, en, ok)).toBe('NVDA fair value');
    expect(getCompletedRowTitle('Bash', call, zh, ok)).toBe('已读取对话内容');
  });

  it('names a new workspace from its own argument', () => {
    const call = bash(`la workspaces create 'Model test'`);
    expect(getCompletedRowTitle('Bash', call, en, ok)).toBe('Created workspace');
    expect(getCompletedSummary('Bash', call, en, ok)).toBe('Model test');
  });

  it('names Home as the app does, and a workspace id only from the app', () => {
    expect(getCompletedSummary('Bash', bash('la threads list -w home'), en, ok)).toBe('Chief of Staff');
    expect(getCompletedSummary('Bash', bash('la threads list -w home'), zh, ok)).toBe('CoS Agent');
    expect(getCompletedSummary('Bash', bash(`la workspaces delete ${WS} -y`), en, ok)).toBe('Semis');
    expect(getCompletedRowTitle('Bash', bash(`la workspaces delete ${WS} -y`), en, ok)).toBe('Deleted workspace');
  });

  it('shows no pill for an id the app holds no name for', () => {
    const call = bash('la threads delete 11111111-2222-4333-8444-555555555555 -y');
    expect(getCompletedRowTitle('Bash', call, en, ok)).toBe('Deleted thread');
    expect(getCompletedSummary('Bash', call, en, ok)).toBeNull();
    expect(getCompletedSummary('Bash', bash('la models list'), en, ok)).toBeNull();
  });

  it('keeps the generic row when it failed, moved to the background, or its outcome is unknown', () => {
    const call = bash(`la threads delete ${ID} -y`);
    const records: Array<ToolCallOutcome | undefined> = [
      { toolCallResult: {}, isFailed: true },
      { toolCallResult: {}, _liveState: 'failed' },
      // As the stream delivers a nonzero exit: an artifact, and nothing marked failed.
      { toolCallResult: { content: 'ERROR: Command failed (exit code 1)\nla: Not found', artifact: {} } },
      { toolCallResult: null },
      undefined,
      { toolCallResult: { artifact: { command_id: 'bg-1' } } },
    ];
    for (const record of records) {
      const context = { record };
      expect(getCompletedRowTitle('Bash', call, en, context)).toBe('Bash');
      expect(getCompletedSummary('Bash', call, en, context)).toBe('Run la');
    }
  });

  it('keeps the generic row for anything it does not read exactly', () => {
    const call = bash('la threads list | head -5');
    expect(getCompletedRowTitle('Bash', call, en, ok)).toBe('Bash');
    expect(getCompletedSummary('Bash', call, en, ok)).toBe('Run la');
  });

  it('names an archive for the computer it archives', () => {
    const call = bash(`la workspaces archive ${WS}`);
    expect(getCompletedRowTitle('Bash', call, en, ok)).toBe("Archived workspace's computer");
    expect(getCompletedSummary('Bash', call, en, ok)).toBe('Semis');
  });

  it('reads a call a public share sent without its arguments as plain Bash', () => {
    expect(getCompletedRowTitle('Bash', { args: {} }, en, ok)).toBe('Bash');
    expect(getCompletedSummary('Bash', { args: {} }, en, ok)).toBeNull();
  });
});

describe('a running la step', () => {
  it('reads as the phrase alone, with no tool name before it', () => {
    expect(getActiveLabel('Bash', bash(`la threads delete ${ID} -y`), en)).toBe('deleting thread...');
    expect(getActiveLabel('Bash', bash('la workspaces list --include-home'), zh)).toBe('列出工作区...');
    expect(getActiveLabel('Bash', bash('la threads list; ls'), en)).toBe('Bash Run la...');
  });
});

describe('a timeline item', () => {
  const item = (over: Partial<ToolActivityItem>): ToolActivityItem => ({
    type: 'tool_call', id: 'c1', toolCallId: 'c1', toolName: 'Bash',
    toolCall: bash(`la threads cancel ${ID}`),
    toolCallResult: { content: 'Cancel requested.' },
    isComplete: true, _liveState: 'completed',
    ...over,
  } as ToolActivityItem);

  it('succeeds only with a result that is not a failure', () => {
    expect(completedToolTitle(item({}), en)).toBe("Cancelled thread's turn");
    expect(completedToolSummary(item({}), en, names)).toBe('NVDA fair value');
    expect(completedToolTitle(item({ isFailed: true }), en)).toBe('Bash');
    expect(completedToolTitle(item({ _liveState: 'failed' }), en)).toBe('Bash');
    expect(completedToolTitle(item({ toolCallResult: undefined }), en)).toBe('Bash');
  });
});
