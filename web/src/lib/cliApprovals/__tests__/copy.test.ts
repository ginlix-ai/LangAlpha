// @vitest-environment node
/**
 * The request's wording outside the dialog: in Chinese, and as plain text for
 * a desktop notification, where the thread cannot be a link.
 */
import { describe, expect, it } from 'vitest';
import i18n from '@/i18n';
import type { CliApproval } from '@/api/cliApprovals';
import { approvalNotes, approvalTitle, formatCountdown, requestSentenceText } from '../copy';

const base: CliApproval = {
  approval_id: 'ap-1',
  status: 'pending',
  action: 'workspace_delete',
  target: { id: 'w-1', name: 'Research' },
  details: { delete_files: true },
  requester: {
    kind: 'chief_of_staff',
    thread_id: 't-1',
    thread_title: 'Morning brief',
    workspace_id: null,
    workspace_name: null,
    computer_name: null,
  },
  created_at: '2026-11-14T00:00:00Z',
  expires_at: '2026-11-14T00:05:00Z',
};

describe('cliApprovals copy', () => {
  it('reads as plain text, the thread unlinked', () => {
    const t = i18n.getFixedT('en-US');
    expect(approvalTitle(t, base)).toBe('Delete workspace “Research”?');
    expect(requestSentenceText(t, base)).toBe(
      'Your Chief of Staff asked in “Morning brief” to delete this workspace, its threads and its files.',
    );
  });

  it('words each requester in Chinese', () => {
    const t = i18n.getFixedT('zh-CN');
    expect(approvalTitle(t, base)).toBe('删除工作区“Research”？');
    expect(requestSentenceText(t, base)).toBe('CoS Agent 在“Morning brief”中请求删除这个工作区及其中的对话和文件。');
    const analyst = { ...base, requester: { ...base.requester, kind: 'analyst' as const, workspace_name: 'NVDA' } };
    expect(requestSentenceText(t, analyst)).toBe('“NVDA”的分析师在“Morning brief”中请求删除这个工作区及其中的对话和文件。');
    const command: CliApproval = {
      ...base,
      action: 'computer_remove',
      target: { id: 'c-1', name: 'Box' },
      details: { remove_key: false, delete_files: false, delete_workspaces: [], delete_workspace_names: [] },
      requester: { ...base.requester, kind: 'command', thread_id: null, thread_title: null, computer_name: 'My Mac' },
    };
    expect(approvalTitle(t, command)).toBe('移除计算机“Box”？');
    expect(requestSentenceText(t, command)).toBe('“My Mac”上运行的命令请求移除这台计算机。');
  });

  it('words the requests that start work, in English and Chinese', () => {
    const analyst = { ...base.requester, kind: 'analyst' as const, workspace_name: 'NVDA', thread_title: 'Earnings prep' };
    const at = (request: Pick<CliApproval, 'action' | 'target' | 'details'>): CliApproval =>
      ({ ...base, requester: analyst, ...request }) as CliApproval;
    const send = at({
      action: 'thread_send',
      target: { id: 't-5', name: 'Q3 prep' },
      details: { kind: 'send', message_preview: 'Hi', workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
    });
    const retry = at({
      action: 'thread_send',
      target: { id: 't-5', name: 'Q3 prep' },
      details: { kind: 'retry', message_preview: null, workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
    });
    const home = at({
      action: 'thread_create',
      target: { id: 'w-h', name: null },
      details: { message_preview: null, workspace_name: null, computer_name: 'Laptop', in_home: true },
    });
    const named = at({
      action: 'thread_create',
      target: { id: 'w-3', name: 'AMD' },
      details: { message_preview: null, workspace_name: 'AMD', computer_name: null, in_home: false },
    });
    const trigger = at({
      action: 'automation_change',
      target: { id: 'a-1', name: 'Morning brief' },
      details: {
        kind: 'trigger',
        automation_name: 'Morning brief',
        prompt_preview: null,
        workspace_name: null,
        computer_name: 'Laptop',
        in_home: true,
      },
    });
    const resume = at({
      action: 'automation_change',
      target: { id: 'a-1', name: 'Morning brief' },
      details: {
        kind: 'resume',
        automation_name: 'Morning brief',
        prompt_preview: null,
        workspace_name: 'AMD',
        computer_name: 'Laptop',
        in_home: false,
      },
    });
    const create = at({
      action: 'automation_change',
      target: { id: null, name: 'Morning brief' },
      details: {
        kind: 'create',
        automation_name: 'Morning brief',
        prompt_preview: null,
        workspace_name: 'AMD',
        computer_name: 'Laptop',
        in_home: false,
      },
    });
    const editNowhere = at({
      action: 'automation_change',
      target: { id: 'a-1', name: 'Morning brief' },
      details: {
        kind: 'edit',
        automation_name: 'Morning brief',
        prompt_preview: null,
        workspace_name: null,
        computer_name: null,
        in_home: false,
      },
    });

    const en = i18n.getFixedT('en-US');
    expect(approvalTitle(en, send)).toBe('Send a message to “Q3 prep”?');
    expect(approvalTitle(en, retry)).toBe('Retry the failed turn in “Q3 prep”?');
    expect(requestSentenceText(en, retry)).toBe(
      "The analyst in “NVDA” asked in “Earnings prep” to run this thread's failed turn again on “Laptop”.",
    );
    expect(approvalTitle(en, home)).toBe('Start a thread with your Chief of Staff?');
    expect(approvalTitle(en, named)).toBe('Start a thread in “AMD”?');
    expect(approvalTitle(en, trigger)).toBe('Run automation “Morning brief”?');
    expect(requestSentenceText(en, trigger)).toBe(
      'The analyst in “NVDA” asked in “Earnings prep” to run this automation now, with your Chief of Staff on “Laptop”.',
    );
    expect(approvalTitle(en, editNowhere)).toBe('Change automation “Morning brief”?');
    expect(requestSentenceText(en, editNowhere)).toBe('The analyst in “NVDA” asked in “Earnings prep” to change this automation.');
    expect(approvalTitle(en, resume)).toBe('Resume automation “Morning brief”?');
    expect(requestSentenceText(en, resume)).toBe(
      'The analyst in “NVDA” asked in “Earnings prep” to resume this automation, which runs in “AMD” on “Laptop”.',
    );
    for (const a of [send, retry, home, trigger, resume, create, editNowhere]) expect(approvalNotes(en, a)).toEqual([]);

    const zh = i18n.getFixedT('zh-CN');
    expect(approvalTitle(zh, send)).toBe('向对话“Q3 prep”发送消息？');
    expect(requestSentenceText(zh, send)).toBe('“NVDA”的分析师在“Earnings prep”中请求在“Laptop”上向这个对话发送消息。');
    expect(approvalTitle(zh, retry)).toBe('重试对话“Q3 prep”中失败的一轮？');
    expect(approvalTitle(zh, home)).toBe('与 CoS Agent 新建对话？');
    expect(requestSentenceText(zh, home)).toBe('“NVDA”的分析师在“Earnings prep”中请求在“Laptop”上与 CoS Agent 新建对话。');
    expect(approvalTitle(zh, create)).toBe('创建自动化“Morning brief”？');
    expect(requestSentenceText(zh, create)).toBe(
      '“NVDA”的分析师在“Earnings prep”中请求创建这个自动化，它将在“Laptop”上的“AMD”中运行。',
    );
    expect(approvalTitle(zh, resume)).toBe('继续自动化“Morning brief”？');
    expect(requestSentenceText(zh, resume)).toBe(
      '“NVDA”的分析师在“Earnings prep”中请求继续这个自动化，它在“Laptop”上的“AMD”中运行。',
    );
    expect(requestSentenceText(zh, trigger)).toBe(
      '“NVDA”的分析师在“Earnings prep”中请求立即由 CoS Agent 在“Laptop”上运行这个自动化。',
    );
  });

  it('counts down in minutes, and in hours past one', () => {
    expect(formatCountdown(90_000)).toBe('1:30');
    expect(formatCountdown(59_001)).toBe('1:00');
    expect(formatCountdown(3_725_000)).toBe('1:02:05');
    expect(formatCountdown(-5)).toBe('0:00');
  });
});
