/**
 * What a waiting request says, in the user's terms: the question, who asked
 * and where, and what goes with it. Shared by the dialog and the desktop
 * notification, so both word a request the same way.
 */
import type { TFunction } from 'i18next';
import {
  CalendarClock,
  CalendarCog,
  CalendarPlus,
  CalendarX,
  FolderPlus,
  FolderX,
  MessageSquarePlus,
  MessageSquareX,
  MonitorX,
  Play,
  RotateCcw,
  Send,
  type LucideIcon,
} from 'lucide-react';
import type { CliApproval, CliApprovalPlace } from '@/api/cliApprovals';
import { formatClock } from '@/lib/elapsed';

/**
 * One per way a request reads: its action, split where the kind of send or
 * automation change, or a new thread's place in Home, changes the words. Each
 * names its keys under `cliApproval.title.*` and `cliApproval.ask.*`.
 */
export type ApprovalVariant =
  | 'threadDelete'
  | 'workspaceDelete'
  | 'workspaceCreate'
  | 'automationDelete'
  | 'computerRemove'
  | 'threadSend'
  | 'threadRetry'
  | 'threadCreate'
  | 'threadCreateHome'
  | 'automationCreate'
  | 'automationEdit'
  | 'automationTrigger'
  | 'automationResume';

interface VariantConfig {
  icon: LucideIcon;
  approveKey: string;
  /** Takes something away for good: the destructive button, and a note that it can't be undone. */
  destructive: boolean;
  /** Its target is a thread, which may have no title yet, so its title has an unnamed form. */
  mayBeUntitled?: true;
}

export const VARIANT: Record<ApprovalVariant, VariantConfig> = {
  threadDelete: { icon: MessageSquareX, approveKey: 'common.delete', destructive: true, mayBeUntitled: true },
  workspaceDelete: { icon: FolderX, approveKey: 'common.delete', destructive: true },
  workspaceCreate: { icon: FolderPlus, approveKey: 'common.create', destructive: false },
  automationDelete: { icon: CalendarX, approveKey: 'common.delete', destructive: true },
  computerRemove: { icon: MonitorX, approveKey: 'cliApproval.remove', destructive: true },
  threadSend: { icon: Send, approveKey: 'cliApproval.send', destructive: false, mayBeUntitled: true },
  threadRetry: { icon: RotateCcw, approveKey: 'common.retry', destructive: false, mayBeUntitled: true },
  threadCreate: { icon: MessageSquarePlus, approveKey: 'cliApproval.start', destructive: false },
  threadCreateHome: { icon: MessageSquarePlus, approveKey: 'cliApproval.start', destructive: false },
  automationCreate: { icon: CalendarPlus, approveKey: 'common.create', destructive: false },
  automationEdit: { icon: CalendarCog, approveKey: 'common.save', destructive: false },
  automationTrigger: { icon: Play, approveKey: 'cliApproval.run', destructive: false },
  automationResume: { icon: CalendarClock, approveKey: 'cliApproval.resume', destructive: false },
};

const AUTOMATION_VARIANT = {
  create: 'automationCreate',
  edit: 'automationEdit',
  trigger: 'automationTrigger',
  resume: 'automationResume',
} as const;

/** The one place a request's action, kind and place decide how it reads. */
export function variant(a: CliApproval): ApprovalVariant {
  switch (a.action) {
    case 'thread_delete':
      return 'threadDelete';
    case 'workspace_delete':
      return 'workspaceDelete';
    case 'workspace_create':
      return 'workspaceCreate';
    case 'automation_delete':
      return 'automationDelete';
    case 'computer_remove':
      return 'computerRemove';
    case 'thread_send':
      return a.details.kind === 'retry' ? 'threadRetry' : 'threadSend';
    case 'thread_create':
      return a.details.in_home ? 'threadCreateHome' : 'threadCreate';
    case 'automation_change':
      return AUTOMATION_VARIANT[a.details.kind];
  }
}

const clean = (value: string | null | undefined): string => value?.trim() ?? '';

/** The dialog's question: "Delete workspace “Research”?". */
export function approvalTitle(t: TFunction, a: CliApproval): string {
  const v = variant(a);
  const name = clean(a.target.name);
  return name || !VARIANT[v].mayBeUntitled ? t(`cliApproval.title.${v}`, { name }) : t(`cliApproval.title.${v}Unnamed`);
}

/** Where an automation runs, as a fragment the ask sentence carries, or none when nothing names it. */
function automationWhere(t: TFunction, place: CliApprovalPlace): string {
  const computer = clean(place.computer_name);
  const workspace = clean(place.workspace_name);
  if (place.in_home) {
    return computer ? t('cliApproval.where.homeOn', { computer }) : t('cliApproval.where.home');
  }
  if (workspace) {
    return computer ? t('cliApproval.where.workspaceOn', { workspace, computer }) : t('cliApproval.where.workspace', { workspace });
  }
  return computer ? t('cliApproval.where.on', { computer }) : '';
}

/** What was asked, as the end of "… asked to {{ask}}". */
function approvalAsk(t: TFunction, a: CliApproval): string {
  const key = `cliApproval.ask.${variant(a)}`;
  switch (a.action) {
    case 'workspace_delete':
      return t(a.details.delete_files ? `${key}Files` : key);
    case 'computer_remove':
      return t(a.details.delete_workspaces.length ? `${key}Workspaces` : key);
    case 'workspace_create':
    case 'thread_send':
    case 'thread_create': {
      const computer = clean(a.details.computer_name);
      return computer ? t(`${key}On`, { computer }) : t(key);
    }
    case 'automation_change': {
      const where = automationWhere(t, a.details);
      return where ? t(`${key}Where`, { where }) : t(key);
    }
    default:
      return t(key);
  }
}

export interface RequestSentence {
  /** A key whose thread title, when it has one, sits in a `<thread>` tag. */
  key: string;
  values: Record<string, string>;
}

/** "Your Chief of Staff asked in “Morning brief” to delete this workspace and its files." */
export function requestSentence(t: TFunction, a: CliApproval): RequestSentence {
  const { requester } = a;
  const values: Record<string, string> = { ask: approvalAsk(t, a) };
  let from: string;
  if (requester.kind === 'chief_of_staff') {
    from = 'chiefOfStaff';
  } else if (requester.kind === 'analyst') {
    const workspace = clean(requester.workspace_name);
    from = workspace ? 'analystOf' : 'analyst';
    if (workspace) values.workspace = workspace;
  } else {
    const computer = clean(requester.computer_name);
    from = computer ? 'commandOn' : 'command';
    if (computer) values.computer = computer;
  }
  const thread = clean(requester.thread_title);
  if (thread && requester.thread_id) {
    values.thread = thread;
    from += 'InThread';
  }
  return { key: `cliApproval.from.${from}`, values };
}

/**
 * The sentence as plain text, for a surface that cannot hold the thread link.
 * The `<thread>` tag is markup for `Trans` alone, so dropping it is the whole
 * difference; a second set of keys without it would only drift from the first.
 */
export function requestSentenceText(t: TFunction, a: CliApproval): string {
  const { key, values } = requestSentence(t, a);
  return t(key, values).replace(/<\/?thread>/g, '');
}

/** What else the answer settles, beyond the question: one line each. */
export function approvalNotes(t: TFunction, a: CliApproval): string[] {
  const notes: string[] = [];
  if (a.action === 'computer_remove') {
    const names = a.details.delete_workspace_names.map(clean).filter(Boolean);
    if (names.length) notes.push(t('cliApproval.note.computerWorkspaces', { names: names.join(', ') }));
    if (a.details.delete_files) notes.push(t('cliApproval.note.computerFiles'));
    if (a.details.remove_key) notes.push(t('cliApproval.note.computerKey'));
  }
  if (VARIANT[variant(a)].destructive) notes.push(t('cliApproval.note.cannotUndo'));
  return notes;
}

/** The text the request carries: a workspace's description, or the start of the message or prompt it sends. */
export function approvalPreview(a: CliApproval): string {
  switch (a.action) {
    case 'workspace_create':
      return clean(a.details.description);
    case 'thread_send':
    case 'thread_create':
      return clean(a.details.message_preview);
    case 'automation_change':
      return clean(a.details.prompt_preview);
    default:
      return '';
  }
}

/** Time left as m:ss, or h:mm:ss past the hour; rounded up, so it reads 0:00 only once it is gone. */
export function formatCountdown(ms: number): string {
  return formatClock(Math.max(0, Math.ceil(ms / 1000) * 1000));
}
