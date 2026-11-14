/**
 * Actions the agent asked to take from a command on a computer that wait on
 * the user: the server holds each until it is approved, declined or runs out
 * of time. App-wide, since the request can arrive while any page is open.
 *
 * Each action's `target` and `details` are what the server's builder for it
 * writes (`src/server/dependencies/cli_approval.py`).
 */
import { api } from '@/api/client';

/** Where work a request starts would run. In Home no workspace is named. */
export interface CliApprovalPlace {
  workspace_name: string | null;
  computer_name: string | null;
  in_home: boolean;
}

/** A thread's title, which a thread may not have yet. */
interface ThreadTarget {
  id: string;
  name: string | null;
}

type ApprovalRequest =
  | { action: 'thread_delete'; target: ThreadTarget; details: Record<string, never> }
  | { action: 'workspace_delete'; target: { id: string; name: string }; details: { delete_files: boolean } }
  | {
      action: 'workspace_create';
      target: { id: null; name: string };
      details: { description: string | null; computer_id: string | null; computer_name: string | null };
    }
  | { action: 'automation_delete'; target: { id: string; name: string }; details: Record<string, never> }
  | {
      action: 'computer_remove';
      target: { id: string; name: string };
      details: {
        /** Each is set only on an SSH machine, the one place it acts. */
        remove_key: boolean;
        delete_files: boolean;
        /** The workspaces on the computer, deleted with it. */
        delete_workspaces: string[];
        delete_workspace_names: (string | null)[];
      };
    }
  | {
      action: 'thread_send';
      target: ThreadTarget;
      details: CliApprovalPlace & ({ kind: 'send'; message_preview: string | null } | { kind: 'retry'; message_preview: null });
    }
  | {
      action: 'thread_create';
      /** The workspace the thread starts in; its name is the place's. */
      target: { id: string; name: string | null };
      details: CliApprovalPlace & { message_preview: string | null };
    }
  | {
      action: 'automation_change';
      /** No id for an automation still to be created. */
      target: { id: string | null; name: string };
      details: CliApprovalPlace & { kind: 'create' | 'edit' | 'trigger' | 'resume'; automation_name: string; prompt_preview: string | null };
    };

export type CliApprovalAction = ApprovalRequest['action'];

export type CliApproval = ApprovalRequest & {
  approval_id: string;
  /** The list holds only requests still waiting. */
  status: 'pending';
  requester: {
    kind: 'chief_of_staff' | 'analyst' | 'command';
    thread_id: string | null;
    thread_title: string | null;
    workspace_id: string | null;
    workspace_name: string | null;
    computer_name: string | null;
  };
  created_at: string;
  expires_at: string;
};

/** The requests still waiting, oldest first. */
export async function getPendingCliApprovals(): Promise<CliApproval[]> {
  const { data } = await api.get<{ approvals: CliApproval[] }>('/api/v1/cli/approvals');
  return data.approvals;
}

/** Answer one. A 409 (`approval_decided`) means it was answered elsewhere or ran out. */
export async function decideCliApproval(approvalId: string, approve: boolean): Promise<void> {
  await api.post(`/api/v1/cli/approvals/${encodeURIComponent(approvalId)}/decision`, { approve });
}
