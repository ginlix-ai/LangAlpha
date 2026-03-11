/** Automation types — automation CRUD and execution records */

export interface Automation {
  id: string;
  name: string;
  description?: string;
  schedule?: string;
  workspace_id?: string;
  status: 'active' | 'paused' | 'error';
  prompt?: string;
  config?: Record<string, unknown>;
  created_at?: string;
  updated_at?: string;
  last_run_at?: string | null;
  next_run_at?: string | null;
  [key: string]: unknown;
}

export interface AutomationExecution {
  id: string;
  automation_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  started_at?: string;
  completed_at?: string | null;
  result?: unknown;
  error?: string | null;
  [key: string]: unknown;
}

export interface AutomationCreatePayload {
  name: string;
  description?: string;
  schedule?: string;
  workspace_id?: string;
  prompt?: string;
  config?: Record<string, unknown>;
}

export interface AutomationUpdatePayload {
  name?: string;
  description?: string;
  schedule?: string;
  workspace_id?: string;
  prompt?: string;
  config?: Record<string, unknown>;
}
