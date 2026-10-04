import { api, type ApiResponse, type QueryParams } from '@/api/client';
import type {
  Automation,
  AutomationExecution,
  AutomationPayload,
  AutomationRun,
  AutomationUpdatePayload,
  DeliveryOptions,
} from '@/types/automation';

export interface AutomationList {
  automations: Automation[];
  total: number;
}

export interface ExecutionList {
  executions: AutomationExecution[];
  has_more: boolean;
}

export interface RunFeedPage {
  executions: AutomationRun[];
  has_more: boolean;
}

export const listAutomations = (params: QueryParams): Promise<ApiResponse<AutomationList>> =>
  api.get('/api/v1/automations', { params });

export const createAutomation = (data: AutomationPayload): Promise<ApiResponse<Automation>> =>
  api.post('/api/v1/automations', data);

export const updateAutomation = (id: string, data: AutomationUpdatePayload): Promise<ApiResponse<Automation>> =>
  api.patch(`/api/v1/automations/${id}`, data);

export const deleteAutomation = (id: string): Promise<ApiResponse> =>
  api.delete(`/api/v1/automations/${id}`);

export const pauseAutomation = (id: string): Promise<ApiResponse> =>
  api.post(`/api/v1/automations/${id}/pause`);

export const resumeAutomation = (id: string): Promise<ApiResponse> =>
  api.post(`/api/v1/automations/${id}/resume`);

export const triggerAutomation = (id: string): Promise<ApiResponse> =>
  api.post(`/api/v1/automations/${id}/trigger`);

export const listExecutions = (id: string, params: QueryParams): Promise<ApiResponse<ExecutionList>> =>
  api.get(`/api/v1/automations/${id}/executions`, { params });

/** Every automation's runs, newest first: the feed. `thread_id` and
 *  `status` narrow it, e.g. to what waits on one thread. */
export const listRecentRuns = (params: QueryParams): Promise<ApiResponse<RunFeedPage>> =>
  api.get('/api/v1/automations/executions', { params });

/** Skip a run waiting for the turn in its thread to end. */
export const skipRun = (automationId: string, executionId: string): Promise<ApiResponse> =>
  api.post(`/api/v1/automations/${automationId}/executions/${executionId}/skip`);

/** Acknowledge a failed run, which takes its automation out of Needs
 *  attention until a newer run fails. */
export const dismissRun = (automationId: string, executionId: string): Promise<ApiResponse<Automation>> =>
  api.post(`/api/v1/automations/${automationId}/executions/${executionId}/dismiss`);

/** The chats each linked app offers an automation in this workspace, and
 *  where its bare entry lands now. */
export const getDeliveryOptions = (workspaceId: string): Promise<ApiResponse<DeliveryOptions>> =>
  api.get('/api/v1/automations/delivery-options', { params: { workspace_id: workspaceId } });

export interface DeliveryDefaultPayload {
  workspace_id: string;
  platform: string;
  /** null clears the workspace's default for the app. */
  address: string | null;
}

/** Set or clear the chat an app's bare entry lands in for one workspace. */
export const setDeliveryDefault = (
  data: DeliveryDefaultPayload,
): Promise<ApiResponse<{ address: string | null; name: string | null }>> =>
  api.put('/api/v1/automations/delivery-default', data);
