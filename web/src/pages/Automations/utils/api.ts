import type { AxiosResponse } from 'axios';
import { api } from '@/api/client';
import type {
  Automation,
  AutomationExecution,
  AutomationPayload,
  AutomationRun,
  AutomationUpdatePayload,
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

export const listAutomations = (params: Record<string, unknown>): Promise<AxiosResponse<AutomationList>> =>
  api.get('/api/v1/automations', { params });

export const createAutomation = (data: AutomationPayload): Promise<AxiosResponse<Automation>> =>
  api.post('/api/v1/automations', data);

export const updateAutomation = (id: string, data: AutomationUpdatePayload): Promise<AxiosResponse<Automation>> =>
  api.patch(`/api/v1/automations/${id}`, data);

export const deleteAutomation = (id: string): Promise<AxiosResponse> =>
  api.delete(`/api/v1/automations/${id}`);

export const pauseAutomation = (id: string): Promise<AxiosResponse> =>
  api.post(`/api/v1/automations/${id}/pause`);

export const resumeAutomation = (id: string): Promise<AxiosResponse> =>
  api.post(`/api/v1/automations/${id}/resume`);

export const triggerAutomation = (id: string): Promise<AxiosResponse> =>
  api.post(`/api/v1/automations/${id}/trigger`);

export const listExecutions = (id: string, params: Record<string, unknown>): Promise<AxiosResponse<ExecutionList>> =>
  api.get(`/api/v1/automations/${id}/executions`, { params });

/** Every automation's runs, newest first: the feed. `thread_id` and
 *  `status` narrow it, e.g. to what waits on one thread. */
export const listRecentRuns = (params: Record<string, unknown>): Promise<AxiosResponse<RunFeedPage>> =>
  api.get('/api/v1/automations/executions', { params });

/** Skip a run waiting for the turn in its thread to end. */
export const skipRun = (automationId: string, executionId: string): Promise<AxiosResponse> =>
  api.post(`/api/v1/automations/${automationId}/executions/${executionId}/skip`);

/** Acknowledge a failed run, which takes its automation out of Needs
 *  attention until a newer run fails. */
export const dismissRun = (automationId: string, executionId: string): Promise<AxiosResponse<Automation>> =>
  api.post(`/api/v1/automations/${automationId}/executions/${executionId}/dismiss`);
