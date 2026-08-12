/**
 * ChatAgent API utilities — permanent façade over utils/api/.
 * All backend endpoints used by the ChatAgent page.
 *
 * Consumers (and the 38 whole-module vi.mock factories) import THIS module;
 * the domain leaves under api/ are implementation. streamFetch/postSSEStream/
 * getAuthHeaders/baseURL stay package-internal (transport.ts) on purpose.
 */
export { apiErrorDetailMessage, formatApiErrorDetail, apiErrorStatus } from './api/errors';
export { parseRunIdFromContentLocation, parseThreadIdFromContentLocation } from './api/transport';
export * from './api/workspaces';
export * from './api/threads';
export * from './api/messages';
export * from './api/files';
export * from './api/sandbox';
export * from './api/metadata';
export * from './api/feedback';
export * from './api/vault';
export * from './api/memory';
export * from './api/memos';
export * from './api/mcp';
