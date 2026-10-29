/**
 * ChatAgent API utilities — permanent façade over utils/api/.
 * All backend endpoints used by the ChatAgent page.
 *
 * Consumers (and the 38 whole-module vi.mock factories) import THIS module;
 * the domain leaves under api/ are implementation. streamFetch/postSSEStream/
 * getAuthHeaders/baseURL stay package-internal (transport.ts) on purpose.
 *
 * api/package.json declares the leaves side-effect-free, so a name imported
 * through here brings in only the leaf that defines it. Keep module-scope work
 * out of the leaves unless it only matters once their exports are used.
 */
export {
  apiErrorDetailMessage,
  apiErrorObject,
  apiErrorReason,
  formatApiErrorDetail,
  apiErrorStatus,
} from '@/api/errors';
export { parseRunIdFromContentLocation, parseThreadIdFromContentLocation } from './api/transport';
export * from './api/workspaces';
export * from './api/computers';
export * from './api/threads';
export * from './api/messages';
export * from './api/files';
export * from './api/sandbox';
export * from './api/metadata';
export * from './api/feedback';
export * from './api/userVault';
export * from './api/memory';
export * from './api/memos';
export * from './api/mcp';
export * from './api/mcpBuiltins';
export * from './api/mcpOauth';
export * from './api/brokerages';
export * from './api/skills';
export * from './api/subagents';
export * from './api/plugins';
export * from './api/marketplace';
export * from './api/orders';
