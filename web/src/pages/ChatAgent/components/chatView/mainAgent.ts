import { ZERO_USAGE } from '../../utils/tokenUsage';
import type { AgentInfo } from './types';

// Static main agent object — never changes, so defined once at module level
export const MAIN_AGENT: AgentInfo = {
  id: 'main',
  name: 'Lead Agent',
  displayName: 'LangAlpha',
  taskId: '',
  description: '',
  type: 'main',
  status: 'active',
  toolCalls: 0,
  tokenUsage: ZERO_USAGE,
  currentTool: '',
  messages: [],
  isActive: true,
  isMainAgent: true,
};
