/**
 * Shared interrupt descriptors — the one place the HITL card vocabulary is
 * declared. Both projections (live stream and history replay) key off these
 * tables; the dedup rebuild in messageFinalizers walks INTERRUPT_CARD_BUCKETS.
 * A new interrupt type registers here first, then adds its projection branches.
 */

import type { AssistantMessage } from '@/types/chat';

/** Interrupt types that map to proposal-based HITL cards (workspace, question, ptc, secretary). */
const PROPOSAL_INTERRUPT_TYPES = new Set([
  'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread',
]);

/** Maps interrupt types to their proposal bucket key on AssistantMessage. */
const PROPOSAL_DATA_KEY_MAP: Record<string, string> = {
  create_workspace: 'workspaceProposals',
  start_question: 'questionProposals',
  ptc_agent: 'ptcAgentProposals',
  delete_workspace: 'secretaryActionProposals',
  stop_workspace: 'secretaryActionProposals',
  delete_thread: 'secretaryActionProposals',
};

/** Secretary action interrupt types (for type guard in handlers). */
const SECRETARY_ACTION_TYPES = new Set(['delete_workspace', 'stop_workspace', 'delete_thread']);

/**
 * Message-map buckets whose entries carry an `interruptId` (rendered HITL
 * cards). `satisfies keyof AssistantMessage` makes a renamed/added bucket a
 * compile error here instead of a silently-wrong dedup rebuild.
 */
const INTERRUPT_CARD_BUCKETS = [
  'planApprovals', 'userQuestions', 'workspaceProposals',
  'questionProposals', 'ptcAgentProposals', 'secretaryActionProposals',
] as const satisfies readonly (keyof AssistantMessage)[];

export {
  PROPOSAL_INTERRUPT_TYPES, PROPOSAL_DATA_KEY_MAP, SECRETARY_ACTION_TYPES,
  INTERRUPT_CARD_BUCKETS,
};
