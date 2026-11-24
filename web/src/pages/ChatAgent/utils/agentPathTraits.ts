import type { AgentPathKind } from './agentPaths';

/** How a kind of path reads and opens outside the panel's routing. */
export interface AgentPathTraits {
  /**
   * An ordinary file to the tool rows: named by its file name, in the tool's
   * own words and category, where a store path (memory, memo, profile, skill)
   * has a phrase and a category of its own.
   */
  plainFile: boolean;
  /** The panel's Files tab opens it, so a turn's file deck may offer it. */
  panelFile: boolean;
}

/**
 * Every kind's traits, exhaustive so a new kind decides each one. A Drive
 * file is a plain file to the tool rows but opens on the Drive page, not in
 * a workspace's panel.
 */
export const AGENT_PATH_TRAITS: Record<AgentPathKind, AgentPathTraits> = {
  file: { plainFile: true, panelFile: true },
  drive: { plainFile: true, panelFile: false },
  memory: { plainFile: false, panelFile: false },
  memo: { plainFile: false, panelFile: false },
  'user-data': { plainFile: false, panelFile: false },
  skill: { plainFile: false, panelFile: false },
};
