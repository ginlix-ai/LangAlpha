import { getWorkspaces, createWorkspace, getFlashWorkspace } from '../../ChatAgent/utils/api';

const DEFAULT_WORKSPACE_NAME = 'LangAlpha';
const DEFAULT_WORKSPACE_DESCRIPTION = 'system default workspace, cannot be deleted';

/**
 * Finds or creates the "LangAlpha" workspace.
 * Also ensures the shared flash workspace exists (fire-and-forget).
 * @param {Function} onCreating - Optional callback when workspace creation starts
 * @param {Function} onCreated - Optional callback when workspace creation completes
 * @returns {Promise<string>} The workspace ID
 */
export async function findOrCreateDefaultWorkspace(onCreating = null, onCreated = null) {
  // Ensure flash workspace exists (fire-and-forget, non-blocking)
  getFlashWorkspace().catch((err) => {
    console.warn('[workspace] Failed to ensure flash workspace:', err);
  });

  const { workspaces } = await getWorkspaces();

  // Look for "LangAlpha" workspace
  const defaultWorkspace = workspaces?.find(
    (ws) => ws.name === DEFAULT_WORKSPACE_NAME
  );

  if (defaultWorkspace) {
    return defaultWorkspace.workspace_id;
  }

  // If not found, create it
  if (onCreating) {
    onCreating();
  }

  try {
    const newWorkspace = await createWorkspace(
      DEFAULT_WORKSPACE_NAME,
      DEFAULT_WORKSPACE_DESCRIPTION,
      {}
    );

    if (onCreated) {
      onCreated();
    }

    return newWorkspace.workspace_id;
  } catch (error) {
    if (onCreated) {
      onCreated();
    }
    throw error;
  }
}

export { DEFAULT_WORKSPACE_NAME, DEFAULT_WORKSPACE_DESCRIPTION };
