import { getWorkspaces, createWorkspace, DEFAULT_USER_ID } from '../../ChatAgent/utils/api';

const DEFAULT_WORKSPACE_NAME = 'Stealth Agent';
const DEFAULT_WORKSPACE_DESCRIPTION = 'system default workspace, cannot be deleted';

/**
 * Finds or creates the "Stealth Agent" workspace
 * @param {Function} onCreating - Optional callback when workspace creation starts
 * @param {Function} onCreated - Optional callback when workspace creation completes
 * @returns {Promise<string>} The workspace ID
 */
export async function findOrCreateDefaultWorkspace(onCreating = null, onCreated = null) {
  // Fetch user's workspaces
  const { workspaces } = await getWorkspaces(DEFAULT_USER_ID);
  
  // Look for "Stealth Agent" workspace
  const stealthAgentWorkspace = workspaces?.find(
    (ws) => ws.name === DEFAULT_WORKSPACE_NAME
  );
  
  if (stealthAgentWorkspace) {
    return stealthAgentWorkspace.workspace_id;
  }
  
  // If not found, create it
  if (onCreating) {
    onCreating();
  }
  
  try {
    const newWorkspace = await createWorkspace(
      DEFAULT_WORKSPACE_NAME,
      DEFAULT_WORKSPACE_DESCRIPTION
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
