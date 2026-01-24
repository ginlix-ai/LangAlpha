const API_BASE_URL = 'http://localhost:8000';
const DEFAULT_USER_ID = 'test_user_001';

/**
 * Gets all workspaces for a user
 * @param {string} userId - The user ID
 * @param {number} limit - Maximum number of workspaces to return
 * @param {number} offset - Offset for pagination
 * @returns {Promise<Object>} Workspaces response with workspaces array and metadata
 */
export const getWorkspaces = async (userId = DEFAULT_USER_ID, limit = 20, offset = 0) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/workspaces?limit=${limit}&offset=${offset}`, {
      method: 'GET',
      headers: {
        'X-User-Id': userId,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get workspaces: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error getting workspaces:', error);
    throw error;
  }
};

/**
 * Creates a new workspace
 * @param {string} name - Workspace name
 * @param {string} description - Workspace description (optional)
 * @param {Object} config - Workspace configuration (optional)
 * @param {string} userId - The user ID
 * @returns {Promise<Object>} Created workspace object
 */
export const createWorkspace = async (name, description = '', config = {}, userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/workspaces`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
      },
      body: JSON.stringify({
        name,
        description,
        config,
      }),
    });

    if (!response.ok) {
      throw new Error(`Failed to create workspace: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error creating workspace:', error);
    throw error;
  }
};

/**
 * Deletes a workspace
 * @param {string} workspaceId - The workspace ID to delete
 * @returns {Promise<void>}
 */
export const deleteWorkspace = async (workspaceId) => {
  try {
    if (!workspaceId) {
      throw new Error('Workspace ID is required');
    }

    const workspaceIdStr = String(workspaceId).trim();
    if (!workspaceIdStr) {
      throw new Error('Workspace ID cannot be empty');
    }

    const url = `${API_BASE_URL}/api/v1/workspaces/${workspaceIdStr}`;
    console.log('DELETE request URL:', url);

    const response = await fetch(url, {
      method: 'DELETE',
    });

    if (!response.ok) {
      // Try to get error message from response body
      let errorMessage = `Failed to delete workspace: ${response.status}`;
      try {
        const errorData = await response.json();
        if (errorData.detail) {
          errorMessage = errorData.detail;
        }
      } catch (e) {
        // If response is not JSON, try text
        try {
          const errorText = await response.text();
          if (errorText) {
            errorMessage = errorText;
          }
        } catch (textError) {
          // Ignore text parsing errors
        }
      }
      throw new Error(errorMessage);
    }

    // DELETE endpoint returns 204 No Content, so no body to parse
  } catch (error) {
    console.error('Error deleting workspace:', error);
    console.error('Workspace ID:', workspaceId);
    
    // Provide more helpful error messages for common issues
    if (error.message.includes('CORS') || error.message.includes('Failed to fetch')) {
      throw new Error('CORS error: Please ensure the backend server allows DELETE requests from this origin.');
    }
    
    throw error;
  }
};

/**
 * Creates or gets a default workspace for the user
 * @param {string} userId - The user ID
 * @returns {Promise<string>} The workspace ID
 */
export const getOrCreateWorkspace = async (userId = DEFAULT_USER_ID) => {
  try {
    // Try to list workspaces first
    const data = await getWorkspaces(userId, 1, 0);
    if (data.workspaces && data.workspaces.length > 0) {
      return data.workspaces[0].workspace_id;
    }

    // Create a new workspace if none exists
    const workspace = await createWorkspace('Default Workspace', 'Default workspace for chat', {}, userId);
    return workspace.workspace_id;
  } catch (error) {
    console.error('Error getting/creating workspace:', error);
    throw error;
  }
};

/**
 * Sends a chat message via SSE streaming
 * @param {string} message - The user's message
 * @param {string} workspaceId - The workspace ID
 * @param {string} threadId - The thread ID (optional)
 * @param {Array} messageHistory - Previous messages for context
 * @param {boolean} planMode - Whether to use plan mode (default: false)
 * @param {Function} onEvent - Callback for SSE events
 * @returns {Promise<void>}
 */
export const sendChatMessageStream = async (
  message,
  workspaceId,
  threadId = '__default__',
  messageHistory = [],
  planMode = false,
  onEvent = () => {}
) => {
  try {
    const messages = [
      ...messageHistory,
      {
        role: 'user',
        content: message,
      },
    ];

    const response = await fetch(`${API_BASE_URL}/api/v1/chat/stream`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        workspace_id: workspaceId,
        thread_id: threadId,
        user_id: 'test_user_001',
        messages: messages,
        plan_mode: planMode,
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let currentEvent = {};

    const processSSELine = (line) => {
      if (line.startsWith('id: ')) {
        currentEvent.id = line.slice(4).trim();
      } else if (line.startsWith('event: ')) {
        currentEvent.event = line.slice(7).trim();
      } else if (line.startsWith('data: ')) {
        try {
          const data = JSON.parse(line.slice(6));
          // Merge event type from the event line into the data
          if (currentEvent.event) {
            data.event = currentEvent.event;
          }
          onEvent(data);
          // Reset for next event
          currentEvent = {};
        } catch (e) {
          console.warn('Failed to parse SSE data:', e, line);
        }
      } else if (line.trim() === '') {
        // Empty line indicates end of event, reset
        currentEvent = {};
      }
    };

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || ''; // Keep incomplete line in buffer

      for (const line of lines) {
        processSSELine(line);
      }
    }

    // Process any remaining buffer
    if (buffer.trim()) {
      const lines = buffer.split('\n');
      for (const line of lines) {
        processSSELine(line);
      }
    }
  } catch (error) {
    console.error('Error streaming chat message:', error);
    throw error;
  }
};
