const API_BASE_URL = 'http://localhost:8080';

/**
 * Sends a chat message to the agent API
 * @param {string} message - The user's message
 * @param {boolean} planMode - Whether to use plan mode (default: false)
 * @returns {Promise<Object>} The response from the backend
 */
export const sendChatMessage = async (message, planMode = false) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: message,
        plan_mode: planMode,
      }),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error sending chat message:', error);
    throw error;
  }
};
