const API_BASE_URL = 'http://localhost:8080';

/**
 * Fetches data from the backend hello endpoint
 * @returns {Promise<string>} The response string from the backend
 */
export const fetchHello = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/hello`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.text();
    return data;
  } catch (error) {
    console.error('Error fetching hello:', error);
    throw error;
  }
};
