const API_BASE_URL = 'http://localhost:8000';
const DEFAULT_USER_ID = 'test_user_001';

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

/**
 * Creates a new user
 * @param {Object} userData - User data (email, name, avatar_url, timezone, locale)
 * @param {string} userId - The user ID (default: DEFAULT_USER_ID)
 * @returns {Promise<Object>} Created user object
 */
export const createUser = async (userData, userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/users`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
      },
      body: JSON.stringify(userData),
    });

    if (!response.ok) {
      throw new Error(`Failed to create user: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error creating user:', error);
    throw error;
  }
};

/**
 * Gets the current user profile and preferences
 * @param {string} userId - The user ID (default: DEFAULT_USER_ID)
 * @returns {Promise<Object>} User profile and preferences
 */
export const getCurrentUser = async (userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/users/me`, {
      method: 'GET',
      headers: {
        'X-User-Id': userId,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get current user: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error getting current user:', error);
    throw error;
  }
};

/**
 * Gets user preferences only
 * @param {string} userId - The user ID (default: DEFAULT_USER_ID)
 * @returns {Promise<Object>} User preferences
 */
export const getPreferences = async (userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/users/me/preferences`, {
      method: 'GET',
      headers: {
        'X-User-Id': userId,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to get preferences: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error getting preferences:', error);
    throw error;
  }
};

/**
 * Updates the current user profile
 * @param {Object} userData - User data to update (email, name, avatar_url, timezone, locale, onboarding_completed)
 * @param {string} userId - The user ID (default: DEFAULT_USER_ID)
 * @returns {Promise<Object>} Updated user profile and preferences
 */
export const updateCurrentUser = async (userData, userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/users/me`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
      },
      body: JSON.stringify(userData),
    });

    if (!response.ok) {
      throw new Error(`Failed to update user: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error updating user:', error);
    throw error;
  }
};

/**
 * Updates user preferences
 * @param {Object} preferences - Preferences to update (risk_preference, investment_preference, agent_preference, other_preference)
 * @param {string} userId - The user ID (default: DEFAULT_USER_ID)
 * @returns {Promise<Object>} Updated preferences
 */
export const updatePreferences = async (preferences, userId = DEFAULT_USER_ID) => {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/users/me/preferences`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'X-User-Id': userId,
      },
      body: JSON.stringify(preferences),
    });

    if (!response.ok) {
      throw new Error(`Failed to update preferences: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error updating preferences:', error);
    throw error;
  }
};
