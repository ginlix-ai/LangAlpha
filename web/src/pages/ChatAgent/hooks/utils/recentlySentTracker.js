/**
 * Recently sent messages tracker
 * Tracks recently sent messages to avoid duplicates when loading history
 */

const RETENTION_TIME_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Creates a tracker for recently sent messages
 * @returns {Object} Tracker object with methods
 */
export function createRecentlySentTracker() {
  const messages = new Map();

  /**
   * Tracks a recently sent message
   * @param {string} content - Message content (trimmed)
   * @param {Date} timestamp - Message timestamp
   * @param {string} id - Message ID
   */
  function track(content, timestamp, id) {
    const messageKey = `${content}-${Date.now()}`;
    messages.set(messageKey, {
      content: content.trim(),
      timestamp,
      id,
    });
    cleanup();
  }

  /**
   * Checks if a message content was recently sent
   * @param {string} content - Message content to check
   * @returns {boolean} True if message was recently sent
   */
  function isRecentlySent(content) {
    cleanup();
    return Array.from(messages.values()).some(
      (msg) => msg.content === content.trim()
    );
  }

  /**
   * Clears all tracked messages
   */
  function clear() {
    messages.clear();
  }

  /**
   * Removes old entries (older than retention time)
   */
  function cleanup() {
    const cutoffTime = Date.now() - RETENTION_TIME_MS;
    for (const [key, msg] of messages.entries()) {
      if (msg.timestamp.getTime() < cutoffTime) {
        messages.delete(key);
      }
    }
  }

  return {
    track,
    isRecentlySent,
    clear,
  };
}
