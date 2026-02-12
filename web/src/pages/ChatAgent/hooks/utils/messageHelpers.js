/**
 * Message creation and manipulation utilities
 * Provides helper functions for creating and updating message objects
 */

/**
 * Creates a user message object
 * @param {string} message - The message content
 * @param {Array|null} attachments - Optional attachment metadata for display
 * @returns {Object} User message object
 */
export function createUserMessage(message, attachments = null) {
  const msg = {
    id: `user-${Date.now()}`,
    role: 'user',
    content: message,
    contentType: 'text',
    timestamp: new Date(),
  };
  if (attachments && attachments.length > 0) {
    msg.attachments = attachments;
  }
  return msg;
}

/**
 * Creates an assistant message placeholder
 * @param {string} messageId - Optional custom message ID (defaults to timestamp-based)
 * @returns {Object} Assistant message object
 */
export function createAssistantMessage(messageId = null) {
  const id = messageId || `assistant-${Date.now()}`;
  return {
    id,
    role: 'assistant',
    content: '',
    contentType: 'text',
    timestamp: new Date(),
    isStreaming: true,
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
    todoListProcesses: {},
  };
}

/**
 * Creates a history user message object
 * @param {string} pairIndex - The pair index from history
 * @param {string} content - The message content
 * @param {Date|string} timestamp - Optional timestamp
 * @returns {Object} History user message object
 */
export function createHistoryUserMessage(pairIndex, content, timestamp = null) {
  return {
    id: `history-user-${pairIndex}-${Date.now()}`,
    role: 'user',
    content,
    contentType: 'text',
    timestamp: timestamp ? new Date(timestamp) : new Date(),
    isHistory: true,
  };
}

/**
 * Creates a history assistant message placeholder
 * @param {string} pairIndex - The pair index from history
 * @param {Date|string} timestamp - Optional timestamp
 * @returns {Object} History assistant message object
 */
export function createHistoryAssistantMessage(pairIndex, timestamp = null) {
  return {
    id: `history-assistant-${pairIndex}-${Date.now()}`,
    role: 'assistant',
    content: '',
    contentType: 'text',
    timestamp: timestamp ? new Date(timestamp) : new Date(),
    isStreaming: false,
    isHistory: true,
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
    todoListProcesses: {},
  };
}

/**
 * Updates a specific message in the messages array
 * @param {Array} messages - Current messages array
 * @param {string} messageId - ID of the message to update
 * @param {Function} updater - Function that receives the message and returns updated message
 * @returns {Array} New messages array with updated message
 */
export function updateMessage(messages, messageId, updater) {
  return messages.map((msg) => {
    if (msg.id !== messageId) return msg;
    return updater(msg);
  });
}

/**
 * Inserts a message at a specific index in the messages array
 * @param {Array} messages - Current messages array
 * @param {number} insertIndex - Index to insert at
 * @param {Object} newMessage - Message object to insert
 * @returns {Array} New messages array with inserted message
 */
export function insertMessage(messages, insertIndex, newMessage) {
  return [
    ...messages.slice(0, insertIndex),
    newMessage,
    ...messages.slice(insertIndex),
  ];
}

/**
 * Appends a message to the end of the messages array
 * @param {Array} messages - Current messages array
 * @param {Object} newMessage - Message object to append
 * @returns {Array} New messages array with appended message
 */
export function appendMessage(messages, newMessage) {
  return [...messages, newMessage];
}
