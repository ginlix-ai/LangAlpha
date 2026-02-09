const KEY = 'chat_session_restore';
const TTL_MS = 5 * 60 * 1000; // 5 minutes

export function saveChatSession({ workspaceId, threadId, scrollTop }) {
  if (!workspaceId || !threadId || threadId === '__default__') return;
  sessionStorage.setItem(KEY, JSON.stringify({
    workspaceId, threadId, scrollTop: scrollTop || 0, ts: Date.now(),
  }));
}

export function getChatSession() {
  const raw = sessionStorage.getItem(KEY);
  if (!raw) return null;
  const session = JSON.parse(raw);
  if (Date.now() - session.ts > TTL_MS) {
    sessionStorage.removeItem(KEY);
    return null;
  }
  return session;
}

export function clearChatSession() {
  sessionStorage.removeItem(KEY);
}
