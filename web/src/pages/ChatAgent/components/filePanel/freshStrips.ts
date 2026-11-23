import { registerAuthReset } from '@/lib/authResets';
import { userLocalStorage } from '@/lib/userStorage';

const stripPrefix = (workspaceId: string) => `filePanel:tabs.${workspaceId}`;

/**
 * A fresh chat's strip in a workspace, before it has a thread. Never in
 * storage: earlier builds seeded new threads from a strip kept under the bare
 * workspace key, older browsers may still hold one, and nothing reads it now.
 * Given the chat, the strip is held in memory under it (`freshStrips`).
 */
export function tabsStorageKey(workspaceId: string, chat?: string): string {
  return chat ? `${stripPrefix(workspaceId)}.new.${chat}` : stripPrefix(workspaceId);
}

/** A thread's own strip. */
export function threadTabsStorageKey(workspaceId: string, threadId: string): string {
  return `${stripPrefix(workspaceId)}.t.${threadId}`;
}

/**
 * Fresh chats' tab strips, serialized, by `tabsStorageKey(ws, chat)`. Held by
 * the chat rather than the panel, since the panel unmounts when it closes or
 * when the window crosses into the phone layout, and kept out of storage so no
 * other chat can pick one up. Handed to the chat's thread once it has one
 * (`handChatStrips`), dropped with the chat (`releaseChatStrips`) and with the
 * account. Its own module so the chat can do both without loading the panel.
 */
export const freshStrips = new Map<string, string>();
registerAuthReset(() => freshStrips.clear());

const heldBy = (chat: string) => `.new.${chat}`;

/**
 * Save what a chat held to the thread its first message minted, for a panel
 * that was closed when that happened: the chat can be dropped before the panel
 * opens again. A thread that already has a strip keeps it. The held copy goes
 * once the thread's reads back, as the panel's own hand-off does.
 */
export function handChatStrips(chat: string, threadId: string): void {
  const own = heldBy(chat);
  for (const [key, json] of freshStrips) {
    if (!key.endsWith(own)) continue;
    const target = `${key.slice(0, -own.length)}.t.${threadId}`;
    if (userLocalStorage.getItem(target) == null) userLocalStorage.setItem(target, json);
    if (userLocalStorage.getItem(target) != null) freshStrips.delete(key);
  }
}

/** Drop what a chat held before its thread, as the chat goes. */
export function releaseChatStrips(chat: string): void {
  const own = heldBy(chat);
  for (const key of freshStrips.keys()) if (key.endsWith(own)) freshStrips.delete(key);
}
