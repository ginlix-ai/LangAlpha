/**
 * Tiny pub/sub for widget context attachments. Decouples the dashboard
 * widgets that produce snapshots from the chat inputs that render them.
 *
 * Why a hand-rolled bus over a React context: chat inputs and overflow pill
 * may live in different React trees (modal, portal, hero card vs in-grid
 * widget) and need to subscribe to the *same* state without lifting an
 * ancestor. A module-scoped event emitter is the smallest unit that works
 * for cross-tree state without dragging in zustand.
 */

import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

export type ContextBusEvent =
  | { type: 'attach'; snapshot: WidgetContextSnapshot }
  | { type: 'detach'; widgetId: string }
  | { type: 'clear' };

export type ContextBusHandler = (event: ContextBusEvent) => void;

const handlers = new Set<ContextBusHandler>();

function publish(event: ContextBusEvent): void {
  // Snapshot the set so a handler that unsubscribes during dispatch doesn't
  // mutate the iterator. Errors in one handler must not silence the others —
  // log and continue.
  for (const h of [...handlers]) {
    try {
      h(event);
    } catch (err) {
      console.error('[contextBus] subscriber threw', err);
    }
  }
}

export const ContextBus = {
  subscribe(handler: ContextBusHandler): () => void {
    handlers.add(handler);
    return () => {
      handlers.delete(handler);
    };
  },
  attach(snapshot: WidgetContextSnapshot): void {
    publish({ type: 'attach', snapshot });
  },
  detach(widgetId: string): void {
    publish({ type: 'detach', widgetId });
  },
  clear(): void {
    publish({ type: 'clear' });
  },
  /** Test-only: drop every subscriber. Never call from product code. */
  __resetForTests(): void {
    handlers.clear();
  },
};

/**
 * Registry of mounted chat input root elements so the overflow pill can
 * decide whether *any* chat input is visible via IntersectionObserver.
 * Module-scoped because the pill renders at the app shell while inputs
 * mount deeper in the tree.
 */
const chatInputElements = new Set<HTMLElement>();

export const ChatInputRegistry = {
  register(el: HTMLElement): () => void {
    chatInputElements.add(el);
    notifyChatInputListeners();
    return () => {
      chatInputElements.delete(el);
      notifyChatInputListeners();
    };
  },
  list(): HTMLElement[] {
    return [...chatInputElements];
  },
  size(): number {
    return chatInputElements.size;
  },
  subscribe(listener: () => void): () => void {
    chatInputListeners.add(listener);
    return () => {
      chatInputListeners.delete(listener);
    };
  },
  /** Test-only. */
  __resetForTests(): void {
    chatInputElements.clear();
    chatInputListeners.clear();
  },
};

const chatInputListeners = new Set<() => void>();

function notifyChatInputListeners(): void {
  for (const l of [...chatInputListeners]) {
    try {
      l();
    } catch (err) {
      console.error('[chatInputRegistry] listener threw', err);
    }
  }
}
