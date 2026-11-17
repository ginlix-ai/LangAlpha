/**
 * Where a segment a frame adds sorts in its reply: at the frame's own id,
 * which the live stream and history replay both carry, so a frame delivered
 * again lands on the same order and dedups; else at the next of the lane's
 * own counter, which `next` advances only when it is needed.
 */
export const eventOrder = (event: { _eventId?: number | string }, next: () => number): number =>
  (event._eventId != null ? Number(event._eventId) : next());
