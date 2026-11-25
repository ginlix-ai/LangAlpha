/**
 * Which thread holds each of a fork's borrowed turns, read off the divider
 * its replay sent.
 */
import type { ForkDividerEvent, Thread } from '@/types/api';

/** The thread that holds a borrowed turn: the segment it falls in, else the source. */
export function borrowedTurnOwner(divider: ForkDividerEvent, turn: number): string {
  const segment = divider.segments.find((seg) => seg.first_turn <= turn && turn <= seg.through_turn);
  return segment?.thread_id ?? divider.source_thread_id;
}

/** Whether `thread` is a fork that shows turns any of `threadIds` holds. */
export function borrowsFrom(thread: Thread | undefined, threadIds: ReadonlySet<string>): boolean {
  const ancestors = thread?.metadata?.fork?.borrow?.ancestors;
  return !!ancestors?.some((ancestor) => threadIds.has(ancestor.thread_id));
}
