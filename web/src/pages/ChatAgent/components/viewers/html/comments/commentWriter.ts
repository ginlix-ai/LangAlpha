import type { TFunction } from 'i18next';
import type { ArtifactCommentMessage } from '@/types/api';
import { personLabel } from '@/lib/sharedThreads';

/** Who a person's message on a comment reads as written by: "You" for the
 *  viewer's own, and in a shared workspace the member who wrote it, so one
 *  member's words never read as another's. */
export function writerName(t: TFunction, message: ArtifactCommentMessage | undefined, me: string | null): string {
  const mine = !message?.written_by || message.written_by === me;
  return personLabel(t, { name: message?.written_by_name ?? null, mine });
}
