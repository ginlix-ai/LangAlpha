import { createContext, use } from 'react';

/**
 * Gives the focus back to a comment's words, for what its composer holds
 * (the "To:" line): a chat picked there leaves the focus on the field, so the
 * words go on and Enter sends them.
 */
export const FieldFocus = createContext<(() => void) | null>(null);

export function useFocusWords(): (() => void) | null {
  return use(FieldFocus);
}
