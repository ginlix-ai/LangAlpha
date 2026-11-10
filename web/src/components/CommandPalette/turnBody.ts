import { useLayoutEffect } from 'react';

/** One side of a turn in the preview pane: the plain fallback, and the Markdown body that replaces it. */
export interface TurnBodyProps {
  /** The side's text, Markdown as written. */
  text: string;
  /** The words searched for, marked where they appear. */
  words: readonly string[];
  /** Called once the text is on screen, to bring its first match into view. */
  onShown: () => void;
}

/**
 * Calls `onShown` before paint whenever a body draws other text or other
 * marks: on its mount (the fallback first, then the Markdown, which moves
 * the marks) and on a new turn or query. Keyed on the words' text, so a
 * render of the palette that keeps them leaves the scroll where the reader
 * put it.
 */
export function useShown({ text, words, onShown }: TurnBodyProps): void {
  const wordsKey = words.join(' ');
  useLayoutEffect(() => onShown(), [text, wordsKey, onShown]);
}
