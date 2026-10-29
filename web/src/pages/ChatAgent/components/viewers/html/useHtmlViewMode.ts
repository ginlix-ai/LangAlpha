import { useState } from 'react';

/**
 * Whether an HTML file shows its markup rather than the rendered page. Held by
 * the panel, whose header carries the switch, and kept across files the way a
 * reader's choice of view is.
 *
 * An anchored open asks for a place in the rendered document, and Source
 * cannot show one: the tab survives both the file and the anchor changing, so
 * a reference clicked while reading markup left the reader on the same markup
 * with nothing to tell them the click had landed. The request is the seq, not
 * the anchor's text, so asking for one section twice is two requests, while
 * switching to Source with a request still in effect is left alone.
 */
export function useHtmlViewMode(anchor: string | null, anchorSeq: number | null) {
  const [source, setSource] = useState(false);
  const request = anchor ? `${anchorSeq ?? ''}\u0000${anchor}` : null;
  const [handled, setHandled] = useState(request);
  if (request !== handled) {
    setHandled(request);
    if (request) setSource(false);
  }
  return { source, setSource };
}
