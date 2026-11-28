import { createContext } from 'react';

/** Set by the status line: a running row there marks itself with the amber
 *  loader glyph (DESIGN.md: live work is the glyph plus text) in place of its
 *  pulsing tool icon, which comes back when the step settles. */
export const RunningGlyphContext = createContext(false);

/** Set by the status line: how many other steps are in flight beside the one
 *  it names, which its row shows after its label (`MoreInFlight`). */
export const MoreInFlightContext = createContext(0);
