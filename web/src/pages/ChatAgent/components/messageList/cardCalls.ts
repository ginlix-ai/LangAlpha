import { createContext } from 'react';

/**
 * The tool calls whose card the message draws, by id. A live row naming one
 * of them is leaving because its call has landed as that card, which takes
 * the row's place in the same commit (see CardArrivals), so the row hands
 * over its space and its look rather than folding beside the card.
 */
export const CardCallsContext = createContext<ReadonlySet<string>>(new Set());
