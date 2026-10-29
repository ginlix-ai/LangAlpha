import { createContext, useContext } from 'react';
import type { UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** Both displays fold finished work behind a summary row. `lean` is the
 *  default; `verbose` also expands reasoning while it streams. */
export type TurnDisplay = 'verbose' | 'lean';
/** How reply text lands while streaming: per token, or held to paragraph
 *  boundaries so the reader never watches a sentence being typed. */
export type StreamingMode = 'token' | 'paragraph';
/** How a running turn shows its tool calls: a row per call (`rows`), or one
 *  line naming the current step under the counts of finished ones (`status`,
 *  being tried out). */
export type ToolCallLayout = 'rows' | 'status';

const TURN_DISPLAY_KEY = 'turn_display';
const STREAMING_MODE_KEY = 'response_streaming_mode';
const TOOL_CALL_LAYOUT_KEY = 'tool_call_layout';

/** `other_preference.turn_display`; anything but 'verbose' reads as 'lean'. */
export function readTurnDisplay(prefs: UserPreferences | null | undefined): TurnDisplay {
  return readOtherPreference(prefs, TURN_DISPLAY_KEY) === 'verbose' ? 'verbose' : 'lean';
}

/** `other_preference.response_streaming_mode`; anything but 'paragraph' reads as 'token'. */
export function readStreamingMode(prefs: UserPreferences | null | undefined): StreamingMode {
  return readOtherPreference(prefs, STREAMING_MODE_KEY) === 'paragraph' ? 'paragraph' : 'token';
}

/** `other_preference.tool_call_layout`; anything but 'status' reads as 'rows'. */
export function readToolCallLayout(prefs: UserPreferences | null | undefined): ToolCallLayout {
  return readOtherPreference(prefs, TOOL_CALL_LAYOUT_KEY) === 'status' ? 'status' : 'rows';
}

export function turnDisplayPatch(next: TurnDisplay): Partial<UserPreferences> {
  return otherPreferencePatch(TURN_DISPLAY_KEY, next);
}
export function streamingModePatch(next: StreamingMode): Partial<UserPreferences> {
  return otherPreferencePatch(STREAMING_MODE_KEY, next);
}
export function toolCallLayoutPatch(next: ToolCallLayout): Partial<UserPreferences> {
  return otherPreferencePatch(TOOL_CALL_LAYOUT_KEY, next);
}

export interface TranscriptDisplay {
  turnDisplay: TurnDisplay;
  streamingMode: StreamingMode;
  /** Optional so a host that predates it (and its fixtures) reads as 'rows'. */
  toolCallLayout?: ToolCallLayout;
}

export const DEFAULT_TRANSCRIPT_DISPLAY: TranscriptDisplay = { turnDisplay: 'lean', streamingMode: 'token', toolCallLayout: 'rows' };

/** Hosts that never mount the provider (shared chat, market view) get the
 *  lean, token-streamed transcript with a row per tool call. */
export const TranscriptDisplayContext = createContext<TranscriptDisplay>(DEFAULT_TRANSCRIPT_DISPLAY);

export function useTranscriptDisplay(): TranscriptDisplay {
  return useContext(TranscriptDisplayContext);
}
