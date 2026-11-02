import type { ArtifactReading, UserPreferences } from '../types/api';
import { otherPreferencePatch, readOtherPreference } from './otherPreference';

/** Which thread an unbound file's comments go to: the chat beside the file,
 *  the thread whose agent made it, or a new thread. The server reads the
 *  same key to answer a file's default destination. */
export type CommentDestination = 'beside' | 'creator' | 'new';
/** What Enter does in the comment box; the other route stays a click away. */
export type CommentEnter = 'queue' | 'send';
/** Where the person would rather have the agent's full answer to comments:
 *  wherever they read at send time, the chat, or under the comment. A
 *  preference: what they say when they send comes first. */
export type ArtifactAnswers = 'auto' | 'chat' | 'file';

const DESTINATION_KEY = 'artifact_comment_destination';
const ENTER_KEY = 'artifact_comment_enter';
const ANSWERS_KEY = 'artifact_answers';

/** `other_preference.artifact_comment_destination`; anything else reads as 'beside'. */
export function readCommentDestination(prefs: UserPreferences | null | undefined): CommentDestination {
  const value = readOtherPreference(prefs, DESTINATION_KEY);
  return value === 'creator' || value === 'new' ? value : 'beside';
}

/** `other_preference.artifact_comment_enter`; anything but 'send' reads as 'queue'. */
export function readCommentEnter(prefs: UserPreferences | null | undefined): CommentEnter {
  return readOtherPreference(prefs, ENTER_KEY) === 'send' ? 'send' : 'queue';
}

/** `other_preference.artifact_answers`; anything else reads as 'auto'. */
export function readArtifactAnswers(prefs: UserPreferences | null | undefined): ArtifactAnswers {
  const value = readOtherPreference(prefs, ANSWERS_KEY);
  return value === 'chat' || value === 'file' ? value : 'auto';
}

export function commentDestinationPatch(next: CommentDestination): Partial<UserPreferences> {
  return otherPreferencePatch(DESTINATION_KEY, next);
}
export function commentEnterPatch(next: CommentEnter): Partial<UserPreferences> {
  return otherPreferencePatch(ENTER_KEY, next);
}
export function artifactAnswersPatch(next: ArtifactAnswers): Partial<UserPreferences> {
  return otherPreferencePatch(ANSWERS_KEY, next);
}

/**
 * The `reading` a send of comments carries. Every send path resolves it here,
 * so the chat composer, the docked one, Send now and Send all agree: `auto`
 * goes by where the person reads at send time (the file when no chat is in
 * view, `covered`), the others override it.
 */
export function readingFor(answers: ArtifactAnswers, covered: boolean): ArtifactReading {
  if (answers !== 'auto') return answers;
  return covered ? 'file' : 'chat';
}
