// @vitest-environment node
/**
 * Each artifact setting reads as today's behavior unless it holds one of its
 * other known values, so a missing key, a stray value or a malformed bag
 * never changes where a comment goes or how it is answered.
 */
import { describe, it, expect } from 'vitest';
import type { UserPreferences } from '@/types/api';
import {
  artifactAnswersPatch,
  commentDestinationPatch,
  commentEnterPatch,
  readArtifactAnswers,
  readCommentDestination,
  readCommentEnter,
  readingFor,
} from '../artifactPreferences';

const other = (bag: unknown) => ({ other_preference: bag }) as UserPreferences;
const GARBAGE: unknown[] = [undefined, null, '', 'CHAT', 'bound', 'auto', 'beside', 'queue', 1, true, ['new'], { v: 'new' }];

describe('readCommentDestination', () => {
  it('reads the two other choices', () => {
    expect(readCommentDestination(other({ artifact_comment_destination: 'creator' }))).toBe('creator');
    expect(readCommentDestination(other({ artifact_comment_destination: 'new' }))).toBe('new');
    expect(readCommentDestination(other({ artifact_comment_destination: 'beside' }))).toBe('beside');
  });

  it('is beside for anything else, and with no usable bag', () => {
    for (const value of GARBAGE.filter((v) => v !== 'beside')) {
      expect(readCommentDestination(other({ artifact_comment_destination: value }))).toBe('beside');
    }
    expect(readCommentDestination(null)).toBe('beside');
    expect(readCommentDestination({})).toBe('beside');
    expect(readCommentDestination(other(['new']))).toBe('beside');
  });
});

describe('readCommentEnter', () => {
  it('is send only for send', () => {
    expect(readCommentEnter(other({ artifact_comment_enter: 'send' }))).toBe('send');
    for (const value of GARBAGE) {
      expect(readCommentEnter(other({ artifact_comment_enter: value }))).toBe('queue');
    }
    expect(readCommentEnter(undefined)).toBe('queue');
    expect(readCommentEnter(other('send'))).toBe('queue');
  });
});

describe('readArtifactAnswers', () => {
  it('reads chat and file, and auto for anything else', () => {
    expect(readArtifactAnswers(other({ artifact_answers: 'chat' }))).toBe('chat');
    expect(readArtifactAnswers(other({ artifact_answers: 'file' }))).toBe('file');
    for (const value of GARBAGE) {
      expect(readArtifactAnswers(other({ artifact_answers: value }))).toBe('auto');
    }
    expect(readArtifactAnswers(null)).toBe('auto');
  });
});

describe('the patches', () => {
  it('send their one key alone, so the shallow merge leaves siblings standing', () => {
    expect(commentDestinationPatch('new')).toEqual({ other_preference: { artifact_comment_destination: 'new' } });
    expect(commentEnterPatch('send')).toEqual({ other_preference: { artifact_comment_enter: 'send' } });
    expect(artifactAnswersPatch('file')).toEqual({ other_preference: { artifact_answers: 'file' } });
  });
});

describe('readingFor', () => {
  it('reads the chat on auto while it is in view, and the file once it is covered', () => {
    expect(readingFor('auto', false)).toBe('chat');
    expect(readingFor('auto', true)).toBe('file');
  });

  it('forces the chat or the file whatever is on screen', () => {
    for (const covered of [true, false]) {
      expect(readingFor('chat', covered)).toBe('chat');
      expect(readingFor('file', covered)).toBe('file');
    }
  });
});
