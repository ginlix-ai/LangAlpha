/**
 * The server's refusals, read back into sentences a person can act on. Each
 * case pins one code and the values its sentence takes, so a renamed code or
 * parameter on either side shows up here rather than as the raw message.
 */
import { describe, expect, it } from 'vitest';

import { ApiError } from '@/api/client';
import i18n from '@/i18n';

import { describeProblem, problemFromError, wantsExample } from '../skillUploadProblems';

const t = i18n.getFixedT('en-US');

function refused(status: number, detail: unknown): ApiError {
  return new ApiError(
    `Request failed with status code ${status}`,
    status >= 500 ? 'ERR_BAD_RESPONSE' : 'ERR_BAD_REQUEST',
    { data: { detail }, status, statusText: '', headers: {} },
  );
}

describe('problemFromError', () => {
  it.each([
    [{ code: 'no_frontmatter' }, { kind: 'noFrontmatter' }],
    [{ code: 'bad_frontmatter' }, { kind: 'badFrontmatter' }],
    [{ code: 'no_name' }, { kind: 'noName' }],
    [{ code: 'no_description' }, { kind: 'noDescription' }],
    [{ code: 'bad_name' }, { kind: 'badName' }],
    [{ code: 'name_too_long', max: 64 }, { kind: 'nameTooLong', max: 64 }],
    [{ code: 'yaml_anchors' }, { kind: 'yamlAnchors' }],
    [{ code: 'not_utf8' }, { kind: 'notUtf8' }],
    [{ code: 'trigger_reserved', trigger: 'pdf' }, { kind: 'reserved', name: 'pdf' }],
    [{ code: 'trigger_taken', trigger: 'recap' }, { kind: 'triggerTaken', name: 'recap' }],
    [
      { code: 'trigger_is_command', trigger: 'recap', skill: 'earnings-recap' },
      { kind: 'commandOf', name: 'recap', other: 'earnings-recap' },
    ],
    [{ code: 'layout' }, { kind: 'layout' }],
    [{ code: 'empty' }, { kind: 'empty' }],
    [{ code: 'not_zip' }, { kind: 'notZip' }],
    [{ code: 'too_many_files', count: 80, max: 64 }, { kind: 'tooManyFiles', count: 80, max: 64 }],
    [
      { code: 'file_too_large', path: 'data/prices.csv', max: 1048576 },
      { kind: 'fileTooLarge', file: 'data/prices.csv', max: 1048576 },
    ],
    [{ code: 'skill_md_too_large', max: 262144 }, { kind: 'skillMdTooLarge', max: 262144 }],
    [{ code: 'description_too_long', max: 1024 }, { kind: 'descriptionTooLong', max: 1024 }],
    [{ code: 'unpacked_too_large', max: 8388608 }, { kind: 'unpackedTooLarge', max: 8388608 }],
    [
      { code: 'upload_too_large', size: 3145728, max: 2097152 },
      { kind: 'uploadTooLarge', size: 3145728, max: 2097152 },
    ],
    [
      { code: 'upload_too_large', size: null, max: 2097152 },
      { kind: 'uploadTooLarge', size: null, max: 2097152 },
    ],
    [{ code: 'skill_count_cap', max: 200 }, { kind: 'countCap', max: 200 }],
    [{ code: 'skill_storage_cap', max: 33554432 }, { kind: 'storageCap', max: 33554432 }],
    [{ code: 'unsafe_path', path: 'run;rm.sh' }, { kind: 'unsafePath', path: 'run;rm.sh' }],
    [{ code: 'not_regular', path: 'link' }, { kind: 'notRegular', path: 'link' }],
    [{ code: 'file_and_directory', path: 'demo/x' }, { kind: 'fileAndDirectory', path: 'demo/x' }],
  ])('%o', (detail, problem) => {
    expect(problemFromError(refused(400, { message: 'server words', ...detail }))).toEqual(problem);
  });

  it('keeps a refusal it has no code for as the server wrote it', () => {
    expect(problemFromError(refused(400, { code: 'something_new', message: 'something new' }))).toEqual({
      kind: 'server',
      text: 'something new',
    });
    expect(problemFromError(refused(400, 'plain words'))).toEqual({ kind: 'server', text: 'plain words' });
  });

  it('does not read a code off the object prototype', () => {
    expect(problemFromError(refused(400, { code: 'toString', message: 'odd' }))).toEqual({
      kind: 'server',
      text: 'odd',
    });
  });

  it('reads an uncoded 502 or 503 as the server being busy, not the upload being wrong', () => {
    expect(problemFromError(refused(503, 'Could not load reserved names'))).toEqual({ kind: 'busy' });
    expect(problemFromError(refused(502, 'Failed to store archive at /var/x'))).toEqual({ kind: 'busy' });
  });

  it('reads a request that never got an answer as the network', () => {
    expect(problemFromError(new ApiError('Network Error', 'ERR_NETWORK'))).toEqual({ kind: 'network' });
    expect(problemFromError(new ApiError('timeout of 30000ms exceeded', 'ECONNABORTED'))).toEqual({
      kind: 'network',
    });
  });
});

describe('describeProblem', () => {
  it('formats sizes in the reader’s locale', () => {
    const text = describeProblem({ kind: 'uploadTooLarge', size: 3 * 1024 * 1024, max: 2 * 1024 * 1024 }, t, 'en-US');
    expect(text).toMatch(/3(\.0)? MB/);
    expect(text).toMatch(/2(\.0)? MB/);
  });

  it('shows an unknown refusal verbatim', () => {
    expect(describeProblem({ kind: 'server', text: 'raw words' }, t, 'en-US')).toBe('raw words');
  });

  it('has a sentence for every kind in both languages', () => {
    const kinds = [
      { kind: 'tooManyFiles', count: 80, max: 64 },
      { kind: 'folderTooBig' },
      { kind: 'reserved', name: 'pdf' },
      { kind: 'commandOf', name: 'recap', other: 'earnings-recap' },
      { kind: 'noFrontmatter' },
      { kind: 'fileAndDirectory', path: 'demo/x' },
      { kind: 'busy' },
    ] as const;
    for (const lng of ['en-US', 'zh-CN']) {
      const fixed = i18n.getFixedT(lng);
      for (const p of kinds) {
        const text = describeProblem(p, fixed, lng);
        expect(text).not.toMatch(/plugins\.skillUpload/);
        expect(text).not.toMatch(/\{\{/);
      }
    }
  });

  it('answers only header problems with an example header', () => {
    expect(wantsExample({ kind: 'noName' })).toBe(true);
    expect(wantsExample({ kind: 'badFrontmatter' })).toBe(true);
    expect(wantsExample({ kind: 'layout' })).toBe(false);
  });
});
