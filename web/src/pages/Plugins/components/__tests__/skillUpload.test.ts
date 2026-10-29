/**
 * The zip that reaches the server. Whatever this side builds holds SKILL.md at
 * its root and never the litter a file browser hides; a zip the user made goes
 * up as it is.
 */
import JSZip from 'jszip';
import { describe, expect, it } from 'vitest';

import type { SkillInfo } from '@/pages/ChatAgent/utils/api';

import type { SkillLimits } from '@/pages/ChatAgent/utils/api';

import {
  declaredName,
  findClash,
  prepareFolder,
  prepareSkillMd,
  prepareZip,
  skillRoot,
} from '../skillUpload';

const SKILL_MD = '---\nname: earnings-recap\ndescription: Summarize an earnings call\n---\n\nSteps.\n';

const LIMITS: SkillLimits = {
  max_upload_bytes: 2 * 1024 * 1024,
  max_stored_bytes: null,
  max_files: 64,
  max_file_bytes: 1024 * 1024,
  max_unpacked_bytes: 8 * 1024 * 1024,
  max_skill_md_bytes: 64 * 1024,
};

const blob = (text: string) => new Blob([text]);

async function namesIn(file: Blob): Promise<string[]> {
  const zip = await JSZip.loadAsync(file);
  return Object.values(zip.files)
    .filter((f) => !f.dir)
    .map((f) => f.name)
    .sort();
}

async function zipOf(files: Record<string, string>, name = 'skill.zip'): Promise<File> {
  const zip = new JSZip();
  for (const [path, text] of Object.entries(files)) zip.file(path, text);
  return new File([await zip.generateAsync({ type: 'blob' })], name, { type: 'application/zip' });
}

describe('declaredName', () => {
  it('reads plain, quoted and commented names', () => {
    expect(declaredName(SKILL_MD)).toBe('earnings-recap');
    expect(declaredName('---\nname: "a-b"\ndescription: x\n---\n')).toBe('a-b');
    expect(declaredName('---\nname: a-b  # mine\ndescription: x\n---\n')).toBe('a-b');
    expect(declaredName('no header')).toBeNull();
  });
});

describe('skillRoot', () => {
  it('finds SKILL.md at the root or in the one top-level folder', () => {
    expect(skillRoot(['SKILL.md', 'a/b.py'])).toBe('');
    expect(skillRoot(['x/SKILL.md', 'x/b.py'])).toBe('x/');
    expect(skillRoot(['x/SKILL.md', 'y/b.py'])).toBeNull();
    expect(skillRoot(['notes.md'])).toBeNull();
  });
});

describe('prepareFolder', () => {
  it('zips the folder’s files at the root and leaves litter and hidden files out', async () => {
    const result = await prepareFolder(
      'earnings-recap v2',
      [
        { path: 'SKILL.md', file: blob(SKILL_MD) },
        { path: 'scripts/run.py', file: blob('print(1)') },
        { path: '.DS_Store', file: blob('x') },
        { path: '.env', file: blob('SECRET=test-not-real') },
        { path: '.git/HEAD', file: blob('ref') },
        { path: 'scripts/__pycache__/run.pyc', file: blob('x') },
        { path: 'node_modules/a/index.js', file: blob('x') },
      ],
      LIMITS,
    );
    expect(result.problem).toBeUndefined();
    expect(result.skill!.name).toBe('earnings-recap');
    expect(result.skill!.fileCount).toBe(2);
    expect(result.skill!.upload.name).toBe('earnings-recap.zip');
    expect(await namesIn(result.skill!.upload)).toEqual(['SKILL.md', 'scripts/run.py']);
  });

  it('lifts a skill out of the one folder it sits in', async () => {
    const result = await prepareFolder(
      null,
      [
        { path: 'earnings-recap/SKILL.md', file: blob(SKILL_MD) },
        { path: 'earnings-recap/ref.md', file: blob('x') },
      ],
      LIMITS,
    );
    expect(await namesIn(result.skill!.upload)).toEqual(['SKILL.md', 'ref.md']);
  });

  it('refuses a folder with no SKILL.md, before zipping anything', async () => {
    const result = await prepareFolder('Downloads', [{ path: 'notes.md', file: blob('x') }], LIMITS);
    expect(result.problem).toEqual({ kind: 'folderNoSkillMd' });
  });

  it('refuses past the file cap and over the size caps', async () => {
    const many = Array.from({ length: 70 }, (_, i) => ({ path: `f${i}.txt`, file: blob('x') }));
    const tooMany = await prepareFolder('x', [{ path: 'SKILL.md', file: blob(SKILL_MD) }, ...many], LIMITS);
    expect(tooMany.problem).toEqual({ kind: 'tooManyFiles', count: 71, max: 64 });

    const truncated = await prepareFolder('x', [{ path: 'SKILL.md', file: blob(SKILL_MD) }], LIMITS, true);
    expect(truncated.problem).toEqual({ kind: 'folderTooBig' });

    const big = await prepareFolder(
      'x',
      [
        { path: 'SKILL.md', file: blob(SKILL_MD) },
        { path: 'data.bin', file: new Blob([new Uint8Array(LIMITS.max_file_bytes + 1)]) },
      ],
      LIMITS,
    );
    expect(big.problem).toEqual({ kind: 'fileTooLarge', file: 'data.bin', max: LIMITS.max_file_bytes });
  });

  it('refuses a SKILL.md with no header, as the server would', async () => {
    const result = await prepareFolder('x', [{ path: 'SKILL.md', file: blob('# Just a title\n') }], LIMITS);
    expect(result.problem).toEqual({ kind: 'noFrontmatter' });
  });

  it('leaves the size caps to the server until it has said what they are', async () => {
    const result = await prepareFolder(
      'x',
      [
        { path: 'SKILL.md', file: blob(SKILL_MD) },
        { path: 'data.bin', file: new Blob([new Uint8Array(LIMITS.max_file_bytes + 1)]) },
      ],
      undefined,
    );
    expect(result.problem).toBeUndefined();
  });
});

describe('prepareSkillMd', () => {
  it('wraps one SKILL.md in a zip, and pasted text gets the newline its header needs', async () => {
    const pasted = await prepareSkillMd('paste', null, SKILL_MD.trimEnd().split('\n').slice(0, 4).join('\n'), LIMITS);
    expect(pasted.skill!.name).toBe('earnings-recap');
    expect(await namesIn(pasted.skill!.upload)).toEqual(['SKILL.md']);
  });
});

describe('prepareZip', () => {
  it('sends a well-formed zip as it is', async () => {
    const file = await zipOf({ 'earnings-recap/SKILL.md': SKILL_MD, 'earnings-recap/a.py': 'x' });
    const result = await prepareZip(file, LIMITS);
    expect(result.skill!.upload).toBe(file);
    expect(result.skill!.fileCount).toBe(2);
  });

  it('sends a Finder zip and a folder named otherwise as they are, for the server to read', async () => {
    const finder = await zipOf({
      'earnings-recap/SKILL.md': SKILL_MD,
      'earnings-recap/a.py': 'x',
      '__MACOSX/earnings-recap/._SKILL.md': 'x',
      'earnings-recap/.DS_Store': 'x',
    });
    const sent = await prepareZip(finder, LIMITS);
    expect(sent.skill!.upload).toBe(finder);
    expect(sent.skill!.fileCount).toBe(2);

    const renamed = await zipOf({ 'recap v2/SKILL.md': SKILL_MD });
    const result = await prepareZip(renamed, LIMITS);
    expect(result.skill!.upload).toBe(renamed);
    expect(result.skill!.name).toBe('earnings-recap');
  });

  it('reads a LICENSE.txt beside the skill’s folder as the server does, set aside', async () => {
    const file = await zipOf({ 'earnings-recap/SKILL.md': SKILL_MD, 'earnings-recap/a.py': 'x', 'LICENSE.txt': 'MIT' });
    const result = await prepareZip(file, LIMITS);
    expect(result.skill!.upload).toBe(file);
    expect(result.skill!.fileCount).toBe(2);
  });

  it('refuses a zip with no SKILL.md where the server looks', async () => {
    const file = await zipOf({ 'a/SKILL.md': SKILL_MD, 'b/x.py': 'x' });
    expect((await prepareZip(file, LIMITS)).problem).toEqual({ kind: 'layout' });
  });

  it('holds a zip to the same path rules as a folder', async () => {
    const file = await zipOf({ 'x/SKILL.md': SKILL_MD, 'x/run;rm.sh': 'x' });
    expect((await prepareZip(file, LIMITS)).problem).toEqual({ kind: 'unsafePath', path: 'run;rm.sh' });
  });

  it('sends a file it cannot open untouched, for the server to say why', async () => {
    const file = new File(['not a zip'], 'x.zip', { type: 'application/zip' });
    const result = await prepareZip(file, LIMITS);
    expect(result.skill!.upload).toBe(file);
    expect(result.skill!.name).toBeNull();
  });

  it('refuses a zip over the upload cap unopened, once the cap is known', async () => {
    const limits = { ...LIMITS, max_upload_bytes: 4096 };
    const huge = new File([new Uint8Array(4097)], 'x.zip', { type: 'application/zip' });
    expect((await prepareZip(huge, limits)).problem).toEqual({ kind: 'uploadTooLarge', size: 4097, max: 4096 });
    expect((await prepareZip(huge, undefined)).skill!.upload).toBe(huge);
  });

  it('holds a zip to the wire cap, and what it zips itself to the stored cap', async () => {
    // The stored cap weighs the skill once the server has set aside what it
    // drops, which a zip as sent may be well over; a zip built here holds
    // only what is kept.
    const limits = { ...LIMITS, max_stored_bytes: 64 };
    const zipped = await zipOf({ 'earnings-recap/SKILL.md': SKILL_MD, 'earnings-recap/.git/HEAD': 'ref' });
    expect(zipped.size).toBeGreaterThan(64);
    expect((await prepareZip(zipped, limits)).skill!.upload).toBe(zipped);

    const folder = await prepareFolder('x', [{ path: 'SKILL.md', file: blob(SKILL_MD) }], limits);
    expect(folder.problem).toMatchObject({ kind: 'uploadTooLarge', max: 64 });
  });
});

describe('findClash', () => {
  const row = (over: Partial<SkillInfo>): SkillInfo =>
    ({ name: 'earnings-recap', origin: 'user', workspace_id: null, plugin_id: null, command: null, ...over }) as SkillInfo;

  it('meets a built-in by name or command', () => {
    expect(findClash('pdf', [row({ name: 'pdf', origin: 'platform' })], null)).toEqual({ kind: 'builtIn' });
    expect(findClash('recap', [row({ name: 'x', origin: 'platform', command: 'recap' })], null)).toEqual({
      kind: 'builtIn',
    });
  });

  it('meets only a skill in the scope it lands in', () => {
    const own = row({});
    const ws = row({ origin: 'workspace', workspace_id: 'ws-1' });
    expect(findClash('earnings-recap', [own, ws], null)).toEqual({ kind: 'own', skill: own });
    expect(findClash('earnings-recap', [own, ws], 'ws-1')).toEqual({ kind: 'own', skill: ws });
    expect(findClash('earnings-recap', [own], 'ws-2')).toBeNull();
  });

  it('tells a plugin’s skill from the user’s own', () => {
    const fromPlugin = row({ plugin_id: 'p1' });
    expect(findClash('earnings-recap', [fromPlugin], null)).toEqual({ kind: 'plugin', skill: fromPlugin });
  });
});
