// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { DriveEntry } from '@/types/drive';
import { drivePathKey, driveLinkPath, followMove, isWithinKey, linkedFile, sortEntries } from '../drivePaths';

describe('linkedFile', () => {
  it('reads a Drive path in the space it names, and a relative one beside the file', () => {
    expect(linkedFile('personal', 'notes', '.agents/drive/sp-2/chart.png')).toEqual({ space: 'sp-2', path: 'chart.png' });
    expect(linkedFile('sp-2', 'notes', '.agents/drive/personal/a/chart.png')).toEqual({ space: 'personal', path: 'a/chart.png' });
    expect(linkedFile('personal', 'notes', '../img/chart.png')).toEqual({ space: 'personal', path: 'img/chart.png' });
  });
});

describe('rooted references in a Drive document', () => {
  it('reads a path that starts at the root from the space\'s top folder, as a workspace document reads one from the workspace\'s', () => {
    expect(linkedFile('personal', 'reports', '/images/logo.png')).toEqual({ space: 'personal', path: 'images/logo.png' });
    expect(driveLinkPath('reports', '/images/', true)).toBe('images');
    expect(driveLinkPath('reports', 'images/logo.png', true)).toBe('images/logo.png');
    expect(driveLinkPath('reports', 'images/logo.png', false)).toBe('reports/images/logo.png');
    expect(driveLinkPath('reports/q3', '../a.md', false)).toBe('reports/a.md');
  });
});

describe('drivePathKey', () => {
  it('folds as the server does: full case folding, a segment at a time', () => {
    expect(drivePathKey('Reports/Data.parquet')).toBe(drivePathKey('reports/DATA.parquet'));
    expect(drivePathKey('Straße.md')).toBe(drivePathKey('STRASSE.md'));
    expect(drivePathKey('ὀδυσσεύς')).toBe(drivePathKey('ὈΔΥΣΣΕΎΣ'));
    expect(drivePathKey('e\u0301.md')).toBe(drivePathKey('\u00c9.md'));
  });
});

describe('sortEntries', () => {
  const named = (name: string) => ({ name, kind: 'file', path: name, size: 1, modified_at: '' }) as DriveEntry;
  it('orders names by the locale it is given, numbers by value, case aside', () => {
    expect(sortEntries([named('ä'), named('z')], 'name', 'sv').map((e) => e.name)).toEqual(['z', 'ä']);
    expect(sortEntries([named('z'), named('ä')], 'name', 'de').map((e) => e.name)).toEqual(['ä', 'z']);
    expect(sortEntries([named('f10'), named('F2')], 'name', 'en-US').map((e) => e.name)).toEqual(['F2', 'f10']);
  });
});

describe('a path spelled as the URL spells it, against the server\'s', () => {
  it('follows a move of the open file or a folder above it, keeping the rest in its own spelling', () => {
    expect(followMove('Reports/Q3.md', 'reports/q3.md', 'reports/q3-final.md')).toBe('reports/q3-final.md');
    expect(followMove('REPORTS/Q3.md', 'reports', 'archive/reports')).toBe('archive/reports/Q3.md');
    expect(followMove('reports2/q3.md', 'reports', 'archive')).toBeNull();
  });

  it('slices by segments where folding changes a name\'s length', () => {
    // `STRASSE` folds to `strasse`, as `straße` does: one more character than the server's name.
    expect(followMove('STRASSE/Notes/a.md', 'straße', 'weg')).toBe('weg/Notes/a.md');
    expect(isWithinKey('STRASSE/Notes/a.md', 'straße/notes')).toBe(true);
  });

  it('knows the open file is in what went to the trash, whatever its case', () => {
    expect(isWithinKey('Reports/Q3.md', 'reports')).toBe(true);
    expect(isWithinKey('Reports/Q3.md', 'reports/q3.md')).toBe(true);
    expect(isWithinKey('reports2/q3.md', 'reports')).toBe(false);
  });
});
