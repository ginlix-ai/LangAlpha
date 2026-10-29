// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { getAvailableTypes, getFileType } from '../fileMeta';

describe('getFileType', () => {
  it('files a macro workbook and a bitmap under the category a reader looks in', () => {
    // Both are extensions the panel already lists and the relay already
    // qualifies, so landing them in `Other` hid them behind the type filter
    // rather than making them unopenable. `getFileIcon` had `bmp` as an image
    // all along, which is what made the miss hard to see.
    expect(getFileType('results/model.xlsm')).toBe('Data');
    expect(getFileType('results/scan.bmp')).toBe('Image');
  });

  it('keeps the categories it already had', () => {
    expect(getFileType('results/report.md')).toBe('Docs');
    expect(getFileType('work/model.py')).toBe('Code');
    expect(getFileType('results/index.jsonl')).toBe('Data');
    expect(getFileType('results/chart.png')).toBe('Image');
    expect(getFileType('results/archive.zip')).toBe('Other');
  });

  it('offers a filter only for the categories present', () => {
    expect(getAvailableTypes(['a.md', 'b.xlsm', 'c.bmp'])).toEqual(['Docs', 'Data', 'Image']);
  });
});
