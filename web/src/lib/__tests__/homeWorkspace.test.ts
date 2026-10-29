import { describe, expect, it } from 'vitest';
import { isHomeWorkspace } from '../homeWorkspace';

const row = (is_home: boolean, status: string) => ({ is_home, status });

describe('isHomeWorkspace', () => {
  it('reads is_home whatever the status, since that is the lifecycle of its computer', () => {
    expect(isHomeWorkspace(row(true, 'running'))).toBe(true);
    expect(isHomeWorkspace(row(true, 'unbound'))).toBe(true);
    expect(isHomeWorkspace(row(false, 'running'))).toBe(false);
  });

  it('reads no row as not Home', () => {
    expect(isHomeWorkspace(null)).toBe(false);
    expect(isHomeWorkspace(undefined)).toBe(false);
  });
});
