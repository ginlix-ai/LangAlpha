import { describe, it, expect } from 'vitest';
import { categorizeFileError } from '../FilePanel';

function makeAxiosError(status: number, detail: string) {
  return { response: { status, data: { detail } } };
}

describe('categorizeFileError', () => {
  it('returns not_found for 404 with running workspace', () => {
    const err = makeAxiosError(404, 'File not found');
    expect(categorizeFileError(err, 'running')).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns not_backed_up for 404 with stopped workspace', () => {
    const err = makeAxiosError(404, 'File not found');
    expect(categorizeFileError(err, 'stopped')).toEqual({
      category: 'not_backed_up',
      detail: 'File not found',
    });
  });

  it('returns not_backed_up for 404 with stopping workspace', () => {
    const err = makeAxiosError(404, 'File not found');
    expect(categorizeFileError(err, 'stopping')).toEqual({
      category: 'not_backed_up',
      detail: 'File not found',
    });
  });

  it('returns binary_file for 415', () => {
    const err = makeAxiosError(415, 'Cannot read binary file as text.');
    expect(categorizeFileError(err)).toEqual({
      category: 'binary_file',
      detail: 'Cannot read binary file as text.',
    });
  });

  it('returns sandbox_starting for 503 with "starting" in detail', () => {
    const err = makeAxiosError(503, 'Sandbox is still starting');
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_starting',
      detail: 'Sandbox is still starting',
    });
  });

  it('returns sandbox_unavailable for 503 without "starting"', () => {
    const err = makeAxiosError(503, 'Sandbox not available');
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_unavailable',
      detail: 'Sandbox not available',
    });
  });

  it('returns no_sandbox for 400 with "flash" in detail', () => {
    const err = makeAxiosError(400, 'Flash workspaces do not have a sandbox');
    expect(categorizeFileError(err)).toEqual({
      category: 'no_sandbox',
      detail: 'Flash workspaces do not have a sandbox',
    });
  });

  it('returns unknown for 400 without "flash"', () => {
    const err = makeAxiosError(400, 'File path is required');
    expect(categorizeFileError(err)).toEqual({
      category: 'unknown',
      detail: 'File path is required',
    });
  });

  it('returns access_denied for 403', () => {
    const err = makeAxiosError(403, 'Access denied: /etc is not in allowed directories');
    expect(categorizeFileError(err)).toEqual({
      category: 'access_denied',
      detail: 'Access denied: /etc is not in allowed directories',
    });
  });

  it('returns unknown for network error (no response)', () => {
    const err = new Error('Network Error');
    expect(categorizeFileError(err)).toEqual({
      category: 'unknown',
      detail: '',
    });
  });

  it('returns unknown for null error', () => {
    expect(categorizeFileError(null)).toEqual({
      category: 'unknown',
      detail: '',
    });
  });

  it('returns unknown for undefined error', () => {
    expect(categorizeFileError(undefined)).toEqual({
      category: 'unknown',
      detail: '',
    });
  });

  it('returns not_found for 404 when wsStatus is undefined', () => {
    const err = makeAxiosError(404, 'File not found');
    expect(categorizeFileError(err, undefined)).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns not_found for 404 with unexpected wsStatus', () => {
    const err = makeAxiosError(404, 'File not found');
    expect(categorizeFileError(err, 'error')).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns unknown for unhandled status codes like 500', () => {
    const err = makeAxiosError(500, 'Internal server error');
    expect(categorizeFileError(err)).toEqual({
      category: 'unknown',
      detail: 'Internal server error',
    });
  });

  it('returns empty detail when response.data.detail is non-string', () => {
    const err = { response: { status: 503, data: { detail: [{ msg: 'error' }] } } };
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_unavailable',
      detail: '',
    });
  });
});
