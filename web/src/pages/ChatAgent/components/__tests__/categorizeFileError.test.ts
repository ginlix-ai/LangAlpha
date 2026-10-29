import { describe, it, expect } from 'vitest';
import { categorizeFileError } from '../FilePanel';

function makeApiError(status: number, detail: string) {
  return { response: { status, data: { detail } } };
}

describe('categorizeFileError', () => {
  it('returns not_found for 404 with running workspace', () => {
    const err = makeApiError(404, 'File not found');
    expect(categorizeFileError(err, 'running')).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns not_backed_up for 404 with stopped workspace', () => {
    const err = makeApiError(404, 'File not found');
    expect(categorizeFileError(err, 'stopped')).toEqual({
      category: 'not_backed_up',
      detail: 'File not found',
    });
  });

  it('returns not_backed_up for 404 with stopping workspace', () => {
    const err = makeApiError(404, 'File not found');
    expect(categorizeFileError(err, 'stopping')).toEqual({
      category: 'not_backed_up',
      detail: 'File not found',
    });
  });

  it('returns binary_file for 415', () => {
    const err = makeApiError(415, 'Cannot read binary file as text.');
    expect(categorizeFileError(err)).toEqual({
      category: 'binary_file',
      detail: 'Cannot read binary file as text.',
    });
  });

  it('returns sandbox_starting for 503 with "starting" in detail', () => {
    const err = makeApiError(503, 'Sandbox is still starting');
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_starting',
      detail: 'Sandbox is still starting',
    });
  });

  it('returns sandbox_unavailable for 503 without "starting"', () => {
    const err = makeApiError(503, 'Sandbox not available');
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_unavailable',
      detail: 'Sandbox not available',
    });
  });

  it('returns sandbox_starting for the 400 Home sends before its computer', () => {
    const err = makeApiError(400, 'Home is not on a computer yet');
    expect(categorizeFileError(err)).toEqual({
      category: 'sandbox_starting',
      detail: 'Home is not on a computer yet',
    });
  });

  it('returns unknown for any other 400', () => {
    const err = makeApiError(400, 'File path is required');
    expect(categorizeFileError(err)).toEqual({
      category: 'unknown',
      detail: 'File path is required',
    });
  });

  it('carries the server\'s sentence on a 422 that has one', () => {
    const err = makeApiError(422, 'The machine did not accept our key for dev.');
    expect(categorizeFileError(err)).toEqual({
      category: 'unknown',
      detail: 'The machine did not accept our key for dev.',
      message: 'The machine did not accept our key for dev.',
    });
    expect(categorizeFileError({ response: { status: 422, data: { detail: [{ msg: 'x' }] } } })).toEqual({
      category: 'unknown',
      detail: '',
    });
  });

  it('returns access_denied for 403', () => {
    const err = makeApiError(403, 'Access denied: /etc is not in allowed directories');
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
    const err = makeApiError(404, 'File not found');
    expect(categorizeFileError(err, undefined)).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns not_found for 404 with unexpected wsStatus', () => {
    const err = makeApiError(404, 'File not found');
    expect(categorizeFileError(err, 'error')).toEqual({
      category: 'not_found',
      detail: 'File not found',
    });
  });

  it('returns unknown for unhandled status codes like 500', () => {
    const err = makeApiError(500, 'Internal server error');
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
