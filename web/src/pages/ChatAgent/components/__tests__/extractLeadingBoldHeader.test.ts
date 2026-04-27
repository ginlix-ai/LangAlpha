/**
 * Reasoning-row subtitle promotion: confirms we only swap the generic
 * "Reasoning" label when the content really begins with a `**heading**\n\nbody`
 * shape, and we leave every other reasoning untouched.
 */
import { describe, it, expect } from 'vitest';
import { extractLeadingBoldHeader } from '../ActivityBlock';

describe('extractLeadingBoldHeader', () => {
  it('promotes a leading bold heading + blank line + body', () => {
    const r = extractLeadingBoldHeader('**Considering task organization**\n\nI think this task is simple.');
    expect(r.title).toBe('Considering task organization');
    expect(r.body).toBe('I think this task is simple.');
  });

  it('promotes when only one newline separates header and body', () => {
    const r = extractLeadingBoldHeader('**Header**\nBody starts here.');
    expect(r.title).toBe('Header');
    expect(r.body).toBe('Body starts here.');
  });

  it('tolerates leading whitespace before the bold marker', () => {
    const r = extractLeadingBoldHeader('   **Title**\n\nBody.');
    expect(r.title).toBe('Title');
    expect(r.body).toBe('Body.');
  });

  it('leaves plain paragraph reasoning alone', () => {
    const original = 'I need to figure out where this comes from. Let me read the file first.';
    const r = extractLeadingBoldHeader(original);
    expect(r.title).toBeNull();
    expect(r.body).toBe(original);
  });

  it('does not promote inline bold mid-sentence', () => {
    const original = 'I think **this** is a problem worth investigating.';
    const r = extractLeadingBoldHeader(original);
    expect(r.title).toBeNull();
    expect(r.body).toBe(original);
  });

  it('does not promote when bold runs into the body without a newline', () => {
    const original = '**Note:** the user wants to remove their preference.';
    const r = extractLeadingBoldHeader(original);
    expect(r.title).toBeNull();
    expect(r.body).toBe(original);
  });

  it('does not promote a bold-only paragraph with no body', () => {
    const r = extractLeadingBoldHeader('**Just a bold line, nothing follows**\n\n');
    expect(r.title).toBeNull();
  });

  it('rejects an over-long candidate (looks like a sentence, not a heading)', () => {
    const longBold = '**' + 'word '.repeat(30).trim() + '**\n\nBody.';
    const r = extractLeadingBoldHeader(longBold);
    expect(r.title).toBeNull();
  });

  it('returns null safely for empty content', () => {
    const r = extractLeadingBoldHeader('');
    expect(r.title).toBeNull();
    expect(r.body).toBe('');
  });

  it('does not match nested asterisks inside the bold span', () => {
    // The capture group rejects internal `*` so the sequence below can\'t be parsed
    // as a clean heading; we play safe and leave content as-is.
    const original = '**A *nested* asterisk title**\n\nBody.';
    const r = extractLeadingBoldHeader(original);
    expect(r.title).toBeNull();
    expect(r.body).toBe(original);
  });
});
