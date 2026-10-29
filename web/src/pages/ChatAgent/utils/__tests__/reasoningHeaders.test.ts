// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { extractReasoningHeaders } from '../reasoningHeaders';

describe('extractReasoningHeaders', () => {
  it('names each phase of a header-led thought', () => {
    const content = '**Checking the filing**\n\n**Drafting the answer**\n\nSome closing note.';
    expect(extractReasoningHeaders(content)).toEqual(['Checking the filing', 'Drafting the answer']);
  });

  it('ignores inline emphasis inside prose', () => {
    const content = 'Definitions:\n\n- **ATH** = running max\n- **Attempt (retest) setup**: close within X%';
    expect(extractReasoningHeaders(content)).toEqual([]);
  });

  it('treats a bold line after opening prose as a sub-section, not a header', () => {
    const content = 'Data is clean.\n\n**Primary study: retest episodes.**\n\nGroup by bucket.';
    expect(extractReasoningHeaders(content)).toEqual([]);
  });

  it('is empty for nothing', () => {
    expect(extractReasoningHeaders('')).toEqual([]);
  });
});
