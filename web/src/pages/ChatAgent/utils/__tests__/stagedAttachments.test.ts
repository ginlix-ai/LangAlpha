// @vitest-environment node
/**
 * A send's staged files survive the round trip back to the composer (a held
 * send returned on Stop) with when each became ready, so the composer stages
 * it again before the server's expiry; that time never reaches the server.
 */
import { describe, expect, it } from 'vitest';

import { attachmentSendParts, stagedFilesOf } from '../fileUpload';

const file = (name: string, type: string) => new File(['x'], name, { type });

describe('staged attachments, sent and handed back', () => {
  it('carries readyAt through the bubble record and back, never into additional_context', () => {
    const sent = [
      { file: file('a.png', 'image/png'), type: 'image/png', attachmentId: 'att-a', readyAt: 1_700_000_000_000 },
      { file: file('b.pdf', 'application/pdf'), type: 'application/pdf', attachmentId: 'att-b' },
    ];
    const { contexts, meta } = attachmentSendParts(sent);

    expect(contexts).toEqual([
      { type: 'image', attachment_id: 'att-a', description: 'a.png' },
      { type: 'pdf', attachment_id: 'att-b', description: 'b.pdf' },
    ]);
    expect(meta[0].readyAt).toBe(1_700_000_000_000);
    expect('readyAt' in meta[1]).toBe(false);

    expect(stagedFilesOf(contexts, meta)).toEqual(sent);
  });
});
