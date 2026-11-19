// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { ArtifactCommentMessage } from '@/types/api';
import { writerName } from '../commentWriter';

const t = ((key: string) => ({ 'sharedWorkspace.youShort': 'You', 'sharedWorkspace.someone': 'Someone' })[key] ?? key) as unknown as TFunction;

const message = (writer: Partial<ArtifactCommentMessage> = {}): ArtifactCommentMessage => ({
  message_id: 'm', seq: 1, author: 'user', body: 'x', sent_at: null, request_key: null,
  thread_id: null, created_at: '2026-11-15T00:00:00Z', ...writer,
});

describe("a comment message's writer", () => {
  it("reads as You in a personal workspace, where no writer is named", () => {
    expect(writerName(t, message(), 'lee')).toBe('You');
    expect(writerName(t, undefined, 'lee')).toBe('You');
  });

  it("names another member in a shared workspace, and You for the viewer's own", () => {
    expect(writerName(t, message({ written_by: 'maya', written_by_name: 'Maya Okafor' }), 'lee')).toBe('Maya Okafor');
    expect(writerName(t, message({ written_by: 'lee', written_by_name: 'Lee Chen' }), 'lee')).toBe('You');
    expect(writerName(t, message({ written_by: 'maya', written_by_name: null }), 'lee')).toBe('Someone');
  });
});
