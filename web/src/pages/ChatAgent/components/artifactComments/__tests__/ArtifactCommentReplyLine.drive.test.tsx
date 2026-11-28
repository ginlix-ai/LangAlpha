import { describe, expect, it } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { ArtifactCommentReplyLines } from '../ArtifactCommentReplyLine';
import { readArtifactCommentReply, type ArtifactCommentReply } from '../artifactCommentReply';

const ENTRY = '0b9e1c52-6f0a-4c7e-9d1e-3a2b5c4d6e7f';

/** A Drive reply as the tool leaves it: the record's key, its entry id, in `path`. */
function reply(): ArtifactCommentReply {
  return readArtifactCommentReply({
    type: 'artifact_comment', artifact_id: 'a-1', workspace_id: 'home', kind: 'drive', path: ENTRY,
    comment_id: 'c-1', number: 1, resolved: false, text: 'Done.', clipped: false,
  })!;
}

describe("the agent's reply line on a Drive file", () => {
  it('is read as the entry id it names, with no path', () => {
    expect(reply()).toMatchObject({ kind: 'drive', entryId: ENTRY, path: null });
  });

  it('never shows the entry id it is keyed by as the file\'s name', () => {
    renderWithProviders(<ArtifactCommentReplyLines members={[{ key: 'c-1', reply: reply() }]} />);

    expect(screen.queryByText(new RegExp(ENTRY))).toBeNull();
    expect(screen.getByText(/a Drive file/)).toBeTruthy();
  });
});
