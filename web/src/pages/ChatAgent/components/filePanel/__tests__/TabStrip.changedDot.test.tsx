/**
 * The amber dot clears when the tab re-reads its file, even where nothing
 * else about the strip changed. A compiled strip caches each tab on the
 * `hasChanged` it was handed, so a read mark has to reach it as a new
 * function, not as a counter bump behind the same one.
 */
import { describe, it, expect } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { TabStrip } from '../TabStrip';
import { useChangedFiles } from '../useChangedFiles';
import type { FileTab } from '../useFileTabs';
import type { TranscriptReader } from '../useTranscript';
import type { WriteEvent } from '../../../utils/fileRefResolver';
import { createTranscriptStore } from '../transcriptStore';

const tabs: FileTab[] = [{ id: 'a', kind: 'file', path: 'notes.md', preview: false, location: null, locationSeq: 0 }];
const noop = () => {};

function Strip({ transcript }: { transcript: TranscriptReader }) {
  const changed = useChangedFiles(transcript);
  return (
    <>
      <TabStrip
        tabs={tabs}
        activeId="a"
        onActivate={noop}
        onClose={noop}
        onPin={noop}
        onNewTab={null}
        hasChanged={changed.hasChanged}
        treeOpen={false}
        onToggleTree={null}
        onPanelClose={null}
      />
      <button type="button" onClick={() => changed.markRead('notes.md')}>read</button>
    </>
  );
}

describe('TabStrip changed dot', () => {
  it('sets on a write after the read and clears on the next read', () => {
    let log: WriteEvent[] = [];
    const collectWrites = () => log;
    const store = createTranscriptStore({ messages: [], collectWrites });
    render(<Strip transcript={store.reader} />);
    fireEvent.click(screen.getByText('read'));
    expect(screen.queryByTitle('Changed since you opened it')).toBeNull();

    log = [{ id: 'w1', path: 'notes.md' }];
    act(() => store.publish({ messages: [], collectWrites }));
    expect(screen.getByTitle('Changed since you opened it')).toBeInTheDocument();

    fireEvent.click(screen.getByText('read'));
    expect(screen.queryByTitle('Changed since you opened it')).toBeNull();
  });
});
