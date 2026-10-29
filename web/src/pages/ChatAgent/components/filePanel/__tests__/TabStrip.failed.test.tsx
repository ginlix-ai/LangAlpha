/**
 * A tool tab whose call failed carries a failure mark beside its name whether
 * or not it is in front. A Task reports its outcome through its own status
 * chip, so it never gets the mark (the rule DetailPanel's header follows).
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TabStrip } from '../TabStrip';
import type { FileTab } from '../useFileTabs';
import type { ToolCallProcessRecord } from '../../ToolCallDetailView';
import { createTranscriptStore } from '../transcriptStore';

const tabs: FileTab[] = [
  { id: 'a', kind: 'tool', preview: false, toolCallId: 'tc-failed' },
  { id: 'b', kind: 'tool', preview: false, toolCallId: 'tc-ok' },
  { id: 'c', kind: 'tool', preview: false, toolCallId: 'tc-task' },
];

const records: Record<string, ToolCallProcessRecord> = {
  'tc-failed': { toolName: 'web_search', toolCall: { name: 'web_search', args: { query: 'AAPL' } }, isFailed: true },
  'tc-ok': { toolName: 'web_search', toolCall: { name: 'web_search', args: { query: 'MSFT' } }, isComplete: true },
  'tc-task': { toolName: 'Task', toolCall: { name: 'Task', args: {} }, isFailed: true },
};

const transcript = createTranscriptStore({ messages: [{ toolCallProcesses: records }] }).reader;

const strip = (activeId: string) => (
  <TabStrip
    tabs={tabs}
    activeId={activeId}
    onActivate={() => {}}
    onClose={() => {}}
    onPin={() => {}}
    onNewTab={null}
    hasChanged={() => false}
    transcript={transcript}
    treeOpen={false}
    onToggleTree={null}
    onPanelClose={null}
  />
);

describe('TabStrip failed tool tabs', () => {
  it('marks the failed call on its tab, in front or not, and no other', () => {
    const { rerender } = render(strip('b'));
    const marks = screen.getAllByRole('img', { name: 'Tool call failed' });
    expect(marks).toHaveLength(1);
    expect(marks[0].closest('[role="tab"]')).toHaveAttribute('data-tab-id', 'a');

    rerender(strip('a'));
    expect(screen.getAllByRole('img', { name: 'Tool call failed' })).toHaveLength(1);
  });
});
