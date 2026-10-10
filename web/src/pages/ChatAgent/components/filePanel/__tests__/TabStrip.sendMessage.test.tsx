/**
 * A sent message's tab shows the app's favicon in place of the tool glyph and
 * keeps its name to the tool's, the app's name moving to the hint. A send that
 * did not go carries the failure mark, as a failed call does.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TabStrip } from '../TabStrip';
import type { FileTab } from '../useFileTabs';
import type { ToolCallProcessRecord } from '../../ToolCallDetailView';
import { createTranscriptStore } from '../transcriptStore';

const delivery = (status: string, platform: string | null) => ({
  type: 'message_delivery', status, code: null, address: platform, platform,
  current: false, duplicate: false, message: '', files: [],
});

function send(status: string, platform: string | null, target?: string): ToolCallProcessRecord {
  const args = target ? { text: 'hi', target } : { text: 'hi' };
  return {
    toolName: 'send_message',
    toolCall: { name: 'send_message', args },
    toolCallResult: { content: `status: ${status}`, artifact: delivery(status, platform) },
    isComplete: true,
  } as ToolCallProcessRecord;
}

function strip(records: Record<string, ToolCallProcessRecord>) {
  const tabs: FileTab[] = Object.keys(records).map((id) => ({ id, kind: 'tool', preview: false, toolCallId: id }));
  const transcript = createTranscriptStore({ messages: [{ toolCallProcesses: records }] }).reader;
  return render(
    <TabStrip
      tabs={tabs}
      activeId={tabs[0].id}
      onActivate={() => {}}
      onClose={() => {}}
      onPin={() => {}}
      onNewTab={null}
      hasChanged={() => false}
      transcript={transcript}
      treeOpen={false}
      onToggleTree={null}
      onPanelClose={null}
    />,
  );
}

const tab = (id: string) => document.querySelector(`[data-tab-id="${id}"]`) as HTMLElement;

describe('TabStrip send_message tabs', () => {
  it("shows the app's favicon and leaves the app out of the name", () => {
    strip({ tg: send('sent', 'telegram') });
    expect(tab('tg').querySelector('img')?.getAttribute('src')).toContain('domain=telegram.org');
    expect(tab('tg').querySelector('.file-panel-tab-name')?.textContent).toBe('Send Message');
  });

  it('finds the app from the target when the result names none', () => {
    strip({ sl: send('failed', null, 'slack:T1/C2') });
    expect(tab('sl').querySelector('img')?.getAttribute('src')).toContain('domain=slack.com');
  });

  it('marks a send that did not go, and only that one', () => {
    strip({ ok: send('sent', 'telegram'), bad: send('failed', 'discord') });
    const marks = screen.getAllByRole('img', { name: 'Tool call failed' });
    expect(marks).toHaveLength(1);
    expect(marks[0].closest('[role="tab"]')).toHaveAttribute('data-tab-id', 'bad');
  });

  it('keeps the tool glyph and the app in the name for an app with no site', () => {
    strip({ mx: send('sent', 'matrix', 'matrix:!room') });
    expect(tab('mx').querySelector('img')).toBeNull();
    expect(tab('mx').querySelector('.file-panel-tab-name')?.textContent).toBe('Send Message · Matrix');
  });
});
