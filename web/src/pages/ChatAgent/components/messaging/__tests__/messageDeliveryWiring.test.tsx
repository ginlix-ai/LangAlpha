/**
 * How a settled `send_message` reaches the transcript.
 *
 * With a `message_delivery` artifact it becomes a card, on both surfaces that
 * draw inline cards, and the card is the way to the detail panel. The card's
 * text comes from the call's arguments, so a surface that forgot to hand them
 * over would draw a card with no message on it: the preview is asserted on
 * each surface for that reason. Without the artifact (a result recorded before
 * it existed) the call stays the plain step row it always was.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { MessageContentSegments } from '../../MessageList';
import { MessageActionsProvider } from '../../messageList/MessageActionsContext';
import ActivityBlock from '../../ActivityBlock';
import {
  INLINE_ARTIFACT_TOOLS,
  isInlineArtifactReady,
} from '../../charts/InlineArtifactCards';
import type { ActivityItem } from '../../messageList/activityTypes';

const CALL_ID = 'call-send';
const ARGS = { text: 'Your NVDA brief is ready.', target: 'telegram:-100123' };
const ARTIFACT = {
  type: 'message_delivery',
  status: 'sent',
  code: null,
  address: 'telegram:-100123',
  platform: 'telegram',
  current: false,
  duplicate: false,
  message: 'Sent to the Telegram group.',
  files: [],
};

function proc(artifact?: Record<string, unknown>) {
  return {
    toolName: 'send_message',
    toolCallId: CALL_ID,
    toolCall: { id: CALL_ID, name: 'send_message', args: ARGS },
    toolCallResult: { content: 'status: sent', tool_call_id: CALL_ID, ...(artifact ? { artifact } : {}) },
    isInProgress: false,
    isComplete: true,
    isFailed: false,
    order: 0,
  };
}

function renderTranscript(onToolCallDetailClick: (id: string) => void, artifact?: Record<string, unknown>) {
  return renderWithProviders(
    <MessageActionsProvider actions={{ onToolCallDetailClick }}>
      <MessageContentSegments
        segments={[{ type: 'tool_call', toolCallId: CALL_ID }] as never}
        reasoningProcesses={{}}
        toolCallProcesses={{ [CALL_ID]: proc(artifact) } as never}
        todoListProcesses={{}}
        subagentTasks={{}}
        isStreaming={false}
      />
    </MessageActionsProvider>,
  );
}

describe('a settled send_message in the transcript', () => {
  it('renders the delivery card, with the message from the call', () => {
    renderTranscript(vi.fn(), ARTIFACT);
    const card = screen.getByTestId('message-delivery-card');
    expect(card).toHaveTextContent('Telegram');
    expect(card).toHaveTextContent('Sent');
    expect(screen.getByTestId('message-delivery-preview')).toHaveTextContent(ARGS.text);
  });

  it('opens the tool detail panel when the card is clicked', () => {
    const onToolCallDetailClick = vi.fn();
    renderTranscript(onToolCallDetailClick, ARTIFACT);
    fireEvent.click(screen.getByTestId('message-delivery-card'));
    expect(onToolCallDetailClick).toHaveBeenCalledWith(CALL_ID);
  });

  it('stays a plain step row when the result has no artifact', async () => {
    renderTranscript(vi.fn());
    expect(screen.queryByTestId('message-delivery-card')).toBeNull();
    // A settled turn folds its working; opening the fold shows the row.
    fireEvent.click(await screen.findByRole('button', { expanded: false }));
    expect(await screen.findByText('Send Message')).toBeInTheDocument();
    expect(screen.getByText('Telegram')).toBeInTheDocument();
  });
});

describe('a settled send_message in an activity block', () => {
  it('renders the card with the message from the call', () => {
    const onToolCallClick = vi.fn();
    const item = {
      ...proc(ARTIFACT),
      type: 'tool_call',
      id: CALL_ID,
      _liveState: 'completed',
    } as unknown as ActivityItem;
    renderWithProviders(
      <ActivityBlock items={[item]} isStreaming={false} onToolCallClick={onToolCallClick} />,
    );
    expect(screen.getByTestId('message-delivery-preview')).toHaveTextContent(ARGS.text);
    fireEvent.click(screen.getByTestId('message-delivery-card'));
    expect(onToolCallClick).toHaveBeenCalledWith(item);
  });
});

describe('the gate that makes a send a card', () => {
  it('opens on the artifact type, not on the tool name', () => {
    expect(isInlineArtifactReady('send_message', ARTIFACT)).toBe(true);
    expect(INLINE_ARTIFACT_TOOLS.has('send_message')).toBe(false);
  });

  it('stays shut for a result with no delivery artifact', () => {
    expect(isInlineArtifactReady('send_message', undefined)).toBe(false);
    // An artifact of some other shape keeps the row rather than becoming a
    // card slot with nothing to draw in it.
    expect(isInlineArtifactReady('send_message', { status: 'sent' })).toBe(false);
  });
});
