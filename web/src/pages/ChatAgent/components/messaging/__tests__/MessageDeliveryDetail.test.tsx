/**
 * A sent message in the tool detail panel, through the real panel dispatch.
 *
 * The panel is where a reader goes to see what the agent actually said and
 * what became of each file, so both are checked against the call's arguments
 * and the artifact. A result recorded before the artifact existed must keep
 * rendering as the raw text it always did.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';

vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
  CodeBlock: ({ code }: { code: string }) => <pre>{code}</pre>,
}));

import ToolCallDetailView from '../../ToolCallDetailView';

const TEXT = '## Daily brief\n\nNVDA closed **up 3%**.';
const RAW = 'status: sent\nto: telegram\nSent to the user\'s Telegram chat with the bot.';

function proc(artifact: Record<string, unknown> | undefined, args: Record<string, unknown> = { text: TEXT }) {
  return {
    toolName: 'send_message',
    toolCall: { id: 'call-1', name: 'send_message', args },
    toolCallResult: { content: RAW, ...(artifact ? { artifact } : {}) },
    isComplete: true,
  };
}

function delivery(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: 'message_delivery',
    status: 'sent',
    code: null,
    address: 'telegram:-100123',
    platform: 'telegram',
    current: true,
    duplicate: false,
    message: "Sent to the user's Telegram chat with the bot.",
    files: [],
    ...overrides,
  };
}

describe('the send_message detail panel', () => {
  it('shows where it went, its status, and the message in full', () => {
    render(<ToolCallDetailView toolCallProcess={proc(delivery())} />);

    const panel = screen.getByTestId('message-delivery-detail');
    expect(within(panel).getByText('Telegram')).toBeInTheDocument();
    expect(within(panel).getByText('Same chat')).toBeInTheDocument();
    expect(within(panel).getByText('telegram:-100123')).toBeInTheDocument();
    expect(within(panel).getByTestId('delivery-status-sent')).toHaveTextContent('Sent');
    // The message, as markdown, from the call: the artifact does not carry it.
    expect(within(panel).getByTestId('markdown-content').textContent).toBe(TEXT);
    // The outcome sentence is there, but not as the raw result.
    expect(within(panel).getByText("Sent to the user's Telegram chat with the bot.")).toBeInTheDocument();
    expect(screen.queryByText(/status: sent/)).toBeNull();
  });

  it("leaves the app's favicon to the tab above", () => {
    render(<ToolCallDetailView toolCallProcess={proc(delivery())} />);

    expect(screen.getByTestId('message-delivery-detail').querySelector('img')).toBeNull();
  });

  it('opens a file in the file panel when it is clicked', () => {
    const onOpenFile = vi.fn();
    const files = [{ path: 'messaging_test/attachment_test.png', status: 'sent', reason: null }];
    render(<ToolCallDetailView toolCallProcess={proc(delivery({ files }))} onOpenFile={onOpenFile} />);

    fireEvent.click(screen.getByRole('button', { name: 'messaging_test/attachment_test.png' }));
    expect(onOpenFile).toHaveBeenCalledWith('messaging_test/attachment_test.png', undefined);
  });

  it("opens a file from the workspace the call read it from", () => {
    const onOpenFile = vi.fn();
    const args = { text: TEXT, files: ['out/a.png'], workspace_id: 'ws-flash-1' };
    const files = [{ path: 'out/a.png', status: 'sent', reason: null }];
    render(<ToolCallDetailView toolCallProcess={proc(delivery({ files }), args)} onOpenFile={onOpenFile} />);

    fireEvent.click(screen.getByRole('button', { name: 'out/a.png' }));
    expect(onOpenFile).toHaveBeenCalledWith('out/a.png', 'ws-flash-1');
  });

  it('leaves out an address that only repeats the app', () => {
    render(<ToolCallDetailView toolCallProcess={proc(delivery({ address: 'telegram', current: false }))} />);

    const panel = screen.getByTestId('message-delivery-detail');
    expect(within(panel).getByText('Telegram')).toBeInTheDocument();
    expect(within(panel).queryByText('telegram')).toBeNull();
  });

  it('gives each file its own outcome and reason', () => {
    const files = [
      { path: 'messaging_test/attachment_test.png', status: 'sent', reason: null },
      { path: 'messaging_test/data.csv', status: 'linked', reason: 'sent as a download link' },
      { path: 'messaging_test/huge.zip', status: 'failed', reason: 'larger than the app accepts' },
    ];
    render(<ToolCallDetailView toolCallProcess={proc(delivery({ status: 'partial', files }))} />);

    expect(screen.getByTestId('delivery-status-partial')).toHaveTextContent('Partly sent');
    const rows = screen.getAllByTestId('message-delivery-detail-file');
    expect(rows).toHaveLength(3);
    expect(within(rows[0]).getByText('messaging_test/attachment_test.png')).toBeInTheDocument();
    expect(within(rows[0]).getByTestId('delivery-file-sent')).toHaveTextContent('Sent');
    expect(within(rows[1]).getByTestId('delivery-file-linked')).toHaveTextContent('Link');
    expect(within(rows[1]).getByText('sent as a download link')).toBeInTheDocument();
    expect(within(rows[2]).getByTestId('delivery-file-failed')).toHaveTextContent('Failed');
    expect(within(rows[2]).getByText('larger than the app accepts')).toBeInTheDocument();
  });

  it('lists the files the call named as not sent when the send failed before delivery', () => {
    const args = { text: TEXT, files: ['out/a.png', 'out/b.pdf'], target: 'discord:G/C' };
    render(
      <ToolCallDetailView
        toolCallProcess={proc(
          delivery({ status: 'failed', code: 'not_allowed', platform: null, address: null, current: false, message: 'That chat is not one you picked.' }),
          args,
        )}
      />,
    );

    expect(screen.getByTestId('delivery-status-failed')).toHaveTextContent('Not sent');
    // No platform resolved: the target the call named stands in.
    expect(screen.getByText('Discord')).toBeInTheDocument();
    expect(screen.getByText('discord:G/C')).toBeInTheDocument();
    const rows = screen.getAllByTestId('message-delivery-detail-file');
    expect(rows.map((row) => within(row).getByTestId('delivery-file-not_sent').textContent)).toEqual(['Not sent', 'Not sent']);
    expect(within(rows[1]).getByText('out/b.pdf')).toBeInTheDocument();
    expect(screen.getByText('That chat is not one you picked.')).toBeInTheDocument();
  });

  it('lists named files with no verdict when the send itself is unknown', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={proc(delivery({ status: 'unknown', files: [] }), { text: TEXT, files: ['out/a.png'] })}
      />,
    );
    const [row] = screen.getAllByTestId('message-delivery-detail-file');
    expect(within(row).getByText('out/a.png')).toBeInTheDocument();
    expect(within(row).queryByTestId(/delivery-file-/)).toBeNull();
  });

  it('notes a send an earlier attempt already made', () => {
    const { rerender } = render(<ToolCallDetailView toolCallProcess={proc(delivery())} />);
    expect(screen.queryByText('Sent earlier')).toBeNull();
    rerender(<ToolCallDetailView toolCallProcess={proc(delivery({ duplicate: true }))} />);
    expect(screen.getByText('Sent earlier')).toBeInTheDocument();
  });

  it('keeps rendering a result recorded before the artifact as its raw text', () => {
    render(<ToolCallDetailView toolCallProcess={proc(undefined)} />);
    expect(screen.queryByTestId('message-delivery-detail')).toBeNull();
    expect(screen.getByTestId('markdown-content').textContent).toBe(RAW);
  });
});
