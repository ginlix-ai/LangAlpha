/**
 * The sent-message card. Its verdict is the one fact a reader scans a
 * transcript for, so every status is drawn here, and a send that did not
 * fully arrive has to read differently from one that did. The message itself
 * is the call's argument, not the artifact's, so the preview is checked to
 * come from there.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageDeliveryCard } from '../MessageDeliveryCard';
import type { DeliveryStatus } from '../messageDelivery';

function artifact(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: 'message_delivery',
    status: 'sent',
    code: null,
    address: 'telegram:-100123',
    platform: 'telegram',
    current: false,
    duplicate: false,
    message: "Sent to the user's Telegram chat with the bot.",
    files: [],
    ...overrides,
  };
}

const ARGS = { text: '**NVDA** closed up 3%. See [the chart](results/nvda.png).' };

describe('MessageDeliveryCard', () => {
  it.each<[DeliveryStatus, string]>([
    ['sent', 'Sent'],
    ['partial', 'Partly sent'],
    ['failed', 'Not sent'],
    ['unknown', 'Unknown'],
  ])('labels a %s send "%s"', (status, label) => {
    render(<MessageDeliveryCard artifact={artifact({ status })} toolArgs={ARGS} />);
    const pill = screen.getByTestId(`delivery-status-${status}`);
    expect(pill).toHaveTextContent(label);
  });

  it('wears a different colour for every verdict short of sent', () => {
    const colours = (['sent', 'partial', 'failed', 'unknown'] as const).map((status) => {
      const { unmount } = render(<MessageDeliveryCard artifact={artifact({ status })} toolArgs={ARGS} />);
      const colour = screen.getByTestId(`delivery-status-${status}`).style.color;
      unmount();
      return colour;
    });
    expect(colours[0]).toBe('var(--color-success)');
    expect(colours[2]).toBe('var(--color-icon-danger)');
    expect(new Set([colours[0], colours[1], colours[2]]).size).toBe(3);
    expect(colours[3]).not.toBe(colours[0]);
  });

  it('reads an unrecognized status as unknown rather than sent', () => {
    render(<MessageDeliveryCard artifact={artifact({ status: 'queued' })} toolArgs={ARGS} />);
    expect(screen.getByTestId('delivery-status-unknown')).toBeInTheDocument();
  });

  it('names the app, and the conversation when the send went into it', () => {
    const { rerender } = render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} />);
    expect(screen.getByText('Telegram')).toBeInTheDocument();
    expect(screen.queryByText('This conversation')).toBeNull();

    rerender(<MessageDeliveryCard artifact={artifact({ current: true })} toolArgs={ARGS} />);
    expect(screen.getByText('This conversation')).toBeInTheDocument();
  });

  it('previews the message from the call arguments, as plain text', () => {
    render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} />);
    expect(screen.getByTestId('message-delivery-preview')).toHaveTextContent(
      'NVDA closed up 3%. See the chart.',
    );
  });

  it('keeps each line of the message on its own line', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact()}
        toolArgs={{ text: '# NVDA brief\n\n- closed up **3%**\n```py\nprint(1)\n```\nSee the chart.' }}
      />,
    );
    expect(screen.getByTestId('message-delivery-preview').textContent).toBe(
      'NVDA brief\nclosed up 3%\nSee the chart.',
    );
  });

  it('previews a message that is only code by its code', () => {
    render(<MessageDeliveryCard artifact={artifact()} toolArgs={{ text: '```\nls -la\n```' }} />);
    expect(screen.getByTestId('message-delivery-preview')).toHaveTextContent('ls -la');
  });

  it('shows no preview for a files-only send', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ files: [{ path: 'a/report.pdf', status: 'sent', reason: null }] })}
        toolArgs={{ text: '', files: ['a/report.pdf'] }}
      />,
    );
    expect(screen.queryByTestId('message-delivery-preview')).toBeNull();
    expect(screen.getByText('report.pdf')).toBeInTheDocument();
  });

  it('lists the attachments and flags the one that failed, even past the chip limit', () => {
    const files = [
      { path: 'out/a.png', status: 'sent', reason: null },
      { path: 'out/b.png', status: 'sent', reason: null },
      { path: 'out/c.csv', status: 'linked', reason: null },
      { path: 'out/huge.zip', status: 'failed', reason: 'too large' },
    ];
    render(<MessageDeliveryCard artifact={artifact({ status: 'partial', files })} toolArgs={ARGS} />);

    const failed = screen.getByTestId('message-delivery-file-failed');
    expect(failed).toHaveTextContent('huge.zip');
    expect(failed.getAttribute('title')).toContain('too large');
    expect(screen.getAllByTestId('message-delivery-file')).toHaveLength(2);
    expect(screen.getByText('+1')).toBeInTheDocument();
  });

  it('falls back to the target the call named when no platform resolved', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'failed', platform: null, address: null, code: 'not_allowed' })}
        toolArgs={{ ...ARGS, target: 'slack:T1/C2' }}
      />,
    );
    expect(screen.getByText('Slack')).toBeInTheDocument();
    expect(screen.getByTestId('delivery-status-failed')).toBeInTheDocument();
  });

  it('says only "Message" when nothing names the app', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'failed', platform: null, address: null })}
        toolArgs={ARGS}
      />,
    );
    expect(screen.getByText('Message')).toBeInTheDocument();
  });

  it('capitalizes an app this build does not list', () => {
    render(<MessageDeliveryCard artifact={artifact({ platform: 'matrix', address: 'matrix:!r' })} toolArgs={ARGS} />);
    expect(screen.getByText('Matrix')).toBeInTheDocument();
  });

  it('opens on a click or from the keyboard', () => {
    const onClick = vi.fn();
    render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} onClick={onClick} />);
    const card = screen.getByRole('button');
    fireEvent.click(card);
    fireEvent.keyDown(card, { key: 'Enter' });
    expect(onClick).toHaveBeenCalledTimes(2);
    expect(within(card).getByText('Telegram')).toBeInTheDocument();
  });

  it('draws nothing for an artifact that is not a delivery', () => {
    const { container } = render(<MessageDeliveryCard artifact={{ type: 'web_search' }} toolArgs={ARGS} />);
    expect(container).toBeEmptyDOMElement();
  });
});
