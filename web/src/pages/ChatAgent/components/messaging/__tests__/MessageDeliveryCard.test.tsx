/**
 * The sent-message pill. Its verdict is the one fact a reader scans a
 * transcript for, so every status is drawn here, and a send that did not
 * fully arrive has to read differently from one that did. The files' count
 * reads "ok/N" once any of them missed, and the app is told by its favicon.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
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

const file = (path: string, status: string | null) => ({ path, status, reason: null });

function files(...statuses: (string | null)[]) {
  return statuses.map((status, i) => file(`out/f${i}.png`, status));
}

describe('MessageDeliveryCard', () => {
  it.each<[DeliveryStatus, string]>([
    ['sent', 'Sent'],
    ['partial', 'Partly sent'],
    ['failed', 'Not sent'],
    ['unknown', 'Unknown'],
  ])('labels a %s send "%s"', (status, label) => {
    render(<MessageDeliveryCard artifact={artifact({ status })} toolArgs={ARGS} />);
    expect(screen.getByTestId(`delivery-status-${status}`)).toHaveTextContent(label);
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

  it.each([
    ['telegram', 'telegram:-100123', 'telegram.org'],
    ['slack', 'slack:T1/C2', 'slack.com'],
    ['discord', 'discord:G/C', 'discord.com'],
    ['imessage', 'imessage', 'apple.com'],
    ['feishu', 'feishu:oc_1', 'feishu.cn'],
  ])('shows the %s favicon', (platform, address, domain) => {
    const { container } = render(<MessageDeliveryCard artifact={artifact({ platform, address })} toolArgs={ARGS} />);
    expect(container.querySelector('img')?.getAttribute('src')).toContain(`domain=${domain}`);
  });

  it('shows the monogram of an app this build does not list, and no image', () => {
    const { container } = render(
      <MessageDeliveryCard artifact={artifact({ platform: 'matrix', address: 'matrix:!r' })} toolArgs={ARGS} />,
    );
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('Matrix')).toBeInTheDocument();
    expect(screen.getByText('M')).toBeInTheDocument();
  });

  it('falls back to the monogram of "Message" when nothing names the app', () => {
    const { container } = render(
      <MessageDeliveryCard artifact={artifact({ status: 'failed', platform: null, address: null })} toolArgs={ARGS} />,
    );
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('Message')).toBeInTheDocument();
    expect(screen.getByText('M')).toBeInTheDocument();
  });

  it('names the app, and takes it from the call target when no platform resolved', () => {
    const { rerender } = render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} />);
    expect(screen.getByText('Telegram')).toBeInTheDocument();
    rerender(
      <MessageDeliveryCard
        artifact={artifact({ status: 'failed', platform: null, address: null })}
        toolArgs={{ ...ARGS, target: 'slack:T1/C2' }}
      />,
    );
    expect(screen.getByText('Slack')).toBeInTheDocument();
  });

  it('shows no count for a send without files', () => {
    render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} />);
    expect(screen.queryByTestId('message-delivery-files')).toBeNull();
  });

  it('counts every file when all of them went', () => {
    render(<MessageDeliveryCard artifact={artifact({ files: files('sent', 'sent', 'sent', 'sent', 'sent') })} toolArgs={ARGS} />);
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·5');
  });

  it('counts a file that went as a link as delivered', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'partial', files: files('sent', 'linked', 'failed') })}
        toolArgs={ARGS}
      />,
    );
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·2/3');
  });

  it('counts a linked file alone as delivered', () => {
    render(<MessageDeliveryCard artifact={artifact({ files: files('linked', 'sent') })} toolArgs={ARGS} />);
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·2');
  });

  it('reads ok/N when one of three failed', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'partial', files: files('sent', 'sent', 'failed') })}
        toolArgs={ARGS}
      />,
    );
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·2/3');
  });

  it('reads 0/1 for a file the send never reached', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'failed', files: [] })}
        toolArgs={{ ...ARGS, files: ['out/a.png'] }}
      />,
    );
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·0/1');
  });

  it('reads N for files of a send whose outcome is unknown', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ status: 'unknown', files: [] })}
        toolArgs={{ ...ARGS, files: ['out/a.png', 'out/b.png'] }}
      />,
    );
    expect(screen.getByTestId('message-delivery-files').textContent).toBe('·2');
  });

  it('carries no message preview and no conversation marker', () => {
    render(<MessageDeliveryCard artifact={artifact({ current: true })} toolArgs={ARGS} />);
    const pill = screen.getByTestId('message-delivery-card');
    expect(pill).not.toHaveTextContent('NVDA');
    expect(pill).not.toHaveTextContent('Same chat');
    expect(pill).not.toHaveTextContent('This conversation');
    expect(screen.queryByTestId('message-delivery-preview')).toBeNull();
  });

  it('labels itself with the app, the files and the verdict', () => {
    render(
      <MessageDeliveryCard
        artifact={artifact({ files: files('sent', 'sent', 'sent', 'sent', 'sent') })}
        toolArgs={ARGS}
      />,
    );
    const pill = screen.getByTestId('message-delivery-card');
    expect(pill).toHaveAttribute('aria-label', 'Telegram \u00b7 5 files \u00b7 Sent');
    expect(pill).toHaveAttribute('title', 'Telegram \u00b7 5 files \u00b7 Sent');
  });

  it('says "1 file" in the singular and leaves files out when there are none', () => {
    const { rerender } = render(<MessageDeliveryCard artifact={artifact({ files: files('sent') })} toolArgs={ARGS} />);
    expect(screen.getByTestId('message-delivery-card')).toHaveAttribute('aria-label', 'Telegram \u00b7 1 file \u00b7 Sent');
    rerender(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} />);
    expect(screen.getByTestId('message-delivery-card')).toHaveAttribute('aria-label', 'Telegram \u00b7 Sent');
  });

  it('opens on a click or from the keyboard', () => {
    const onClick = vi.fn();
    render(<MessageDeliveryCard artifact={artifact()} toolArgs={ARGS} onClick={onClick} />);
    const pill = screen.getByRole('button');
    fireEvent.click(pill);
    // A real button turns Enter into a click in a browser; jsdom does not.
    fireEvent.keyDown(pill, { key: 'Enter' });
    expect(pill.tagName).toBe('BUTTON');
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it('draws nothing for an artifact that is not a delivery', () => {
    const { container } = render(<MessageDeliveryCard artifact={{ type: 'web_search' }} toolArgs={ARGS} />);
    expect(container).toBeEmptyDOMElement();
  });
});
