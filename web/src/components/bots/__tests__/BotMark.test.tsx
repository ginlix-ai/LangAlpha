/**
 * The one bot mark component: the identity it draws, the sizes it is used at,
 * and the accessibility contract (a labelled image, or hidden beside a name).
 */
import { describe, it, expect } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { BotMark } from '../BotMark';
import { BOT_COLORS, botColorToken, lookOfBot } from '@/lib/botMark';

const markOf = (botId: string, name = 'Scout', avatar: Record<string, unknown> = {}) =>
  lookOfBot({ bot_id: botId, name, avatar });

function colorOf(botId: string): string {
  const look = markOf(botId);
  return look.kind === 'mark' ? botColorToken(look.color) : '';
}

describe('BotMark', () => {
  it('draws the id-derived mark with a label', () => {
    const { container } = render(<BotMark bot={{ bot_id: 'bot-1', name: 'Scout', avatar: {} }} />);
    const mark = screen.getByRole('img', { name: 'Scout' });
    const look = markOf('bot-1');

    expect(mark).toHaveAttribute('data-bot-mark');
    expect(mark).toHaveAttribute('data-mark-kind', 'mark');
    expect(mark).toHaveTextContent('S');
    const element = mark as HTMLElement;
    // The fill is the palette token: a background for the CSS-rounded shapes
    // and the SVG's fill for the drawn ones.
    const token = colorOf('bot-1');
    const svg = container.querySelector('svg');
    if (svg) {
      expect(svg.querySelector(`[fill="${token}"]`)).not.toBeNull();
    } else {
      expect(element.getAttribute('style')).toContain(`background-color: ${token}`);
    }
    expect(look.kind).toBe('mark');
    expect(element.style.width).toBe('22px');
    expect(element.style.height).toBe('22px');
  });

  it('gives the same bot the same mark at a different size', () => {
    const small = render(<BotMark bot={{ bot_id: 'bot-7', name: 'Scout' }} size={16} />);
    const { container, unmount } = small;
    const smallMark = container.querySelector('[data-bot-mark]') as HTMLElement;
    unmount();

    const large = render(<BotMark bot={{ bot_id: 'bot-7', name: 'Scout' }} size={36} />);
    const largeMark = large.container.querySelector('[data-bot-mark]') as HTMLElement;

    expect(smallMark).toHaveAttribute('data-mark-color', largeMark.getAttribute('data-mark-color'));
    expect(smallMark).toHaveAttribute('data-mark-shape', largeMark.getAttribute('data-mark-shape'));
    expect(smallMark.textContent).toBe(largeMark.textContent);
    expect(smallMark.style.width).toBe('16px');
    expect(largeMark.style.width).toBe('36px');
  });

  it('draws an image avatar, labelled unless decorative', () => {
    const bot = { bot_id: 'bot-1', name: 'Scout', avatar: { kind: 'image', url: 'https://x/a.png' } };
    const { rerender } = render(<BotMark bot={bot} />);
    const image = screen.getByRole('img', { name: 'Scout' });
    expect(image.tagName).toBe('IMG');
    expect(image).toHaveAttribute('src', 'https://x/a.png');

    rerender(<BotMark bot={bot} decorative />);
    expect(screen.queryByRole('img')).toBeNull();
    expect(screen.getByAltText('')).toHaveAttribute('src', 'https://x/a.png');
  });

  it('draws an emoji avatar', () => {
    render(<BotMark bot={{ bot_id: 'bot-1', name: 'Scout', avatar: { emoji: '🦊' } }} />);
    expect(screen.getByRole('img', { name: 'Scout' })).toHaveTextContent('🦊');
  });

  it('rounds a circle and a squircle with CSS', () => {
    const circle = render(<BotMark bot={{ bot_id: 'b', name: 'S', avatar: { kind: 'mark', shape: 'circle', color: 'blue' } }} size={36} />);
    expect(circle.container.querySelector('[data-bot-mark]')).toHaveClass('rounded-full');
    circle.unmount();

    const squircle = render(<BotMark bot={{ bot_id: 'b', name: 'S', avatar: { kind: 'mark', shape: 'squircle', color: 'blue' } }} size={36} />);
    const mark = squircle.container.querySelector('[data-bot-mark]') as HTMLElement;
    expect(mark).toHaveAttribute('data-mark-shape', 'squircle');
    expect(mark.style.borderRadius).toBe('10px');
  });

  it('draws a hexagon and a blob as inline SVG in the token colour', () => {
    const hex = render(<BotMark bot={{ bot_id: 'b', name: 'S', avatar: { kind: 'mark', shape: 'hex', color: 'violet' } }} />);
    expect(hex.container.querySelector('polygon')).toHaveAttribute('fill', 'var(--color-bot-4)');
    hex.unmount();

    const blob = render(<BotMark bot={{ bot_id: 'b', name: 'S', avatar: { kind: 'mark', shape: 'blob', color: 'moss' } }} />);
    expect(blob.container.querySelector('path')).toHaveAttribute('fill', 'var(--color-bot-6)');
  });

  it('is hidden from assistive tech when decorative', () => {
    const { container } = render(<BotMark bot={{ bot_id: 'bot-1', name: 'Scout' }} decorative />);
    expect(container.querySelector('[data-bot-mark]')).toHaveAttribute('aria-hidden', 'true');
    expect(screen.queryByRole('img')).toBeNull();
  });

  it('draws a mark from a name alone for a sender with no bot row', () => {
    render(<BotMark name="Scout" size={22} />);
    expect(screen.getByRole('img', { name: 'Scout' })).toHaveTextContent('S');
  });

  it('takes a glyph from a CJK or emoji name', () => {
    const cjk = render(<BotMark bot={{ bot_id: 'b', name: '泡泡' }} />);
    expect(cjk.container.querySelector('[data-bot-mark]')).toHaveTextContent('泡');
    cjk.unmount();

    const emoji = render(<BotMark bot={{ bot_id: 'b', name: '🦊 分析师' }} />);
    expect(emoji.container.querySelector('[data-bot-mark]')).toHaveTextContent('🦊');
  });

  it('paints every palette colour from its own token', () => {
    BOT_COLORS.forEach((color) => {
      const { container, unmount } = render(
        <BotMark bot={{ bot_id: 'b', name: 'S', avatar: { kind: 'mark', shape: 'circle', color } }} />,
      );
      const mark = container.querySelector('[data-bot-mark]') as HTMLElement;
      expect(mark).toHaveAttribute('data-mark-color', color);
      expect(mark.getAttribute('style')).toContain(`background-color: ${botColorToken(color)}`);
      unmount();
    });
  });

  it('defaults the label to Bot when the name is empty', () => {
    render(<BotMark bot={{ bot_id: 'bot-1', name: '' }} />);
    expect(screen.getByRole('img', { name: 'Bot' })).toBeInTheDocument();
  });

  it('falls back to the deterministic mark when a stored image fails to load', () => {
    const bot = { bot_id: 'bot-1', name: 'Scout', avatar: { kind: 'image', url: 'https://example.com/dead.png' } };
    const { container } = render(<BotMark bot={bot} />);
    const image = container.querySelector('img');
    expect(image).not.toBeNull();

    fireEvent.error(image as HTMLImageElement);

    // The broken picture becomes the mark the bot would wear with no avatar.
    expect(container.querySelector('img')).toBeNull();
    const mark = container.querySelector('[data-bot-mark]') as HTMLElement;
    expect(mark).toHaveAttribute('data-mark-kind', 'mark');
    expect(mark).toHaveTextContent('S');
    expect(screen.getByRole('img', { name: 'Scout' })).toBe(mark);
  });

  it('never loads an image URL with an unsafe scheme', () => {
    const { container } = render(
      <BotMark bot={{ bot_id: 'bot-1', name: 'Scout', avatar: { kind: 'image', url: 'javascript:alert(1)' } }} />,
    );

    expect(container.querySelector('img')).toBeNull();
    expect(container.querySelector('[data-bot-mark]')).toHaveAttribute('data-mark-kind', 'mark');
  });});
