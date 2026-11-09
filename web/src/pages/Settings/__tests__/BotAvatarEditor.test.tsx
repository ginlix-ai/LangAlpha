/**
 * The avatar editor's hardening: an image is only saved with a URL this app
 * will load, and a glyph is one grapheme, not a code unit.
 */
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { BotAvatarEditor } from '@/pages/Settings/panels/BotAvatarEditor';

function lastAvatar(onChange: ReturnType<typeof vi.fn>): Record<string, unknown> {
  return onChange.mock.calls.at(-1)?.[0] as Record<string, unknown>;
}

describe('BotAvatarEditor', () => {
  it('does not save an image kind for an empty URL', () => {
    const onChange = vi.fn();
    renderWithProviders(<BotAvatarEditor botId="bot-1" name="Scout" value={{}} onChange={onChange} />);

    fireEvent.click(screen.getByRole('button', { name: 'Image' }));

    expect(lastAvatar(onChange)).toEqual({});
    expect(lastAvatar(onChange)).not.toHaveProperty('kind', 'image');
  });

  it('accepts only http(s) and data:image URLs', () => {
    const onChange = vi.fn();
    renderWithProviders(<BotAvatarEditor botId="bot-1" name="Scout" value={{}} onChange={onChange} />);
    fireEvent.click(screen.getByRole('button', { name: 'Image' }));

    const input = screen.getByLabelText('Image URL');
    fireEvent.change(input, { target: { value: 'javascript:alert(1)' } });
    expect(lastAvatar(onChange)).not.toHaveProperty('kind', 'image');

    fireEvent.change(input, { target: { value: 'file:///etc/passwd' } });
    expect(lastAvatar(onChange)).not.toHaveProperty('kind', 'image');

    fireEvent.change(input, { target: { value: 'https://example.com/a.png' } });
    expect(lastAvatar(onChange)).toMatchObject({ kind: 'image', url: 'https://example.com/a.png' });

    fireEvent.change(input, { target: { value: 'data:image/png;base64,AAAA' } });
    expect(lastAvatar(onChange)).toMatchObject({ kind: 'image' });
  });

  it('keeps a glyph to one grapheme, emoji sequences included', () => {
    const onChange = vi.fn();
    renderWithProviders(
      <BotAvatarEditor
        botId="bot-1"
        name="Scout"
        value={{ kind: 'mark', shape: 'circle', color: 'blue' }}
        onChange={onChange}
      />,
    );

    const glyph = screen.getByLabelText('Glyph (optional)');
    fireEvent.change(glyph, { target: { value: '👨‍👩‍👧' } });
    expect(lastAvatar(onChange)).toMatchObject({ glyph: '👨‍👩‍👧' });

    fireEvent.change(glyph, { target: { value: 'AB' } });
    expect(lastAvatar(onChange)).toMatchObject({ glyph: 'A' });
  });

  it('resets to the automatic mark when asked', () => {
    const onChange = vi.fn();
    renderWithProviders(
      <BotAvatarEditor
        botId="bot-1"
        name="Scout"
        value={{ kind: 'mark', shape: 'hex', color: 'teal', note: 'keep' }}
        onChange={onChange}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Use automatic mark' }));
    // Unknown keys belong to a newer client; only this UI's keys are dropped.
    expect(lastAvatar(onChange)).toEqual({ note: 'keep' });
  });
});
