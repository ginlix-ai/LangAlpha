// @vitest-environment node
/**
 * The deterministic BotMark identity: one palette/shape/glyph derivation, one
 * reader of the open avatar dict. Pinned here because every avatar surface
 * draws from it, and the same bot must look the same everywhere.
 */
import { describe, it, expect } from 'vitest';
import {
  avatarKindOf,
  autoAvatar,
  BOT_COLORS,
  BOT_SHAPES,
  botColorToken,
  botGlyph,
  emojiAvatar,
  fnv1a,
  firstGrapheme,
  imageAvatar,
  isBotColor,
  isBotShape,
  isSafeImageUrl,
  lookOfBot,
  markAvatar,
} from '../botMark';

describe('fnv1a', () => {
  it('is the standard 32-bit FNV-1a', () => {
    expect(fnv1a('')).toBe(2166136261);
    expect(fnv1a('a')).toBe(3826002220);
    expect(fnv1a('foobar')).toBe(3214735720);
  });

  it('spreads ids over the whole unsigned range', () => {
    for (const id of ['bot-1', 'a785c5fe-1', '泡泡', '🦊']) {
      const hash = fnv1a(id);
      expect(hash).toBeGreaterThanOrEqual(0);
      expect(hash).toBeLessThanOrEqual(0xffffffff);
    }
  });
});

describe('lookOfBot with no avatar', () => {
  it('derives the palette and shape from the id, stably', () => {
    const bot = { bot_id: 'bot-1', name: 'Scout', avatar: {} };
    const first = lookOfBot(bot);
    expect(first).toEqual(lookOfBot(bot));
    expect(first).toEqual({
      kind: 'mark',
      color: BOT_COLORS[fnv1a('bot-1') % BOT_COLORS.length],
      shape: BOT_SHAPES[fnv1a('bot-1:shape') % BOT_SHAPES.length],
      glyph: 'S',
    });
  });

  it('gives different bots different marks', () => {
    const looks = ['bot-1', 'bot-2', 'bot-3', 'bot-4', 'bot-5', 'bot-6']
      .map((bot_id) => lookOfBot({ bot_id, name: 'Scout' }));
    expect(new Set(looks.map((look) => `${look.kind}:${'color' in look ? look.color : ''}:${'shape' in look ? look.shape : ''}`)).size)
      .toBeGreaterThan(1);
  });

  it('treats an absent avatar and a null bot the same way', () => {
    expect(lookOfBot({ bot_id: 'bot-1', name: 'Scout' })).toEqual(lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: null }));
    expect(lookOfBot(null).kind).toBe('mark');
  });
});

describe('lookOfBot avatar kinds', () => {
  it('draws an explicit image above anything else', () => {
    expect(lookOfBot({ bot_id: 'b', avatar: { kind: 'image', url: 'https://x/a.png', emoji: '🦊' } }))
      .toEqual({ kind: 'image', url: 'https://x/a.png' });
  });

  it('keeps a legacy { url } working as an image', () => {
    expect(lookOfBot({ bot_id: 'b', avatar: { url: 'https://x/a.png' } }))
      .toEqual({ kind: 'image', url: 'https://x/a.png' });
  });

  it('draws an explicit emoji, and a legacy { emoji }', () => {
    expect(lookOfBot({ bot_id: 'b', avatar: { kind: 'emoji', emoji: '🦊' } })).toEqual({ kind: 'emoji', emoji: '🦊' });
    expect(lookOfBot({ bot_id: 'b', avatar: { emoji: '🦊' } })).toEqual({ kind: 'emoji', emoji: '🦊' });
  });

  it('draws an explicit mark, with a stored glyph kept as written', () => {
    expect(lookOfBot({ bot_id: 'b', name: 'Scout', avatar: { kind: 'mark', shape: 'hex', color: 'teal', glyph: 'z' } }))
      .toEqual({ kind: 'mark', shape: 'hex', color: 'teal', glyph: 'z' });
  });

  it('fills a retired or partial mark from the derived one', () => {
    const derived = lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: {} });
    const look = lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: { kind: 'mark', shape: 'diamond', color: 'pink' } });
    expect(look).toEqual({
      kind: 'mark',
      shape: derived.kind === 'mark' ? derived.shape : undefined,
      color: derived.kind === 'mark' ? derived.color : undefined,
      glyph: 'S',
    });
  });

  it('falls back to the derived mark when a kind names nothing drawable', () => {
    expect(lookOfBot({ bot_id: 'bot-1', avatar: { kind: 'image' } }))
      .toEqual(lookOfBot({ bot_id: 'bot-1', avatar: {} }));
    expect(lookOfBot({ bot_id: 'bot-1', avatar: { kind: 'emoji' } }))
      .toEqual(lookOfBot({ bot_id: 'bot-1', avatar: {} }));
    expect(lookOfBot({ bot_id: 'bot-1', avatar: { kind: 'character', url: '' } }))
      .toEqual(lookOfBot({ bot_id: 'bot-1', avatar: {} }));
  });
});

describe('the glyph', () => {
  it('keeps a CJK character whole', () => {
    expect(botGlyph('泡泡')).toBe('泡');
    expect(firstGrapheme('泡泡')).toBe('泡');
  });

  it('capitalises a Latin initial', () => {
    expect(botGlyph('scout')).toBe('S');
    expect(botGlyph('  rates  ')).toBe('R');
  });

  it('keeps an emoji as one grapheme, joined sequences included', () => {
    expect(botGlyph('🦊 Scout')).toBe('🦊');
    expect(firstGrapheme('👨‍👩‍👧 family')).toBe('👨‍👩‍👧');
  });

  it('falls back to a middot when the name has no glyph', () => {
    expect(botGlyph('')).toBe('·');
    expect(botGlyph('   ')).toBe('·');
    expect(botGlyph(null)).toBe('·');
  });
});

describe('the editor helpers', () => {
  it('reads the explicit kind a stored avatar was saved as', () => {
    expect(avatarKindOf({})).toBe('auto');
    expect(avatarKindOf(null)).toBe('auto');
    expect(avatarKindOf({ url: 'https://x/a.png' })).toBe('image');
    expect(avatarKindOf({ kind: 'image', url: 'https://x/a.png' })).toBe('image');
    expect(avatarKindOf({ emoji: '🦊' })).toBe('emoji');
    expect(avatarKindOf({ kind: 'mark', shape: 'hex', color: 'teal' })).toBe('mark');
  });

  it('keeps keys this UI does not own while replacing the recognised ones', () => {
    const stored = { kind: 'mark', shape: 'hex', color: 'teal', note: 'keep me', emoji: '🦊' };
    expect(autoAvatar(stored)).toEqual({ note: 'keep me' });
    expect(markAvatar('circle', 'rose', 'Z', stored)).toEqual({
      note: 'keep me', kind: 'mark', shape: 'circle', color: 'rose', glyph: 'Z',
    });
    expect(imageAvatar('https://x/b.png', stored)).toEqual({
      note: 'keep me', kind: 'image', url: 'https://x/b.png',
    });
    expect(emojiAvatar('🐢', stored)).toEqual({
      note: 'keep me', kind: 'emoji', emoji: '🐢',
    });
  });

  it('names the token a colour paints with, in palette order', () => {
    expect(botColorToken('amber')).toBe('var(--color-bot-1)');
    expect(botColorToken('slate')).toBe('var(--color-bot-8)');
  });

  it('validates palette and shape names against the lists', () => {
    for (const color of BOT_COLORS) expect(isBotColor(color)).toBe(true);
    for (const shape of BOT_SHAPES) expect(isBotShape(shape)).toBe(true);
    expect(isBotColor('pink')).toBe(false);
    expect(isBotShape('diamond')).toBe(false);
  });

describe('image URL safety', () => {
  it('accepts http(s) and inline raster images only', () => {
    expect(isSafeImageUrl('https://example.com/a.png')).toBe(true);
    expect(isSafeImageUrl('http://example.com/a.png')).toBe(true);
    expect(isSafeImageUrl('data:image/png;base64,AAAA')).toBe(true);
    expect(isSafeImageUrl('data:image/svg+xml,<svg/>')).toBe(true);
    expect(isSafeImageUrl('')).toBe(false);
    expect(isSafeImageUrl('   ')).toBe(false);
    expect(isSafeImageUrl('/uploads/a.png')).toBe(false);
    expect(isSafeImageUrl('javascript:alert(1)')).toBe(false);
    expect(isSafeImageUrl('file:///etc/passwd')).toBe(false);
    expect(isSafeImageUrl('data:text/html,<script/>')).toBe(false);
  });

  it('draws the derived mark for a stored unsafe or empty image URL', () => {
    const derived = lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: {} });
    expect(lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: { kind: 'image', url: 'javascript:alert(1)' } }))
      .toEqual(derived);
    expect(lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: { url: '  ' } })).toEqual(derived);
    expect(lookOfBot({ bot_id: 'bot-1', name: 'Scout', avatar: { url: 'https://example.com/a.png' } }))
      .toEqual({ kind: 'image', url: 'https://example.com/a.png' });
  });
});});
