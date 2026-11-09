/**
 * Deterministic bot identity: the palette, shape and glyph a `BotMark` draws.
 *
 * The server keeps `bots.avatar` open (a JSONB column), so this module is the
 * one reader that normalises it. A bot with an explicit image, emoji or mark
 * gets that; a bot with nothing set gets a mark derived from its id, so the
 * same bot is the same face on every surface and size without a DB backfill.
 *
 * Pure, so the derivation is pinned without rendering anything; the component
 * that draws it is `components/bots/BotMark.tsx`.
 */

/** The eight brand colours a mark may take; the index is its token number. */
export const BOT_COLORS = [
  'amber',
  'teal',
  'blue',
  'violet',
  'rose',
  'moss',
  'clay',
  'slate',
] as const;

export type BotColor = (typeof BOT_COLORS)[number];

/** The four mark shapes: two CSS-rounded, two inline SVG. */
export const BOT_SHAPES = ['circle', 'squircle', 'hex', 'blob'] as const;

export type BotShape = (typeof BOT_SHAPES)[number];

/** What a BotMark reads: a bot row, or a bare name for a sender with no row. */
export interface BotIdentity {
  bot_id?: string | null;
  name?: string | null;
  avatar?: Record<string, unknown> | null;
}

/** A normalised identity: exactly one of the three kinds, all fields present. */
export type BotLook =
  | { kind: 'image'; url: string }
  | { kind: 'emoji'; emoji: string }
  | { kind: 'mark'; shape: BotShape; color: BotColor; glyph: string };

/** What the avatar editor starts from: the explicit kind, or the auto mark. */
export type BotAvatarKind = 'auto' | 'image' | 'emoji' | 'mark';

/** The CSS token each colour paints with, by name. Spelled out rather than
 *  built from the index so the token a palette entry paints is greppable, and
 *  `tokens.css` is the single place their values live. */
const BOT_COLOR_TOKEN: Record<BotColor, string> = {
  amber: 'var(--color-bot-1)',
  teal: 'var(--color-bot-2)',
  blue: 'var(--color-bot-3)',
  violet: 'var(--color-bot-4)',
  rose: 'var(--color-bot-5)',
  moss: 'var(--color-bot-6)',
  clay: 'var(--color-bot-7)',
  slate: 'var(--color-bot-8)',
};

/** The CSS token a colour paints with. Kept here so the component, the editor
 *  swatches and any future surface read the palette from one list. */
export function botColorToken(color: BotColor): string {
  return BOT_COLOR_TOKEN[color];
}

/** FNV-1a, 32-bit: the identity hash. Small, stable across platforms, and
 *  deliberately not the app's own hashers — it seeds colours, not keys. */
export function fnv1a(value: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

// A grapheme keeps a CJK character, a Latin letter and an emoji (including a
// joined sequence) whole, which code-point iteration would split.
const segmenter = typeof Intl !== 'undefined' && 'Segmenter' in Intl
  ? new Intl.Segmenter(undefined, { granularity: 'grapheme' })
  : null;

/** The first grapheme of a string, or '' when it holds none. */
export function firstGrapheme(value: string | null | undefined): string {
  const trimmed = (value ?? '').trim();
  if (!trimmed) return '';
  if (segmenter) {
    const first = segmenter.segment(trimmed)[Symbol.iterator]().next();
    return first.done ? '' : String(first.value.segment);
  }
  return Array.from(trimmed)[0] ?? '';
}

/** The glyph a mark draws: the name's first grapheme, a Latin letter
 *  capitalised, fallback `·` when the name has nothing to draw. */
export function botGlyph(name: string | null | undefined): string {
  const grapheme = firstGrapheme(name);
  if (!grapheme) return '·';
  return /^[A-Za-z]$/.test(grapheme) ? grapheme.toUpperCase() : grapheme;
}

export function isBotColor(value: unknown): value is BotColor {
  return typeof value === 'string' && (BOT_COLORS as readonly string[]).includes(value);
}

export function isBotShape(value: unknown): value is BotShape {
  return typeof value === 'string' && (BOT_SHAPES as readonly string[]).includes(value);
}

/** The mark a bot with no avatar set gets, derived from its id alone. */
function derivedMark(bot: BotIdentity | null | undefined): Extract<BotLook, { kind: 'mark' }> {
  const seed = (typeof bot?.bot_id === 'string' && bot.bot_id) || bot?.name || '';
  const color = BOT_COLORS[fnv1a(seed) % BOT_COLORS.length];
  const shape = BOT_SHAPES[fnv1a(`${seed}:shape`) % BOT_SHAPES.length];
  return { kind: 'mark', shape, color, glyph: botGlyph(bot?.name) };
}

/**
 * The mark an avatar's own fields name, with anything missing or unreadable
 * filled from the derived mark: a stored shape the palette retired still draws
 * the colour it has, and the pair always resolves to a drawable mark.
 */
function storedMark(bot: BotIdentity | null | undefined, avatar: Record<string, unknown>): BotLook {
  const derived = derivedMark(bot);
  const glyph = typeof avatar.glyph === 'string' ? firstGrapheme(avatar.glyph) : '';
  return {
    kind: 'mark',
    shape: isBotShape(avatar.shape) ? avatar.shape : derived.shape,
    color: isBotColor(avatar.color) ? avatar.color : derived.color,
    glyph: glyph || derived.glyph,
  };
}

/**
 * The identity a bot draws as. An explicit `kind` wins; a legacy object with
 * no kind names its kind by the field it carries (`{ url }` / `{ emoji }`);
 * anything else draws the id-derived mark.
 */
/**
 * Whether a stored image URL is one this app will load: http(s) or an inline
 * raster image. A `javascript:` or `file:` value in the open avatar dict is
 * never a face, so it falls through to the derived mark instead.
 */
export function isSafeImageUrl(url: string | null | undefined): boolean {
  const trimmed = (url ?? '').trim();
  if (!trimmed) return false;
  return /^https?:\/\/\S+$/i.test(trimmed) || /^data:image\/[a-z0-9.+-]+[\s;,]/i.test(trimmed);
}

export function lookOfBot(bot: BotIdentity | null | undefined): BotLook {
  const avatar = bot?.avatar ?? {};
  const kind = typeof avatar.kind === 'string' ? avatar.kind : null;
  const url = typeof avatar.url === 'string' ? avatar.url.trim() : '';
  const emoji = typeof avatar.emoji === 'string' ? avatar.emoji.trim() : '';

  if (kind === 'image') return isSafeImageUrl(url) ? { kind: 'image', url } : derivedMark(bot);
  if (kind === 'emoji') return emoji ? { kind: 'emoji', emoji } : derivedMark(bot);
  if (kind === 'mark') return storedMark(bot, avatar);
  if (isSafeImageUrl(url)) return { kind: 'image', url };
  if (emoji) return { kind: 'emoji', emoji };
  return derivedMark(bot);
}

/** The explicit kind an avatar was saved as, for the editor to open on. */
export function avatarKindOf(avatar: Record<string, unknown> | null | undefined): BotAvatarKind {
  const kind = avatar?.kind;
  if (kind === 'image' || kind === 'emoji' || kind === 'mark') return kind;
  if (isSafeImageUrl(typeof avatar?.url === 'string' ? avatar.url : null)) return 'image';
  if (typeof avatar?.emoji === 'string' && avatar.emoji.trim()) return 'emoji';
  return 'auto';
}

/** Keys this UI understands. A builder drops them and keeps every other key,
 *  so a field written by a newer client survives an edit here. */
const AVATAR_KEYS = new Set(['kind', 'url', 'emoji', 'shape', 'color', 'glyph']);

function avatarExtras(base: Record<string, unknown> | null | undefined): Record<string, unknown> {
  const extras: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(base ?? {})) {
    if (!AVATAR_KEYS.has(key)) extras[key] = value;
  }
  return extras;
}

/** `{}`: the id-derived mark, with keys this UI does not own preserved. */
export function autoAvatar(base?: Record<string, unknown> | null): Record<string, unknown> {
  return avatarExtras(base);
}

export function imageAvatar(url: string, base?: Record<string, unknown> | null): Record<string, unknown> {
  return { ...avatarExtras(base), kind: 'image', url };
}

export function emojiAvatar(emoji: string, base?: Record<string, unknown> | null): Record<string, unknown> {
  return { ...avatarExtras(base), kind: 'emoji', emoji };
}

export function markAvatar(
  shape: BotShape,
  color: BotColor,
  glyph?: string,
  base?: Record<string, unknown> | null,
): Record<string, unknown> {
  const mark: Record<string, unknown> = { ...avatarExtras(base), kind: 'mark', shape, color };
  const trimmed = (glyph ?? '').trim();
  if (trimmed) mark.glyph = trimmed;
  return mark;
}
