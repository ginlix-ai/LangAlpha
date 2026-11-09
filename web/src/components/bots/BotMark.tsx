/**
 * One bot's face, on every surface and at every size.
 *
 * The identity itself (image / emoji / deterministic mark, palette, shape and
 * glyph) comes from `lib/botMark`, so the same bot is drawn identically in the
 * chat header, a message row, the Bots list and editor preview, a room member
 * chip, the room log and an automation's target picker. Quiet by design: no
 * animation, a solid fill and the design tokens' white glyph ink.
 */
import React, { useState, type CSSProperties } from 'react';
import { botColorToken, lookOfBot, type BotIdentity } from '@/lib/botMark';
import { cn } from '@/lib/utils';

export interface BotMarkProps {
  /** The bot row; when absent, `name` alone seeds a derived mark. */
  bot?: BotIdentity | null;
  /** A name for a sender with no bot row (the user, the system, a legacy row). */
  name?: string;
  /** Rendered edge in px: 16 (chat header, room log), 20 (room chips),
   *  22 (bot message row), 36 (Bots list and editor preview). */
  size?: number;
  /** The name is written beside it, so screen readers skip the mark. */
  decorative?: boolean;
}

/** The glyph's type size, proportional but legible at the 16px mark. */
function glyphSize(size: number): number {
  return Math.max(8, Math.round(size * 0.44));
}

/** The box every kind fills; `flexShrink` keeps it whole in a truncating row. */
function boxStyle(size: number): CSSProperties {
  return { width: size, height: size, flexShrink: 0 };
}

const HEX_POINTS = '16,1 29,8.5 29,23.5 16,31 3,23.5 3,8.5';
const BLOB_PATH =
  'M16.3 1.6c5.9.1 10.9 3 12.9 7.7 1.9 4.5.6 9.6-3 12.9-3.6 3.3-8.9 5-13.8 3.8-4.8-1.2-8.6-4.9-9.8-9.2-1.2-4.4.3-8.8 3.5-11.7 2.9-2.6 6.6-3.6 10.2-3.5z';

function Glyph({ size, children }: { size: number; children: React.ReactNode }): React.ReactElement {
  return (
    <span
      aria-hidden
      className="relative inline-flex select-none items-center justify-center font-semibold leading-none"
      style={{ color: 'var(--color-bot-glyph)', fontSize: `${glyphSize(size)}px` }}
    >
      {children}
    </span>
  );
}

export function BotMark({ bot, name, size = 22, decorative = false }: BotMarkProps): React.ReactElement {
  const identity = bot ?? null;
  const label = (identity?.name ?? name ?? '').trim();
  // A stored image can be dead (a moved upload, an expired link). The mark
  // falls back to the deterministic one the bot would wear with no avatar,
  // once per URL, rather than showing a broken image.
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const stored = lookOfBot(identity ? { ...identity } : { name: label });
  const look = stored.kind === 'image' && failedUrl === stored.url
    ? lookOfBot({ ...(identity ?? {}), avatar: {} })
    : stored;
  // The image's alt carries the name; every other kind names a wrapper.
  const a11y = decorative ? { 'aria-hidden': true as const } : { role: 'img' as const, 'aria-label': label || 'Bot' };
  const box = boxStyle(size);
  const data = { 'data-bot-mark': '', 'data-mark-kind': look.kind } as const;

  if (look.kind === 'image') {
    return (
      <img
        {...data}
        src={look.url}
        alt={decorative ? '' : label}
        className="shrink-0 rounded-full object-cover"
        style={box}
        onError={() => setFailedUrl(look.url)}
      />
    );
  }

  if (look.kind === 'emoji') {
    return (
      <span
        {...data}
        {...a11y}
        className="inline-flex shrink-0 items-center justify-center"
        style={{ ...box, fontSize: `${Math.round(size * 0.72)}px`, lineHeight: 1 }}
      >
        {look.emoji}
      </span>
    );
  }

  const fill = botColorToken(look.color);
  const shapeData = { 'data-mark-shape': look.shape, 'data-mark-color': look.color } as const;

  if (look.shape === 'hex' || look.shape === 'blob') {
    return (
      <span
        {...data}
        {...shapeData}
        {...a11y}
        className="relative inline-flex shrink-0 items-center justify-center"
        style={box}
      >
        <svg
          aria-hidden
          focusable="false"
          viewBox="0 0 32 32"
          className="absolute inset-0 h-full w-full"
        >
          {look.shape === 'hex'
            ? <polygon points={HEX_POINTS} fill={fill} />
            : <path d={BLOB_PATH} fill={fill} />}
        </svg>
        <Glyph size={size}>{look.glyph}</Glyph>
      </span>
    );
  }

  return (
    <span
      {...data}
      {...shapeData}
      {...a11y}
      className={cn('inline-flex shrink-0 items-center justify-center', look.shape === 'circle' ? 'rounded-full' : '')}
      style={{
        ...box,
        backgroundColor: fill,
        ...(look.shape === 'squircle' ? { borderRadius: `${Math.round(size * 0.28)}px` } : {}),
      }}
    >
      <Glyph size={size}>{look.glyph}</Glyph>
    </span>
  );
}
