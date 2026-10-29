import { useId, type CSSProperties, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUp, Mic, MousePointer2, Plus } from 'lucide-react';
import { cn } from '@/lib/utils';
import { easeInOut, pressed, SEND_MS, useSceneTime } from './clock';
import './kit.css';
import './scenes.css';

/*
 * The parts every scene is drawn with: the page's cards and composer in the
 * app's own surfaces and type, plus the pieces that move on the scene clock
 * (typing, running steps, a scripted pointer). A scene draws its page at its
 * own size and a `Camera` shows the part a step is about. All data is
 * invented placeholder.
 */
/* eslint-disable react-refresh/only-export-components -- scene kit: small
   components plus the `rv` style helper */

export const rv = (s: number): CSSProperties => ({ '--rv': `${s}s` }) as CSSProperties;

/** This component's own id, usable inside `url(#…)`: React's ids carry
    colons. */
export function useSvgId(): string {
  return useId().replace(/[^a-zA-Z0-9_-]/g, '');
}

/** A move's colour, as the app tints market figures: profit for a change at
    or above zero (or `true`), loss below it; `soft` is the matching wash a
    pill sits on. */
export function signColor(change: number | boolean, soft = false): string {
  const up = typeof change === 'boolean' ? change : change >= 0;
  if (soft) return up ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)';
  return up ? 'var(--color-profit)' : 'var(--color-loss)';
}

/** The transform of a button pressed at `at`, while the press holds. */
export function pressAt(t: number, at: number): string | undefined {
  return pressed(t, at) ? 'scale(0.92)' : undefined;
}

/** Grey skeleton bar standing in for text. */
function Ln({ w, h = 3, c }: { w: string; h?: number; c?: string }) {
  return (
    <span
      className="block rounded-full"
      style={{ width: w, height: h, background: c ?? 'var(--iv-line)' }}
    />
  );
}

/** A small label chip, as the app draws tags: a solid chip, colored text. */
export function Tag({
  children,
  color = 'var(--color-text-secondary)',
}: {
  children: ReactNode;
  color?: string;
}) {
  return (
    <span
      className="w-fit shrink-0 rounded-[4px] px-1 py-0.5 text-[8px] font-medium leading-none"
      style={{ background: 'var(--color-bg-tag)', color }}
    >
      {children}
    </span>
  );
}

/** A card on the page; `active` = in use (held, say). */
export function Region({
  active = false,
  dashed = false,
  delay,
  className,
  style,
  children,
}: {
  active?: boolean;
  dashed?: boolean;
  delay?: number;
  className?: string;
  style?: CSSProperties;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn(
        'intro-region',
        active && 'intro-region--active',
        dashed && 'intro-region--dashed',
        delay != null && 'intro-rv',
        className
      )}
      style={{ ...(delay != null ? rv(delay) : null), ...style }}
    >
      {children}
    </div>
  );
}

/**
 * The chat composer (`ChatInput`), as the app draws it: a rounded card with
 * the text on top and attach / model / mic / send below. `text` replaces the
 * `placeholder` (a `Typed` while the user writes) and wraps, the box growing
 * a line at a time; `focused` gives it the accent edge the real one takes on
 * focus; the send button fills once there is text, or once the message is
 * `ready` without any (a chart selection waiting as a chip), and presses at
 * each time in `sendAt`.
 */
export function InputDock({
  focused = false,
  delay,
  chip,
  text,
  placeholder,
  ready = false,
  sendAt,
}: {
  focused?: boolean;
  delay?: number;
  chip?: ReactNode;
  text?: ReactNode;
  placeholder?: string;
  ready?: boolean;
  sendAt?: number[];
}) {
  const { t: tr } = useTranslation();
  const t = useSceneTime();
  const down = pressed(t, sendAt, SEND_MS);
  const filled = text != null || ready;
  return (
    <div
      className={cn(
        'mt-auto flex flex-col gap-1.5 rounded-[11px] border p-1.5',
        delay != null && 'intro-rv'
      )}
      style={{
        ...(delay != null ? rv(delay) : null),
        background: 'var(--color-bg-card)',
        borderColor: focused ? 'var(--color-accent-primary)' : 'var(--color-border-input)',
      }}
    >
      {chip}
      <div className="flex min-h-2 items-center px-0.5">
        {text == null ? (
          <span
            className="truncate text-[8px] leading-none"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {placeholder ?? tr('chat.placeholderDefault')}
          </span>
        ) : (
          /* the app's composer is a textarea that grows as it fills, so a
             long message wraps onto a new line rather than scrolling; the
             negative margin keeps one line at the placeholder's height */
          <span
            className="-my-px block min-w-0 flex-1 text-[8px] leading-[10px]"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {text}
          </span>
        )}
      </div>
      <div className="flex items-center gap-1.5">
        <Plus className="h-2.5 w-2.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        <span className="ml-auto">
          <Ln w="30px" h={3} c="var(--iv-line-2)" />
        </span>
        <Mic className="h-2.5 w-2.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        <span
          className="flex h-4 w-4 shrink-0 items-center justify-center rounded-md transition-transform duration-press"
          style={{
            background: filled ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-elevated)',
            transform: down ? 'scale(0.85)' : undefined,
          }}
        >
          <ArrowUp
            className="h-2 w-2"
            style={{ color: filled ? 'var(--color-btn-primary-text)' : 'var(--color-icon-muted)' }}
          />
        </span>
      </div>
    </div>
  );
}

/* ── moving parts, all driven by the scene clock ───────────────────────── */

/**
 * Mounts its children once the script reaches `at` ms; they enter with the
 * reveal, or scale in with `pop`. `reserve` holds their space until then, for
 * layouts that should not shift.
 */
export function Appear({
  at,
  pop = false,
  reserve = false,
  className,
  style,
  children,
}: {
  at: number;
  pop?: boolean;
  reserve?: boolean;
  className?: string;
  style?: CSSProperties;
  children?: ReactNode;
}) {
  const shown = useSceneTime() >= at;
  if (!shown && !reserve) return null;
  return (
    <div
      className={cn(shown && (pop ? 'intro-pop' : 'intro-rv'), className)}
      style={{ ...rv(0), ...style, ...(shown ? null : { visibility: 'hidden' }) }}
    >
      {children}
    </div>
  );
}

/** A blinking text caret. */
export function Caret() {
  return <span className="intro-caret" aria-hidden />;
}

/** How many characters of a text typed from `at`, `cps` a second, are in at
    `t`; negative before `at`. */
export function typedCount(t: number, at: number, cps: number): number {
  return Math.floor(((t - at) * cps) / 1000);
}

/** `text` typed out from `at`, `cps` characters a second, caret at the end while it types. */
export function Typed({ text, at, cps = 32 }: { text: string; at: number; cps?: number }) {
  const t = useSceneTime();
  const n = Math.min(Math.max(typedCount(t, at, cps), 0), text.length);
  return (
    <>
      {text.slice(0, n)}
      {t >= at && n < text.length && <Caret />}
    </>
  );
}

/** A field's text typed in place: an invisible copy of the finished text
    holds the line's size, so nothing around it moves while it types. A
    focused field that has not started typing shows the caret. */
export function TypedInPlace({ text, at, focus }: { text: string; at: number; focus: boolean }) {
  const t = useSceneTime();
  return (
    <span className="grid">
      <span className="invisible [grid-area:1/1]">{text}</span>
      <span className="[grid-area:1/1]">
        <Typed text={text} at={at} />
        {focus && t < at && <Caret />}
      </span>
    </span>
  );
}

const LOADER_FRAMES = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏';

/** The app's `Loader`: a braille glyph stepping every 80ms on the scene
    clock. It sits on the surrounding text's line, at that text's size
    unless a caller's `text-[n]` sets its own. */
export function Loader({
  color = 'currentColor',
  className,
}: {
  color?: string;
  className?: string;
}) {
  const t = useSceneTime();
  const frame = Number.isFinite(t) ? Math.floor(t / 80) % LOADER_FRAMES.length : 0;
  return (
    <span className={cn('shrink-0 font-mono', className)} style={{ color }} aria-hidden>
      {LOADER_FRAMES[frame]}
    </span>
  );
}

/**
 * The user's message, right-aligned in the chat column. A message nobody
 * typed here carries its `caption` above it, naming who sent it.
 *
 * `wrap` is a message long enough to wrap, set as the app's bubble sets it:
 * no wider than 80% of the column, its lines 1.5 apart as the chat's
 * `leading-normal` spaces them. Its padding gives back the 2px each line
 * adds above and below its type, so its first line sits where a one-line
 * bubble's does.
 */
export function UserBubble({
  at,
  caption,
  wrap = false,
  className,
  children,
}: {
  at: number;
  caption?: ReactNode;
  wrap?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <Appear at={at} className={cn('flex flex-col items-end gap-1', className)}>
      {caption}
      <div
        className={cn('rounded-md px-1.5', wrap ? 'max-w-[80%] py-0.5' : 'py-1')}
        style={{ background: 'var(--color-bg-elevated)' }}
      >
        <span
          className={cn('block text-[8px]', wrap ? 'leading-[1.5]' : 'leading-none')}
          style={{ color: 'var(--color-text-primary)' }}
        >
          {children}
        </span>
      </div>
    </Appear>
  );
}

/** One waypoint of a scripted pointer: where it is (px in the 400px scene) at `at` ms. */
interface Waypoint {
  at: number;
  x: number;
  y: number;
}

/**
 * One beat of a pointer's script: it travels to (`x`, `y`), getting there
 * at `arrive`; clicks at `click`, if it clicks; and holds still there until
 * `leave`, or moves straight on to the next beat if it has no `leave`.
 */
interface Beat {
  x: number;
  y: number;
  arrive: number;
  click?: number;
  leave?: number;
}

/**
 * A pointer's script as the camera plays it: the waypoints it glides
 * through, when it clicks, when it comes on, and each beat's times by name
 * (`leave` is `arrive` for a beat it moves straight on from), so a scene
 * can time a hover or a drop to the pointer.
 */
export interface PointerScript<K extends string = string> {
  path: Waypoint[];
  clicks: number[];
  enter: number;
  beats: Record<K, Beat & { leave: number }>;
}

/** The script of a pointer that comes on at `from` and plays `beats` in
    order. */
export function pointerScript<K extends string>(
  from: Waypoint,
  beats: Record<K, Beat>
): PointerScript<K> {
  const path = [from];
  const clicks: number[] = [];
  const timed = {} as PointerScript<K>['beats'];
  for (const [name, beat] of Object.entries(beats) as [K, Beat][]) {
    const { x, y, arrive, click, leave = arrive } = beat;
    path.push({ at: arrive, x, y });
    if (leave !== arrive) path.push({ at: leave, x, y });
    if (click != null) clicks.push(click);
    timed[name] = { ...beat, leave };
  }
  return { path, clicks, enter: from.at, beats: timed };
}

/** Where a pointer on `path` is at `t`, so whatever it carries rides along. */
export function pointAt(path: Waypoint[], t: number): { x: number; y: number } {
  for (let i = 0; i < path.length - 1; i++) {
    const a = path[i];
    const b = path[i + 1];
    if (t < b.at) {
      const e = easeInOut(t, a.at, b.at - a.at);
      return { x: a.x + (b.x - a.x) * e, y: a.y + (b.y - a.y) * e };
    }
  }
  const { x, y } = path[path.length - 1];
  return { x, y };
}

/**
 * A pointer gliding between waypoints, pressing at each click. Outlined in
 * the page color, as a system pointer is, so it stays visible over a button
 * of its own color. Place it inside a `relative` box whose origin the
 * waypoints are measured from.
 */
export function Cursor({
  path,
  clicks = [],
  project,
}: {
  path: Waypoint[];
  clicks?: number[];
  /** Maps a waypoint to where it shows, for a pointer over a moving camera. */
  project?: (p: { x: number; y: number }) => { x: number; y: number };
}) {
  const t = useSceneTime();
  if (path.length === 0 || t < path[0].at) return null;
  const at = pointAt(path, t);
  const { x, y } = project ? project(at) : at;
  const down = pressed(t, clicks);
  return (
    <div className="pointer-events-none absolute z-10" style={{ left: x, top: y }}>
      <span
        className="block transition-transform duration-press"
        style={{ transform: down ? 'scale(0.82)' : undefined, transformOrigin: 'top left' }}
      >
        <MousePointer2
          className="intro-rv h-3 w-3"
          strokeWidth={1.5}
          style={{ ...rv(0), color: 'var(--color-bg-page)', fill: 'var(--color-text-primary)' }}
        />
      </span>
    </div>
  );
}

/* ── dialogs and menus ─────────────────────────────────────────────────── */

/**
 * The dim a dialog sits over, filling the page. With `on` it fades in and
 * out on the app's enter motion; a scene that drives the fade itself passes
 * `opacity` instead (its dialog can sit inside, as children, and fade with it).
 */
export function Scrim({
  on = true,
  opacity,
  className,
  children,
}: {
  on?: boolean;
  opacity?: number;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn('absolute inset-0', className)}
      style={{
        background: 'var(--color-bg-overlay-strong)',
        opacity: opacity ?? (on ? 1 : 0),
        transition: opacity == null ? 'opacity var(--motion-enter) var(--ease-out)' : undefined,
      }}
    >
      {children}
    </div>
  );
}

/**
 * A dialog centered on the page, `width` px wide. The centering transform
 * lives on this static wrapper: the dialog's own pop animates `transform`
 * and would clobber a translate on itself.
 */
export function CenteredDialog({ width, children }: { width: number; children: ReactNode }) {
  return (
    <div
      className="absolute left-1/2 top-1/2 -translate-y-1/2"
      style={{ width, marginLeft: -width / 2 }}
    >
      {children}
    </div>
  );
}

/** A dropdown's surface: the popover fill, a hairline, the menu shadow. */
export function MenuSurface({
  className,
  style,
  children,
}: {
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  return (
    <div
      className={cn('rounded-[5px] border p-[3px] shadow-md', className)}
      style={{
        background: 'hsl(var(--popover))',
        borderColor: 'var(--color-border-muted)',
        ...style,
      }}
    >
      {children}
    </div>
  );
}
