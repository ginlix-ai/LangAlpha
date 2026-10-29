import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Copy, ExternalLink, FileDown, Link2, Lock, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { ease, useSceneTime } from './clock';
import { Appear, Loader, Scrim } from './kit';

/* The share controls, as the share scene draws them: the chat header's
   popover that shares a conversation (`ShareButton`), the dialog that shares
   one file on its own link (`ShareLinkDialog`), and the bar over a shared
   file as a visitor opens it (`ShareTopBar`). Sizes are the app's at the
   scenes' scale, about half; the labels are the app's own keys. */

/** The popover's width: the app's w-80 at the scenes' scale. */
export const POPOVER_W = 160;

/** The dialog's width: the app's max-w-lg at the scenes' scale. */
const DIALOG_W = 272;

/** A link as the app prints one: this origin, then the share's path. */
function href(path: string): string {
  const origin = typeof window === 'undefined' ? '' : window.location.origin;
  return `${origin}${path}`;
}

const sm = 'text-[7px] leading-none';
const xs = 'text-[6px] leading-none';

/** The app's switch (`ToggleSwitch`): the accent with its knob at the right
    once on; off, `offFill` (the popover's own switch sits on the muted
    border, the dialog's on the raised page). */
function Switch({ on, offFill }: { on: boolean; offFill: string }) {
  return (
    <span
      className="relative inline-flex h-[10.5px] w-[19px] shrink-0 rounded-full border transition-colors duration-quick"
      style={{
        background: on ? 'var(--color-accent-primary)' : offFill,
        borderColor: on ? 'transparent' : 'var(--color-border-muted)',
      }}
    >
      <span
        className="absolute left-[0.75px] top-[0.75px] h-[8px] w-[8px] rounded-full bg-white shadow-sm transition-transform duration-quick"
        style={{ transform: on ? 'translateX(8.5px)' : undefined }}
      />
    </span>
  );
}

/** A checkbox as the browser draws one, ticked in the accent. */
function CheckBox({ on }: { on: boolean }) {
  return (
    <span
      className="flex h-[6.5px] w-[6.5px] shrink-0 items-center justify-center rounded-[1.5px] border"
      style={
        on
          ? {
              background: 'var(--color-accent-primary)',
              borderColor: 'var(--color-accent-primary)',
            }
          : { borderColor: 'var(--color-text-tertiary)' }
      }
    >
      {on && <Check className="h-[5px] w-[5px] text-white" strokeWidth={4} />}
    </span>
  );
}

/** A link in its well: the read-only box the share controls print it in. */
function LinkWell({ path, mono = false }: { path: string; mono?: boolean }) {
  return (
    <span
      className={cn('flex min-w-0 flex-1 items-center rounded-[3px] px-[6px]', xs)}
      style={{ background: 'var(--color-bg-input)', color: 'var(--color-text-secondary)' }}
    >
      <span className={cn('truncate py-[1px]', mono && 'font-mono')}>{href(path)}</span>
    </span>
  );
}

/** An outline button (`Button` variant outline, size sm). */
function OutlineButton({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return (
    <span
      className={cn(
        'flex h-[17px] shrink-0 items-center gap-[3px] rounded-[3px] border px-[6px] font-medium',
        xs
      )}
      style={{
        background: 'var(--color-bg-page)',
        borderColor: 'var(--color-border-default)',
        color: 'var(--color-text-primary)',
        ...style,
      }}
    >
      {children}
    </span>
  );
}

/**
 * The conversation's share popover (`ShareButton`), open from `at` under the
 * header's share button, its right edge on the button's. Off, it is a title
 * and a switch. Switched on at `on`, it prints the conversation's link with
 * its copy button, the permissions (browsing ticked at `files`, which brings
 * up download beneath it) and who can view it. `copied` swaps the copy
 * button for the tick.
 */
export function SharePopover({
  at,
  on,
  files,
  copied,
  style,
}: {
  at: number;
  on: number;
  files: number;
  copied: number;
  style?: CSSProperties;
}) {
  const { t: tr } = useTranslation();
  const t = useSceneTime();
  const p = ease(t, at, 160);
  const shared = t >= on;
  const browse = t >= files;
  const done = t >= copied;
  return (
    <div
      className="absolute flex origin-top-right flex-col gap-[6px] rounded-[4px] border p-[8px] shadow-md"
      style={{
        width: POPOVER_W,
        background: 'var(--color-bg-elevated, var(--color-bg-card))',
        borderColor: 'var(--color-border-muted)',
        opacity: p,
        transform: `translateY(${(1 - p) * -2}px) scale(${0.95 + 0.05 * p})`,
        ...style,
      }}
    >
      {/* the title, and the switch */}
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-[4px]">
          {shared ? (
            <Link2 className="h-2 w-2" style={{ color: 'var(--color-accent-primary)' }} />
          ) : (
            <Lock className="h-2 w-2" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <span className={cn(sm, 'font-medium')} style={{ color: 'var(--color-text-primary)' }}>
            {shared ? tr('share.publicLinkEnabled') : tr('share.shareConversation')}
          </span>
        </span>
        <Switch on={shared} offFill="var(--color-border-muted)" />
      </div>
      <Appear at={on} className="flex flex-col gap-[6px]">
        {/* the link and its copy button */}
        <div className="flex h-[14px] items-center gap-[4px]">
          <LinkWell path="/s/k7Qm2xVp9LcT" />
          <span
            className="flex h-[14px] w-[14px] shrink-0 items-center justify-center"
            style={{ color: done ? 'var(--color-success)' : 'var(--color-text-secondary)' }}
          >
            {done ? <Check className="h-2 w-2" /> : <Copy className="h-2 w-2" />}
          </span>
        </div>
        {/* what a visitor may do with its files */}
        <div className="flex flex-col gap-[4px] pt-[2px]">
          <span className={xs} style={{ color: 'var(--color-text-tertiary)' }}>
            {tr('share.permissions')}
          </span>
          <span className="flex items-center gap-[4px]">
            <CheckBox on={browse} />
            <span className={xs} style={{ color: 'var(--color-text-secondary)' }}>
              {tr('share.allowFileBrowsing')}
            </span>
          </span>
          <Appear at={files} className="ml-[8px] flex items-center gap-[4px]">
            <CheckBox on={false} />
            <span className={xs} style={{ color: 'var(--color-text-secondary)' }}>
              {tr('share.allowFileDownload')}
            </span>
          </Appear>
        </div>
        <span
          className={cn('pt-[2px]', xs, 'leading-[1.35]')}
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {tr('share.anyoneWithLink')}
        </span>
      </Appear>
    </div>
  );
}

/** A file the link covers: its folder and name, its size, and why it is on
    the list. */
interface Covered {
  dir: string;
  name: string;
  size: string;
  reason: 'entry' | 'page';
}

/** What capex-tracker.html's link covers: the page, and the call data it
    loads. */
const COVERED: Covered[] = [
  { dir: 'results/', name: 'capex-tracker.html', size: '48 KB', reason: 'entry' },
  { dir: 'results/', name: 'capex-calls.csv', size: '21 KB', reason: 'page' },
];

/**
 * One file's share dialog (`ShareLinkDialog`) over the dimmed page, from
 * `at`: its link, minted private; the switch that opens it to anyone with
 * it, flipped at `on`; and the files the link covers, listed once the check
 * of what the page references comes back at `listed`. `copied` is the copy
 * button's press. Shared, the footer offers the visitor's view.
 */
export function ShareDialog({
  at,
  listed,
  on,
  copied,
}: {
  at: number;
  listed: number;
  on: number;
  copied: number;
}) {
  const { t: tr } = useTranslation();
  const t = useSceneTime();
  const p = ease(t, at, 200);
  const shared = t >= on;
  const done = t >= copied;
  const ready = t >= listed;
  return (
    <Scrim className="flex items-center justify-center" opacity={Math.min(p * 1.6, 1)}>
      <div
        className="relative flex flex-col gap-[8px] rounded-[4px] border p-[12px] shadow-lg"
        style={{
          width: DIALOG_W,
          background: 'var(--color-bg-page)',
          borderColor: 'var(--color-border-muted)',
          transform: `scale(${0.96 + 0.04 * p})`,
        }}
      >
        <X
          className="absolute right-[8px] top-[8px] h-2 w-2 opacity-70"
          style={{ color: 'var(--color-text-primary)' }}
        />
        <span
          className="truncate pr-[12px] text-[8px] font-semibold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {tr('shareLink.title', { file: 'capex-tracker.html' })}
        </span>
        <span className={cn(xs, 'leading-[1.35]')} style={{ color: 'var(--color-text-secondary)' }}>
          {tr('shareLink.chatStaysPrivate')}
        </span>
        <div className="flex flex-col gap-[8px] pt-[1px]">
          {/* the link, and its copy button */}
          <div className="flex h-[17px] items-stretch gap-[4px]">
            <LinkWell path="/a/R4nWb8sJq3Ze" mono />
            <OutlineButton>
              {done ? (
                <Check className="h-[7px] w-[7px]" style={{ color: 'var(--color-success)' }} />
              ) : (
                <Copy className="h-[7px] w-[7px]" />
              )}
              {done ? tr('shareLink.copied') : tr('shareLink.copy')}
            </OutlineButton>
          </div>
          {/* the switch */}
          <div
            className="flex items-center justify-between gap-[6px] rounded-[4px] border px-[6px] py-[5px]"
            style={{ background: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
          >
            <span className="flex min-w-0 items-center gap-[4px]">
              {shared ? (
                <Link2
                  className="h-2 w-2 shrink-0"
                  style={{ color: 'var(--color-accent-primary)' }}
                />
              ) : (
                <Lock
                  className="h-2 w-2 shrink-0"
                  style={{ color: 'var(--color-text-tertiary)' }}
                />
              )}
              <span className="flex min-w-0 flex-col gap-[2.5px]">
                <span
                  className={cn(sm, 'font-medium')}
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {tr('shareLink.anyoneWithLink')}
                </span>
                <span className={xs} style={{ color: 'var(--color-text-tertiary)' }}>
                  {shared ? tr('shareLink.sharedHint') : tr('shareLink.privateHint')}
                </span>
              </span>
            </span>
            <Switch on={shared} offFill="var(--color-bg-elevated)" />
          </div>
          {/* the files the link covers */}
          <div className="flex flex-col gap-[3px]">
            <span
              className={cn(xs, 'font-medium')}
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {ready
                ? tr('shareLink.linkCovers', { count: COVERED.length })
                : tr('shareLink.listingFiles')}
            </span>
            <div
              className="flex flex-col divide-y overflow-hidden rounded-[4px] border"
              style={{ borderColor: 'var(--color-border-muted)' }}
            >
              {!ready && (
                <span className="flex h-[25px] items-center justify-center">
                  <Loader className={xs} color="var(--color-text-tertiary)" />
                </span>
              )}
              {COVERED.map((file) => (
                <span
                  key={file.name}
                  className={cn(
                    ready ? 'flex' : 'hidden',
                    'h-[12.5px] items-center gap-[6px] px-[6px]',
                    xs
                  )}
                  style={{ borderColor: 'var(--color-border-subtle)' }}
                >
                  <span className="min-w-0 flex-1 truncate">
                    <span style={{ color: 'var(--color-text-tertiary)' }}>{file.dir}</span>
                    <span style={{ color: 'var(--color-text-primary)' }}>{file.name}</span>
                  </span>
                  <span
                    className="shrink-0 tabular-nums"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    {file.size}
                  </span>
                  <span
                    className="w-[40px] shrink-0 text-right"
                    style={{ color: 'var(--color-text-quaternary)' }}
                  >
                    {tr(`shareLink.reason.${file.reason}`)}
                  </span>
                </span>
              ))}
            </div>
          </div>
          {/* the footer: the visitor's view once shared, and done */}
          <div className="flex items-center justify-between pt-[2px]">
            {shared ? (
              <span
                className={cn('flex items-center gap-[3px]', xs)}
                style={{ color: 'var(--color-text-secondary)' }}
              >
                <ExternalLink className="h-[7px] w-[7px]" />
                {tr('shareLink.viewAsVisitor')}
              </span>
            ) : (
              <span />
            )}
            <OutlineButton>{tr('common.done')}</OutlineButton>
          </div>
        </div>
      </div>
    </Scrim>
  );
}

/** The bar over a shared file as a visitor opens it (`ShareTopBar`): the
    product's name, the file's, and the one action the page offers. */
export function SharedFileBar({ name }: { name: string }) {
  const { t: tr } = useTranslation();
  return (
    <div
      className="flex h-[23px] shrink-0 items-center gap-[4px] border-b px-[8.5px]"
      style={{ background: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}
    >
      <span
        className={cn('title-font shrink-0 font-semibold', sm)}
        style={{ color: 'var(--color-text-primary)' }}
      >
        LangAlpha
      </span>
      <span className={cn('shrink-0', sm)} style={{ color: 'var(--color-text-quaternary)' }}>
        /
      </span>
      <span
        className={cn('min-w-0 flex-1 truncate', sm)}
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {name}
      </span>
      <OutlineButton style={{ height: 16, paddingInline: 5 }}>
        <FileDown className="h-[7px] w-[7px]" />
        {tr('filePanel.saveAsPdf')}
      </OutlineButton>
    </div>
  );
}
