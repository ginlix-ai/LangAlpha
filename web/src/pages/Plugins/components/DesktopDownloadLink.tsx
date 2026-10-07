import { Monitor } from 'lucide-react';
import type React from 'react';
import { useTranslation } from 'react-i18next';
import { canBeginMcpOAuth } from '@/lib/desktop';
import { RowNote } from './RowNote';

/** The page offers both desktop editions, the hosted one and the one that
 *  connects to a self-hosted server, so one link serves every build. */
const DESKTOP_DOWNLOAD_URL = 'https://langalpha.ai/download';

/**
 * Where to get the desktop app, beside a sentence saying a connection needs it.
 *
 * The click stops at the link because the brokerage row it sits in opens its
 * detail on a click anywhere in its text.
 */
export function DesktopDownloadLink({
  short = false,
  children,
}: {
  short?: boolean;
  /** The link text when it sits inside a translated sentence (a `<Trans>` component). */
  children?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const full = t('plugins.oauth.desktopDownloadFull');
  return (
    <a
      href={DESKTOP_DOWNLOAD_URL}
      target="_blank"
      rel="noopener noreferrer"
      // "Download" alone names nothing out of context; the label keeps the
      // visible word so a voice command still finds it.
      aria-label={short ? full : undefined}
      onClick={(e) => e.stopPropagation()}
      className="underline underline-offset-2 transition-opacity hover:opacity-80"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      {children ?? (short ? t('plugins.oauth.desktopDownload') : full)}
    </a>
  );
}

/**
 * "Needs the desktop app" on a vendor that only accepts a loopback callback.
 *
 * The link sits beside the note rather than inside it: a Connect button points
 * at the note's `id` for the reason it is unavailable, and that reason should
 * not read out a link's label as part of it.
 */
export function NativeOnlyNote({ id }: { id?: string }) {
  const { t } = useTranslation();
  // The link only where this window cannot finish the flow: inside a shell
  // that can, the user already has what it would download.
  const download = !canBeginMcpOAuth();
  return (
    <span className="inline-flex items-center gap-1.5 text-[0.6875rem]">
      <RowNote icon={Monitor} id={id}>
        {t('plugins.oauth.nativeOnlyNote')}
      </RowNote>
      {download && <DesktopDownloadLink short />}
    </span>
  );
}
