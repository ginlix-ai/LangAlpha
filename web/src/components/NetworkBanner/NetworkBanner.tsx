import { WifiOff } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNetworkStatus } from '@/hooks/useNetworkStatus';

/**
 * App-wide banner surfacing the browser's offline state.
 *
 * Mounted once in `Main`, because losing the link is not a dashboard concern:
 * a chat turn that stalls mid-stream is the case where the user most needs to
 * know it is their wifi and not the agent.
 *
 * In flow rather than fixed: `.main` is a flex column whose route child is
 * `height:100%`, and flex shrinks that child by the banner's height instead of
 * letting it overflow. Fixed positioning would land the bar on top of the page
 * header and the sidebar logo.
 *
 * Keep the visual treatment subtle. This is informational, not blocking:
 * cached data keeps rendering and an interrupted turn resumes on its own.
 */
export default function NetworkBanner() {
  const { t } = useTranslation();
  const { online } = useNetworkStatus();
  if (online) return null;

  return (
    <div
      role="status"
      aria-live="polite"
      className="relative z-[1020] shrink-0 flex items-center justify-center gap-2 px-4 py-2 text-xs font-medium border-b"
      style={{
        backgroundColor: 'var(--color-warning-soft)',
        color: 'var(--color-warning)',
        // Saturated warning for the divider: borderColor matching the soft
        // background made the `border-b` invisible.
        borderColor: 'var(--color-warning)',
      }}
    >
      <WifiOff size={14} />
      <span>{t('common.networkBanner.offlineMessage')}</span>
    </div>
  );
}
