import { useState } from 'react';
import { Share2 } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { cn } from '@/lib/utils';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useThreadShareStatus } from '../hooks/useThreadShareStatus';
import { ThreadSharePanel } from './ThreadSharePanel';

interface ShareButtonProps {
  threadId: string;
  initialIsShared?: boolean;
  /** The workspace whose shared files the popover lists; null hides the list. */
  workspaceId?: string | null;
}

/**
 * ShareButton -- Toggle public sharing for a thread, with permission controls.
 * Shows a popover with share toggle, URL copy, and permission checkboxes.
 */
export default function ShareButton({ threadId, initialIsShared = false, workspaceId = null }: ShareButtonProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const [open, setOpen] = useState(false);
  // The thread's cached status, shared with every share panel; the thread
  // load's answer stands in until it arrives.
  const { data } = useThreadShareStatus(threadId);
  const isShared = data ? data.is_shared === true : initialIsShared;

  // Radix owns dismissal: Escape and an outside press close it, and a keyboard
  // close hands focus back to the trigger.
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        {/* Sized as the files toggle beside it is: the panel's own button
            size on desktop, a touch target on mobile. */}
        <button
          type="button"
          className={cn('transition-colors', isMobile ? 'p-2 rounded-md' : 'p-1.5 rounded-[4px]')}
          style={{
            color: isShared ? 'var(--color-accent-primary)' : 'var(--color-text-primary)',
            backgroundColor: open ? 'var(--color-border-muted)' : undefined,
          }}
          title={t('share.shareConversation')}
          onMouseEnter={(e) => { if (!open) e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
          onMouseLeave={(e) => { if (!open) e.currentTarget.style.backgroundColor = ''; }}
        >
          <Share2 className={isMobile ? 'h-5 w-5' : 'h-4 w-4'} />
        </button>
      </PopoverTrigger>

      <PopoverContent
        align="end"
        sideOffset={8}
        className="w-80 p-0 rounded-lg shadow-lg"
        style={{
          backgroundColor: 'var(--color-bg-elevated, var(--color-bg-card))',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        <ThreadSharePanel
          key={threadId}
          threadId={threadId}
          workspaceId={workspaceId}
        />
      </PopoverContent>
    </Popover>
  );
}
