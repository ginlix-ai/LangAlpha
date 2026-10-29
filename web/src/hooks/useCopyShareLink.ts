import { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { copyText } from '@/lib/clipboard';
import { shareLinkHref } from '@/pages/ChatAgent/utils/api/shareLinks';

const COPIED_MS = 2000;

/**
 * Copy an item's `/a/<code>` link, with the code a button shows as copied for
 * a moment. The write starts synchronously in the click: Safari refuses a
 * clipboard write that follows a network await, so a caller copies a link it
 * already holds and disables its button until then.
 */
export function useCopyShareLink(): {
  /** Resolves true once the link is on the clipboard; a failure has already toasted. */
  copy: (code: string, path?: string | null) => Promise<boolean>;
  copiedCode: string | null;
} {
  const { t } = useTranslation();
  const [copiedCode, setCopiedCode] = useState<string | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (timer.current) clearTimeout(timer.current);
  }, []);

  const copy = useCallback(async (code: string, path?: string | null) => {
    try {
      await copyText(shareLinkHref(code, path));
    } catch (err) {
      console.error('[useCopyShareLink] Copy failed:', err);
      toast({ description: t('filePanel.shareLinkFailed'), variant: 'destructive' });
      return false;
    }
    setCopiedCode(code);
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setCopiedCode(null), COPIED_MS);
    return true;
  }, [t]);

  return { copy, copiedCode };
}
