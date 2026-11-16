import { useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { ExternalLink, FileText } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import type { PendingVendorTerms } from '@/api/vendorTerms';

export interface VendorTermsDialogProps {
  terms: PendingVendorTerms;
  onAccept: (terms: PendingVendorTerms) => void;
  /** "Not now", or the dialog closed without an answer. */
  onDismiss: (terms: PendingVendorTerms) => void;
}

/**
 * A vendor's Terms of Service, put to the user wherever they are in the app
 * when the agent first needs that vendor. Styled as the agent's command
 * requests are, since the two take turns in the same place.
 */
export default function VendorTermsDialog({ terms, onAccept, onDismiss }: VendorTermsDialogProps) {
  const { t } = useTranslation();
  const notNowRef = useRef<HTMLButtonElement>(null);
  const name = terms.name;

  return (
    <Dialog open onOpenChange={(open) => { if (!open) onDismiss(terms); }}>
      <DialogContent
        data-vendor-terms={terms.vendor}
        className="border sm:max-w-md"
        style={{
          backgroundColor: 'var(--color-shell-float, var(--color-bg-elevated))',
          borderColor: 'var(--color-shell-edge, var(--color-border-elevated))',
        }}
        // It opens unasked, so focus lands on Not now, never on Accept.
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          notNowRef.current?.focus();
        }}
      >
        <DialogHeader className="gap-2 space-y-0 text-left sm:text-left">
          <div className="flex items-start gap-3 pr-6">
            <span
              aria-hidden
              className="mt-[-2px] flex h-7 w-7 shrink-0 items-center justify-center rounded-md"
              style={{ backgroundColor: 'var(--color-bg-subtle)', color: 'var(--color-text-secondary)' }}
            >
              <FileText className="h-4 w-4" />
            </span>
            <DialogTitle className="title-font min-w-0 text-base leading-snug break-words" style={{ color: 'var(--color-text-primary)' }}>
              {t('vendorTerms.title', { name })}
            </DialogTitle>
          </div>
          <DialogDescription className="leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
            {t('vendorTerms.body', { name })}
          </DialogDescription>
        </DialogHeader>

        {/* In the desktop app a new-tab link leaves for the system browser. */}
        <a
          href={terms.terms_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex w-fit items-center gap-1.5 text-sm font-medium text-(--color-text-primary) underline decoration-(--color-border-strong) underline-offset-2 hover:decoration-current"
        >
          {t('vendorTerms.link', { name })}
          <ExternalLink aria-hidden="true" className="h-3.5 w-3.5" />
        </a>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button
            type="button"
            variant="ghost"
            ref={notNowRef}
            onClick={() => onDismiss(terms)}
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('vendorTerms.notNow')}
          </Button>
          <Button type="button" onClick={() => onAccept(terms)}>
            {t('vendorTerms.accept')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
