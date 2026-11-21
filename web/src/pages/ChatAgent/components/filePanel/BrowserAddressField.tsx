import React, { useState } from 'react';
import { Lock } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { isImeComposing } from '@/lib/ime';
import { showAddress } from './browserAddress';

/**
 * The browser's address: the page's own, drawn with the site apart from the rest,
 * until the person holding the page types over it. What they typed stays while it
 * is on its way, and after a refusal, so it can be fixed.
 */
export function BrowserAddressField({
  pageUrl,
  holding,
  steering,
  onOpen,
}: {
  /** The page's address, or '' on a blank page. */
  pageUrl: string;
  /** The person holds the page, so the field invites an address. */
  holding: boolean;
  /** The field takes an address now: held, live, and nothing else under way. */
  steering: boolean;
  /** Resolves to whether the page went there; a refusal is said by the caller. */
  onOpen: (url: string) => Promise<boolean>;
}): React.ReactElement {
  const { t } = useTranslation();
  // What the person is typing, null while the field shows the page's address.
  const [draft, setDraft] = useState<string | null>(null);
  const [opening, setOpening] = useState(false);
  // A draft the browser refused stays to be fixed, whenever focus leaves it.
  const [refused, setRefused] = useState(false);
  const editable = steering && !opening;
  const drawn = draft === null && pageUrl ? showAddress(pageUrl) : null;

  const open = async (url: string) => {
    setOpening(true);
    const went = await onOpen(url);
    setOpening(false);
    setRefused(!went);
    if (went) setDraft(null);
  };

  return (
    <div className="browser-address-box" data-steering={editable || undefined}>
      <input
        type="text"
        inputMode="url"
        className="browser-address font-mono"
        aria-label={t('filePanel.browser.address', 'Address')}
        title={holding ? pageUrl || undefined : t('filePanel.browser.addressLocked', 'Take control to enter an address')}
        placeholder={holding ? t('filePanel.browser.addressPlaceholder', 'Enter an address') : ''}
        readOnly={!editable}
        // The drawn address shows through while nobody is typing.
        data-drawn={drawn ? '' : undefined}
        value={draft ?? pageUrl}
        autoCapitalize="off"
        autoComplete="off"
        autoCorrect="off"
        spellCheck={false}
        onFocus={(e) => {
          if (!editable) return;
          // A refused address stays to be fixed; otherwise typing starts from the page's.
          setDraft((current) => current ?? pageUrl);
          e.currentTarget.select();
        }}
        onChange={(e) => {
          if (!editable) return;
          setDraft(e.target.value);
          setRefused(false);
        }}
        onBlur={() => { if (!opening && !refused) setDraft(null); }}
        onKeyDown={(e) => {
          e.stopPropagation();
          // Enter and Escape belong to an input method while it composes.
          if (isImeComposing(e)) return;
          if (e.key === 'Escape') {
            setDraft(null);
            setRefused(false);
            e.currentTarget.blur();
          } else if (e.key === 'Enter' && editable && draft?.trim()) {
            e.preventDefault();
            void open(draft);
          }
        }}
      />
      {drawn && (
        <span className="browser-address-drawn font-mono" aria-hidden="true">
          {drawn.secure && <Lock className="h-3 w-3 shrink-0" />}
          <span className="browser-address-quiet">{drawn.scheme}</span>
          <span className="browser-address-host">{drawn.host}</span>
          <span className="browser-address-quiet">{drawn.rest}</span>
        </span>
      )}
    </div>
  );
}
