import { useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { FileArchive, Upload } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import type { Refusal } from '@/pages/ChatAgent/utils/api/refusal';
import {
  validatePluginSourceUrl,
  validatePluginZip,
} from '../utils/pluginSchemas';
import { useCatalogOn } from '../hooks/useCatalogOn';
import { RefusalBox } from './RefusalView';

const DISCOVER_HREF = '/plugins?tab=plugins&view=discover';

/** What this step produces: an uploaded package, or a URL to fetch one from. */
export type PluginSource = { file: File } | { url: string };

/**
 * Step 1 of the install wizard: a zip dropzone or a public git https URL.
 * Whichever was touched last wins; submit is gated on one being valid. Deep
 * validation is server-side — this step refuses only what is certain to fail
 * (wrong extension, over the size cap, a URL the SSRF policy will reject).
 *
 * A refusal here goes up to the wizard rather than into local state, so this
 * step's own errors and the ones the install attempt came back with are one
 * message in one place instead of two that can both be on screen. `initial`
 * closes the same loop for the input itself: this step is unmounted while the
 * install runs, so a failed attempt hands its source back for the retry rather
 * than leaving an error above an empty field.
 */

export function PluginSourceStep({
  initial,
  onSubmit,
  onError,
  refusal,
  note = null,
  onBrowseDiscover,
}: {
  /** The source to open with, normally the one that just failed. */
  initial: PluginSource | null;
  onSubmit: (source: PluginSource) => void;
  /** Report a refusal (or null to clear the step's error). */
  onError: (message: string | null) => void;
  /** Why this step is open again: its own refusal, or the last attempt's. */
  refusal: Refusal | null;
  /** A neutral word on why the step is open again, where a refusal would go. */
  note?: string | null;
  /** Leaving for Discover: the wizard closes on the way. */
  onBrowseDiscover: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const catalogOn = useCatalogOn();
  const inputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(
    initial && 'file' in initial ? initial.file : null,
  );
  const [url, setUrl] = useState(initial && 'url' in initial ? initial.url : '');
  const [dragOver, setDragOver] = useState(false);

  function pick(f: File | null | undefined) {
    onError(null);
    if (!f) return;
    const reason = validatePluginZip(f);
    if (reason) {
      // Drop the previous pick too. Keeping it leaves the dropzone naming a
      // file the user believes they just replaced, under an error describing
      // a different one, with Install still armed on the old file.
      setFile(null);
      onError(t(`plugins.install.${reason}`));
      return;
    }
    setFile(f);
    setUrl('');
  }

  const trimmedUrl = url.trim();
  const canSubmit = !!file || !!trimmedUrl;

  function submit() {
    if (!canSubmit) return;
    if (file) {
      onSubmit({ file });
      return;
    }
    const reason = validatePluginSourceUrl(trimmedUrl);
    if (reason) {
      onError(t(`plugins.install.${reason}`));
      return;
    }
    onSubmit({ url: trimmedUrl });
  }

  // Dropping a file anywhere on the zip box picks it, the picked one included,
  // so replacing a zip is the same gesture as choosing the first.
  const dropTarget = {
    onDragOver: (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(true);
    },
    onDragLeave: () => setDragOver(false),
    onDrop: (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      pick(e.dataTransfer.files?.[0]);
    },
  };
  const boxBorder = dragOver ? 'var(--color-accent-primary)' : 'var(--color-border-muted)';

  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {catalogOn ? (
          <Trans
            i18nKey="plugins.install.sourceHint"
            // Not `link`: the parser reads that as the void HTML element and
            // leaves the anchor empty beside its text.
            components={{
              discover: (
                <Link
                  to={DISCOVER_HREF}
                  onClick={onBrowseDiscover}
                  className="underline underline-offset-2 transition-colors hover:text-(--color-text-primary)"
                  style={{ color: 'var(--color-text-secondary)' }}
                />
              ),
            }}
          />
        ) : (
          t('plugins.install.sourceHintNoCatalog')
        )}
      </p>

      {file ? (
        <div
          {...dropTarget}
          className="flex flex-col items-center justify-center gap-1 rounded-md py-6 px-4 text-center"
          style={{ border: `1px solid ${boxBorder}`, backgroundColor: 'var(--color-bg-card)' }}
          data-testid="plugin-source-zip"
        >
          <FileArchive className="h-5 w-5 mb-1" style={{ color: 'var(--color-text-tertiary)' }} />
          <span
            className="text-xs font-medium max-w-full truncate"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {file.name}
          </span>
          <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {formatBytes(file.size, locale)}
            {' · '}
            <button
              type="button"
              onClick={() => {
                onError(null);
                setFile(null);
              }}
              aria-label={t('plugins.install.removeZip', { name: file.name })}
              className="underline underline-offset-2 transition-colors hover:text-(--color-text-primary)"
            >
              {t('plugins.install.remove')}
            </button>
          </span>
        </div>
      ) : (
        <div
          role="button"
          tabIndex={0}
          onClick={() => inputRef.current?.click()}
          onKeyDown={(e) => {
            if (e.key !== 'Enter' && e.key !== ' ') return;
            e.preventDefault();
            inputRef.current?.click();
          }}
          {...dropTarget}
          className="flex flex-col items-center justify-center gap-2 rounded-md py-8 px-4 cursor-pointer text-center"
          style={{
            border: `1px dashed ${boxBorder}`,
            color: 'var(--color-text-tertiary)',
          }}
        >
          <Upload className="h-5 w-5" />
          <span className="text-xs">{t('plugins.install.sourceZip')}</span>
        </div>
      )}
      <input
        ref={inputRef}
        type="file"
        accept=".zip,application/zip"
        className="hidden"
        tabIndex={-1}
        onChange={(e) => {
          pick(e.target.files?.[0]);
          // The same file chosen again after a Remove still fires a change.
          e.target.value = '';
        }}
      />

      <div
        className="flex items-center gap-2 text-[0.6875rem]"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <span className="flex-1 border-t" style={{ borderColor: 'var(--color-border-muted)' }} />
        {t('plugins.install.or')}
        <span className="flex-1 border-t" style={{ borderColor: 'var(--color-border-muted)' }} />
      </div>

      <div className="flex flex-col gap-1">
        <label
          className="text-xs"
          style={{ color: 'var(--color-text-secondary)' }}
          htmlFor="plugin-source-url"
        >
          {t('plugins.install.sourceGit')}
        </label>
        <input
          id="plugin-source-url"
          value={url}
          onChange={(e) => {
            onError(null);
            setUrl(e.target.value);
            if (e.target.value.trim()) setFile(null);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter') submit();
          }}
          placeholder="https://github.com/owner/repo"
          spellCheck={false}
          // An example URL in the house placeholder grey reads as a value
          // already filled in; a fainter one reads as the example it is.
          className="text-xs px-2 py-1.5 rounded-md placeholder:text-(--color-text-quaternary)"
          style={{
            color: 'var(--color-text-primary)',
            backgroundColor: 'var(--color-bg-input)',
            border: '1px solid var(--color-border-muted)',
          }}
        />
      </div>

      {refusal ? (
        <RefusalBox refusal={refusal} />
      ) : (
        note && (
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {note}
          </p>
        )
      )}

      <div className="flex justify-end">
        <button
          type="button"
          onClick={submit}
          disabled={!canSubmit}
          className="px-3 py-1.5 text-xs rounded-md disabled:opacity-50"
          style={{
            color: 'var(--color-btn-primary-text)',
            backgroundColor: 'var(--color-btn-primary-bg)',
          }}
        >
          {t('plugins.install.installConfirm')}
        </button>
      </div>
    </div>
  );
}
