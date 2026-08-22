import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Upload } from 'lucide-react';
import {
  validatePluginSourceUrl,
  validatePluginZip,
} from '../utils/pluginSchemas';

/**
 * Step 1 of the install wizard: a zip dropzone or a public git https URL.
 * Whichever was touched last wins; submit is gated on one being valid. Deep
 * validation is server-side — this step refuses only what is certain to fail
 * (wrong extension, over the size cap, a URL the SSRF policy will reject).
 */

export function PluginSourceStep({
  onSubmit,
  busy,
}: {
  onSubmit: (source: { file: File } | { url: string }) => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  const inputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [url, setUrl] = useState('');
  const [dragOver, setDragOver] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function pick(f: File | null | undefined) {
    setError(null);
    if (!f) return;
    const reason = validatePluginZip(f);
    if (reason) {
      setError(t(`plugins.install.${reason}`));
      return;
    }
    setFile(f);
    setUrl('');
  }

  const trimmedUrl = url.trim();
  const canSubmit = !busy && (!!file || !!trimmedUrl);

  function submit() {
    if (!canSubmit) return;
    if (file) {
      onSubmit({ file });
      return;
    }
    const reason = validatePluginSourceUrl(trimmedUrl);
    if (reason) {
      setError(reason);
      return;
    }
    onSubmit({ url: trimmedUrl });
  }

  return (
    <div className="flex flex-col gap-3">
      <div
        role="button"
        tabIndex={0}
        onClick={() => inputRef.current?.click()}
        onKeyDown={(e) => {
          if (e.key !== 'Enter' && e.key !== ' ') return;
          e.preventDefault();
          inputRef.current?.click();
        }}
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragOver(false);
          pick(e.dataTransfer.files?.[0]);
        }}
        className="flex flex-col items-center justify-center gap-2 rounded-md py-8 px-4 cursor-pointer text-center"
        style={{
          border: `1px dashed ${dragOver ? 'var(--color-accent-primary)' : 'var(--color-border-muted)'}`,
          color: 'var(--color-text-tertiary)',
        }}
      >
        <Upload className="h-5 w-5" />
        <span className="text-xs">
          {file ? file.name : t('plugins.install.sourceZip')}
        </span>
        <input
          ref={inputRef}
          type="file"
          accept=".zip,application/zip"
          className="hidden"
          tabIndex={-1}
          onChange={(e) => pick(e.target.files?.[0])}
        />
      </div>

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
            setError(null);
            setUrl(e.target.value);
            if (e.target.value.trim()) setFile(null);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter') submit();
          }}
          placeholder="https://github.com/owner/repo"
          spellCheck={false}
          className="text-xs px-2 py-1.5 rounded-md outline-none"
          style={{
            color: 'var(--color-text-primary)',
            backgroundColor: 'var(--color-bg-input)',
            border: '1px solid var(--color-border-muted)',
          }}
        />
      </div>

      {error && (
        <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
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
