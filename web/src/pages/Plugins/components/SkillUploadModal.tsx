import { useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Upload } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';

/**
 * Single-skill zip upload: a SKILL.md at the archive root (or in one top-level
 * directory). Validation is server-side — name rules, caps, reserved names —
 * so the modal surfaces the backend's message verbatim on failure and gates
 * only on what is cheap to check before spending the upload.
 */

// Courtesy pre-flight so an oversized file is not uploaded just to be
// rejected. The server's MAX_SKILL_ARCHIVE_BYTES is authoritative; if the two
// ever disagree the upload simply fails a step later with the server's reason.
const MAX_ZIP_BYTES = 2 * 1024 * 1024;

const NOOP = () => {};

export function SkillUploadModal({
  onClose,
  onUpload,
}: {
  onClose: () => void;
  onUpload: (file: File, onProgress: (percent: number) => void) => Promise<unknown>;
}) {
  const { t } = useTranslation();
  const titleId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  // Every dismissal route waits out the upload. It would finish regardless, so
  // closing early saves nothing and only hides the reason if it fails.
  const close = uploading ? NOOP : onClose;
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);

  function pick(f: File | null | undefined) {
    setError(null);
    if (!f) return;
    if (!f.name.toLowerCase().endsWith('.zip')) {
      setError(t('plugins.skills.uploadNotZip'));
      return;
    }
    if (f.size > MAX_ZIP_BYTES) {
      setError(t('plugins.skills.uploadTooLarge'));
      return;
    }
    setFile(f);
  }

  async function handleUpload() {
    if (!file || uploading) return;
    setUploading(true);
    setError(null);
    try {
      await onUpload(file, setProgress);
      onClose();
    } catch (err) {
      setError(formatApiErrorDetail(err));
      setUploading(false);
      setProgress(0);
    }
  }

  const footer = (
    <div className="flex justify-end gap-2">
      <button
        type="button"
        onClick={close}
        disabled={uploading}
        className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('common.cancel')}
      </button>
      <button
        type="button"
        onClick={handleUpload}
        disabled={!file || uploading}
        className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
        style={{
          color: 'var(--color-btn-primary-text)',
          backgroundColor: 'var(--color-btn-primary-bg)',
        }}
      >
        {uploading && <Loader size={12} className="text-current" />}
        {uploading
          ? t('plugins.skills.uploading', { percent: progress })
          : t('plugins.skills.uploadConfirm')}
      </button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('plugins.skills.uploadTitle')}
      subtitle={t('plugins.skills.uploadHint')}
      onClose={onClose}
      closeDisabled={uploading}
      width="narrow"
      footer={footer}
    >
        <div
          role="button"
          tabIndex={0}
          onClick={() => inputRef.current?.click()}
          onKeyDown={(e) => {
            if (e.key !== 'Enter' && e.key !== ' ') return;
            e.preventDefault(); // Space on a role=button must not scroll the page
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
            {file ? file.name : t('plugins.skills.uploadDropzone')}
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

        {error && (
          <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
            {error}
          </p>
        )}
    </ModalShell>
  );
}
