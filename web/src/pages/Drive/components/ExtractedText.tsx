import { useTranslation } from 'react-i18next';
import { Download } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useDriveVersionText, useTextsApart } from '@/hooks/useDrive';
import { DocumentErrorFallback } from '@/pages/ChatAgent/components/filePanel/fallbacks';
import { useDriveContext } from '../driveContext';
import { DriveLoading } from './DriveLoading';

/** The read that names a version's bytes (the entry for the head, the versions for a past one), asked again when a text disagrees. */
export type TextSource = { refetch: () => Promise<unknown>; dataUpdatedAt: number; isError: boolean };

/**
 * A document the browser cannot draw, shown as the text the server reads out
 * of it, under a line that says what is missing and offers the file itself.
 * The text a memo's changes compare is the same text, so a Word report reads
 * here as it reads in its record.
 */
export function ExtractedText({ entryId, name, seq, sha, source, onDownload }: {
  entryId: string;
  name: string;
  seq: number;
  sha: string;
  source: TextSource;
  onDownload: () => void;
}) {
  const { t } = useTranslation();
  const { space } = useDriveContext();
  const text = useDriveVersionText(space, entryId, name, { seq, sha });
  // A save folded into this version while its text was read: the text is of
  // other bytes than the header names, so it waits for the two to agree.
  const apart = useTextsApart(source, [{ read: text, sha }]);

  // Asking again failed, the source or the text: the wait would never end, so
  // offer the way out, and still never the text of other bytes.
  if (apart && (source.isError || text.isError)) {
    const retry = source.isError ? source.refetch : text.refetch;
    return <DocumentErrorFallback onDownload={onDownload} onRetry={() => void retry()} />;
  }
  if (text.isPending || apart) return <DriveLoading />;
  const found = text.data;
  if (!found || found.status === 'failed' || found.status === 'too_large') {
    // A request that failed may pass on a second try; a document the server could not read will not.
    return <DocumentErrorFallback onDownload={onDownload} onRetry={text.error ? () => void text.refetch() : undefined} />;
  }
  const paragraphs = (found.text ?? '').split(/\n+/).filter((p) => p.trim());
  return (
    <div className="drive-extract">
      <div className="drive-extract-note">
        <span>{t('drive:file.textOnly')}</span>
        <HeaderButton variant="ghost" icon={Download} onClick={onDownload}>{t('drive:actions.download')}</HeaderButton>
      </div>
      <article className="drive-prose drive-extract-text">
        {paragraphs.length === 0
          ? <p className="drive-extract-empty">{t('drive:file.textNone')}</p>
          : paragraphs.map((p, i) => <p key={i}>{p}</p>)}
        {found.truncated && <p className="drive-extract-empty">{t('drive:file.textCut')}</p>}
      </article>
    </div>
  );
}
