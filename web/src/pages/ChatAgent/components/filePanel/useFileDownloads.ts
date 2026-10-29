import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { categorizeFileError, type FileError } from './fileErrors';

interface DownloadArgs {
  workspaceId: string;
  triggerDownloadFn: (workspaceId: string, filePath: string) => Promise<void>;
  workspaceStatus?: string;
}

/**
 * Saving a workspace file to disk, and where its failure is reported.
 *
 * A save is slow enough that the reader can be looking at another file by the
 * time it fails, so the failure is filed under the path it was started from
 * and only shown while that path is the one on screen. Reporting it anywhere
 * else attributes one file's problem to another.
 */
export function useFileDownloads({ workspaceId, triggerDownloadFn, workspaceStatus }: DownloadArgs) {
  const { t } = useTranslation();
  const [failed, setFailed] = useState<{ path: string; error: FileError } | null>(null);

  const download = useCallback((path: string) => {
    triggerDownloadFn(workspaceId, path).catch((err: unknown) => {
      console.error('[FilePanel] Download failed:', err);
      setFailed({ path, error: categorizeFileError(err, workspaceStatus) });
    });
  }, [triggerDownloadFn, workspaceId, workspaceStatus]);

  /**
   * The same save, offered from inside a viewer's error boundary. Raising the
   * panel's own error here would replace the thing the reader is looking at
   * with a second error, so this failure is reported additively.
   */
  const downloadQuietly = useCallback((path: string) => {
    void triggerDownloadFn(workspaceId, path).catch((err: unknown) => {
      console.error('[FilePanel] Download failed:', err);
      toast({ description: t('filePanel.downloadFailed'), variant: 'destructive' });
    });
  }, [triggerDownloadFn, workspaceId, t]);

  /** Several files is several saves; the browser wants them one at a time. */
  const downloadMany = useCallback(async (paths: string[]) => {
    for (const path of paths) {
      try {
        await triggerDownloadFn(workspaceId, path);
      } catch (err) {
        console.error('[FilePanel] Download failed:', err);
        toast({ description: t('filePanel.downloadFailed'), variant: 'destructive' });
        return;
      }
    }
  }, [triggerDownloadFn, workspaceId, t]);

  /** The failure, but only while the file it belongs to is the one on screen. */
  const errorFor = useCallback(
    (path: string | null) => (failed && failed.path === path ? failed.error : null),
    [failed],
  );

  return { download, downloadQuietly, downloadMany, errorFor, clearError: () => setFailed(null) };
}
