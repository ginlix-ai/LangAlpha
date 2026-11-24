import type { TFunction } from 'i18next';
import { driveErrorMessage, driveErrorType } from '@/api/drive';
import { apiErrorStatus } from '@/api/errors';
import { formatBytes } from '@/lib/format';
import type { DriveUsage } from '@/types/drive';

export interface Refusal {
  /** Why a name was refused, for the field it was typed in. */
  field?: string;
  /** What a toast says, or an upload row's line (`description` when there is one). */
  toast: { title: string; description?: string };
}

export interface RefusalContext {
  /** The entry acted on, by name. */
  subject: string;
  /** The title when nothing more particular fits ("Could not move it"). */
  failed: string;
  locale: string;
  /** The space as the refusal was judged against: its limits and the room left. */
  usage: DriveUsage | null;
  /** The bytes the act needed, where it carried a file. */
  size?: number | null;
}

/** The largest file one upload may carry; a server older than uploads in parts names only what one request carries. */
export function uploadLimit(usage: DriveUsage): number {
  return usage.max_upload_bytes ?? usage.max_file_bytes;
}

/**
 * Every refusal the page shows, turned into words in one place, whether the
 * server sent it or the page found it first: a file too large or a space
 * without room reads the same either way, with the numbers. A name refused
 * or in the way carries the server's own sentence (it names the entry in the
 * way, under case folding, or the 1024-byte limit), which the caller shows by
 * the field when there is one.
 */
export function driveRefusal(err: unknown, t: TFunction, ctx: RefusalContext): Refusal {
  const type = driveErrorType(err);
  const name = ctx.subject;
  switch (type) {
    case 'exists':
    case 'is_folder':
    case 'not_folder': {
      const field = driveErrorMessage(err) ?? t('drive:errors.taken', { name });
      return { field, toast: { title: ctx.failed, description: field } };
    }
    case 'invalid_path': {
      const field = driveErrorMessage(err) ?? t('drive:errors.badName', { name });
      return { field, toast: { title: ctx.failed, description: field } };
    }
    case 'not_found':
      return { toast: { title: t('drive:errors.gone') } };
    case 'over_quota': {
      const { usage, size } = ctx;
      if (usage?.max_bytes == null) return { toast: { title: t('drive:upload.full') } };
      const left = formatBytes(Math.max(0, usage.max_bytes - usage.bytes), ctx.locale);
      const max = formatBytes(usage.max_bytes, ctx.locale);
      return {
        toast: {
          title: size
            ? t('drive:errors.noRoomFile', { name, size: formatBytes(size, ctx.locale), left, max })
            : t('drive:errors.noRoom', { name, left, max }),
        },
      };
    }
    case 'too_many_entries':
      return { toast: { title: t('drive:upload.tooMany') } };
    case 'unavailable':
      return { toast: { title: t('drive:errors.busy', { name }) } };
    case 'too_many_uploads':
      return { toast: { title: t('drive:upload.tooManyUploads') } };
    case 'mismatch':
      return { toast: { title: t('drive:upload.mismatch') } };
    default:
      break;
  }
  // A 413 with no Drive body came from something in front of the server,
  // whose limit is its own: the Drive's would name the wrong one.
  if (type === 'too_large' || apiErrorStatus(err) === 413) {
    return {
      toast: {
        title: ctx.usage && type === 'too_large'
          ? t('drive:upload.tooLarge', { limit: formatBytes(uploadLimit(ctx.usage), ctx.locale) })
          : t('drive:upload.tooLargeNoLimit'),
      },
    };
  }
  return { toast: { title: ctx.failed } };
}

/** A refusal as one line, for a place with room for only one (an upload row). */
export function refusalLine(refusal: Refusal): string {
  return refusal.field ?? refusal.toast.description ?? refusal.toast.title;
}
