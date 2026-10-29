import type { TFunction } from 'i18next';
import { ApiError } from '@/api/client';
import { formatBytes, integer } from '@/lib/format';
import { apiErrorObject, apiErrorStatus, formatApiErrorDetail } from '@/api/errors';

/**
 * Why a skill upload cannot go ahead, in terms a person can act on.
 *
 * The server names each refusal with a `code` and the values its sentence
 * needs, and the dialog's own pre-flight refusals use the same kinds, so a
 * problem caught before sending reads exactly like the one the server would
 * have sent. A refusal with no code this module knows is shown as the server
 * wrote it.
 */
export type UploadProblem =
  | { kind: 'unsupported' }
  | { kind: 'unreadable' }
  | { kind: 'folderNoSkillMd' }
  | { kind: 'layout' }
  | { kind: 'empty' }
  | { kind: 'notZip' }
  | { kind: 'folderTooBig' }
  | { kind: 'tooManyFiles'; count: number; max: number }
  | { kind: 'fileTooLarge'; file: string; max: number }
  | { kind: 'skillMdTooLarge'; max: number }
  | { kind: 'unpackedTooLarge'; max: number }
  | { kind: 'uploadTooLarge'; size: number | null; max: number }
  | { kind: 'noFrontmatter' }
  | { kind: 'badFrontmatter' }
  | { kind: 'noName' }
  | { kind: 'noDescription' }
  | { kind: 'descriptionTooLong'; max: number }
  | { kind: 'badName' }
  | { kind: 'nameTooLong'; max: number }
  | { kind: 'reserved'; name: string }
  | { kind: 'triggerTaken'; name: string }
  | { kind: 'commandOf'; name: string; other: string }
  | { kind: 'countCap'; max: number }
  | { kind: 'storageCap'; max: number }
  | { kind: 'unsafePath'; path: string }
  | { kind: 'notRegular'; path: string }
  | { kind: 'fileAndDirectory'; path: string }
  | { kind: 'notUtf8' }
  | { kind: 'yamlAnchors' }
  | { kind: 'busy' }
  | { kind: 'network' }
  | { kind: 'server'; text: string };

type Params = Record<string, unknown>;

// The codes the upload route answers with (validate.py, commands.py,
// database/user_skills.py, app/skills.py).
const CODED: Record<string, (p: Params) => UploadProblem> = {
  not_zip: () => ({ kind: 'notZip' }),
  empty: () => ({ kind: 'empty' }),
  layout: () => ({ kind: 'layout' }),
  too_many_files: (p) => ({ kind: 'tooManyFiles', count: Number(p.count), max: Number(p.max) }),
  file_too_large: (p) => ({ kind: 'fileTooLarge', file: String(p.path), max: Number(p.max) }),
  skill_md_too_large: (p) => ({ kind: 'skillMdTooLarge', max: Number(p.max) }),
  unpacked_too_large: (p) => ({ kind: 'unpackedTooLarge', max: Number(p.max) }),
  upload_too_large: (p) => ({
    kind: 'uploadTooLarge',
    size: typeof p.size === 'number' ? p.size : null,
    max: Number(p.max),
  }),
  no_frontmatter: () => ({ kind: 'noFrontmatter' }),
  bad_frontmatter: () => ({ kind: 'badFrontmatter' }),
  no_name: () => ({ kind: 'noName' }),
  no_description: () => ({ kind: 'noDescription' }),
  description_too_long: (p) => ({ kind: 'descriptionTooLong', max: Number(p.max) }),
  bad_name: () => ({ kind: 'badName' }),
  name_too_long: (p) => ({ kind: 'nameTooLong', max: Number(p.max) }),
  yaml_anchors: () => ({ kind: 'yamlAnchors' }),
  not_utf8: () => ({ kind: 'notUtf8' }),
  unsafe_path: (p) => ({ kind: 'unsafePath', path: String(p.path) }),
  not_regular: (p) => ({ kind: 'notRegular', path: String(p.path) }),
  file_and_directory: (p) => ({ kind: 'fileAndDirectory', path: String(p.path) }),
  trigger_reserved: (p) => ({ kind: 'reserved', name: String(p.trigger) }),
  trigger_taken: (p) => ({ kind: 'triggerTaken', name: String(p.trigger) }),
  trigger_is_command: (p) => ({ kind: 'commandOf', name: String(p.trigger), other: String(p.skill) }),
  skill_count_cap: (p) => ({ kind: 'countCap', max: Number(p.max) }),
  skill_storage_cap: (p) => ({ kind: 'storageCap', max: Number(p.max) }),
};

/** A rejected upload call as a problem. */
export function problemFromError(err: unknown): UploadProblem {
  if (err instanceof ApiError && (err.code === 'ERR_NETWORK' || err.code === 'ECONNABORTED')) {
    return { kind: 'network' };
  }
  const detail = apiErrorObject(err);
  const code = detail?.code;
  if (detail && typeof code === 'string' && Object.hasOwn(CODED, code)) return CODED[code](detail);
  // 503: the server could not list the names it reserves; 502: it could not
  // store the archive. Neither is the upload's fault, and the raw texts carry
  // a server path.
  const status = apiErrorStatus(err);
  if (status === 502 || status === 503) return { kind: 'busy' };
  return { kind: 'server', text: formatApiErrorDetail(err) };
}

/** Header problems, which the dialog answers with a short example header. */
export function wantsExample(problem: UploadProblem): boolean {
  return (
    problem.kind === 'noFrontmatter' ||
    problem.kind === 'badFrontmatter' ||
    problem.kind === 'noName' ||
    problem.kind === 'noDescription'
  );
}

/** One sentence (or two) saying what is wrong and what to do about it. */
export function describeProblem(problem: UploadProblem, t: TFunction, locale: string): string {
  const bytes = (n: number) => formatBytes(n, locale);
  switch (problem.kind) {
    case 'unsupported':
      return t('plugins.skillUpload.problem.unsupported');
    case 'unreadable':
      return t('plugins.skillUpload.problem.unreadable');
    case 'folderNoSkillMd':
      return t('plugins.skillUpload.problem.folderNoSkillMd');
    case 'layout':
      return t('plugins.skillUpload.problem.layout');
    case 'empty':
      return t('plugins.skillUpload.problem.empty');
    case 'notZip':
      return t('plugins.skillUpload.problem.notZip');
    case 'folderTooBig':
      return t('plugins.skillUpload.problem.folderTooBig');
    case 'tooManyFiles':
      return t('plugins.skillUpload.problem.tooManyFiles', { files: problem.count, max: problem.max });
    case 'fileTooLarge':
      return t('plugins.skillUpload.problem.fileTooLarge', { file: problem.file, max: bytes(problem.max) });
    case 'skillMdTooLarge':
      return t('plugins.skillUpload.problem.skillMdTooLarge', { max: bytes(problem.max) });
    case 'unpackedTooLarge':
      return t('plugins.skillUpload.problem.unpackedTooLarge', { max: bytes(problem.max) });
    case 'uploadTooLarge':
      return problem.size === null
        ? t('plugins.skillUpload.problem.uploadTooLargeOver', { max: bytes(problem.max) })
        : t('plugins.skillUpload.problem.uploadTooLarge', {
            size: bytes(problem.size),
            max: bytes(problem.max),
          });
    case 'noFrontmatter':
      return t('plugins.skillUpload.problem.noFrontmatter');
    case 'badFrontmatter':
      return t('plugins.skillUpload.problem.badFrontmatter');
    case 'noName':
      return t('plugins.skillUpload.problem.noName');
    case 'noDescription':
      return t('plugins.skillUpload.problem.noDescription');
    case 'descriptionTooLong':
      return t('plugins.skillUpload.problem.descriptionTooLong', { max: integer(problem.max, locale) });
    case 'badName':
      return t('plugins.skillUpload.problem.badName');
    case 'nameTooLong':
      return t('plugins.skillUpload.problem.nameTooLong', { max: problem.max });
    case 'reserved':
      return t('plugins.skillUpload.problem.reserved', { name: problem.name });
    case 'triggerTaken':
      return t('plugins.skillUpload.problem.triggerTaken', { name: problem.name });
    case 'commandOf':
      return t('plugins.skillUpload.problem.commandOf', { name: problem.name, other: problem.other });
    case 'countCap':
      return t('plugins.skillUpload.problem.countCap', { max: problem.max });
    case 'storageCap':
      return t('plugins.skillUpload.problem.storageCap', { max: bytes(problem.max) });
    case 'unsafePath':
      return t('plugins.skillUpload.problem.unsafePath', { path: problem.path });
    case 'notRegular':
      return t('plugins.skillUpload.problem.notRegular', { path: problem.path });
    case 'fileAndDirectory':
      return t('plugins.skillUpload.problem.fileAndDirectory', { path: problem.path });
    case 'notUtf8':
      return t('plugins.skillUpload.problem.notUtf8');
    case 'yamlAnchors':
      return t('plugins.skillUpload.problem.yamlAnchors');
    case 'busy':
      return t('plugins.skillUpload.problem.busy');
    case 'network':
      return t('plugins.skillUpload.problem.network');
    case 'server':
      return problem.text;
  }
}
