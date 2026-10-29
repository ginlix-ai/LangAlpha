import { useId, useRef, useState, type DragEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { ClipboardPaste, FileArchive, FileText, Folder, FolderOpen, Upload } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useSkillLimits, useSkills } from '@/hooks/useSkills';
import { formatBytes, mediumDate, shortMonthDay } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { markJustAdded } from '../hooks/useJustAdded';
import { detailHref } from '../utils/detailParam';
import {
  declaredName,
  findClash,
  listFolder,
  pickedFolder,
  prepareFolder,
  prepareSkillMd,
  prepareZip,
  readDropped,
  skillZipCap,
  type Prepared,
  type PreparedSkill,
} from './skillUpload';
import {
  describeProblem,
  problemFromError,
  wantsExample,
  type UploadProblem,
} from './skillUploadProblems';

/**
 * Adds one skill from a folder, a zip, a single SKILL.md, or SKILL.md pasted
 * as text; each goes up as the zip the server takes (skillUpload.ts prepares
 * it). The server validates. The dialog checks what it can before sending,
 * says what the upload would replace, and reads a refusal back into what to
 * fix (skillUploadProblems.ts).
 */

const NOOP = () => {};

const ICON = { zip: FileArchive, folder: Folder, file: FileText, paste: FileText } as const;

// Not in React's attribute types, and a plain attribute is all it needs.
const FOLDER_PICKER = { webkitdirectory: '' } as object;

// A folder with this many files is far past any skill's file cap; the walk
// stops there and says so, rather than reading a home folder dropped by
// mistake.
const WALK_CAP = 256;

const quietButton =
  'inline-flex items-center gap-1.5 px-2 py-1 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none';

export function SkillUploadModal({
  onClose,
  onUpload,
  workspaceId = null,
}: {
  onClose: () => void;
  /** Resolves to the stored skill, which the dialog names on its way out. */
  onUpload: (file: File, onProgress: (percent: number) => void) => Promise<SkillInfo | undefined>;
  /** Where the skill lands: one workspace, or null for every workspace. */
  workspaceId?: string | null;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow(60_000, false);
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const titleId = useId();
  const pasteId = useId();
  const fileRef = useRef<HTMLInputElement>(null);
  const folderRef = useRef<HTMLInputElement>(null);
  // Picks finish out of order (a folder reads slower than a zip), and only
  // the latest may land.
  const pickSeq = useRef(0);
  const limits = useSkillLimits().data;
  // The Plugins list's own query, so it is usually already cached. It is what
  // tells a new name from a replacement, so Upload waits for it: read as empty,
  // it let an upload replace a skill without a word.
  const { data: skills, isError: skillsFailed } = useSkills({
    includeDisabled: true,
    allScopes: true,
  });

  const [mode, setMode] = useState<'pick' | 'paste'>('pick');
  const [picked, setPicked] = useState<PreparedSkill | null>(null);
  const [reading, setReading] = useState(false);
  const [text, setText] = useState('');
  const [problem, setProblem] = useState<UploadProblem | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  // Every dismissal route waits out the upload. It would finish regardless, so
  // closing early saves nothing and only hides the reason if it fails.
  const close = uploading ? NOOP : onClose;

  const name = mode === 'paste' ? declaredName(text) : (picked?.name ?? null);
  const clash = name && skills ? findClash(name, skills, workspaceId) : null;
  const replacing = clash?.kind === 'own' || clash?.kind === 'plugin';
  // A built-in name is refused whatever else is right, so it is said before
  // the upload rather than after it.
  const shown: UploadProblem | null =
    problem ?? (clash?.kind === 'builtIn' && name ? { kind: 'reserved', name } : null);
  const ready = mode === 'paste' ? text.trim() !== '' : picked !== null;
  // A list that failed does not hold the upload: what it would have said is
  // said as a possibility instead.
  const unchecked = !skills && skillsFailed;
  const canUpload =
    ready && !reading && !uploading && (skills !== undefined || unchecked) && clash?.kind !== 'builtIn';
  const example = `---\nname: earnings-recap\ndescription: ${t('plugins.skillUpload.exampleDescription')}\n---`;

  let notice: string | null = null;
  if (unchecked && ready) {
    notice = name
      ? t('plugins.skillUpload.replaceUnknown', { name })
      : t('plugins.skillUpload.replaceUnknownUnnamed');
  } else if (clash?.kind === 'plugin') {
    notice = clash.skill.plugin_name
      ? t('plugins.skillUpload.replacePlugin', { name: clash.skill.name, plugin: clash.skill.plugin_name })
      : t('plugins.skillUpload.replacePluginUnnamed', { name: clash.skill.name });
  } else if (clash?.kind === 'own') {
    const { name: own, enabled, updated_at: updatedAt } = clash.skill;
    const at = updatedAt ? new Date(updatedAt) : null;
    if (at && !Number.isNaN(at.getTime())) {
      const format = at.getFullYear() === new Date(now).getFullYear() ? shortMonthDay : mediumDate;
      const date = format(at, locale);
      notice = enabled
        ? t('plugins.skillUpload.replaceOwn', { name: own, date })
        : t('plugins.skillUpload.replaceOwnOff', { name: own, date });
    } else {
      notice = enabled
        ? t('plugins.skillUpload.replaceOwnUndated', { name: own })
        : t('plugins.skillUpload.replaceOwnUndatedOff', { name: own });
    }
  }

  async function prepare(work: () => Promise<Prepared>) {
    const seq = ++pickSeq.current;
    setProblem(null);
    // A refused pick clears the one before it: kept, its name sat under the
    // refusal and Upload sent it, so the user got a skill they had replaced.
    setPicked(null);
    setReading(true);
    let result: Prepared;
    try {
      result = await work();
    } catch {
      result = { problem: { kind: 'unreadable' } };
    }
    if (seq !== pickSeq.current) return;
    setReading(false);
    if (result.problem) setProblem(result.problem);
    else setPicked(result.skill);
  }

  function pickFile(file: File | undefined) {
    if (!file) return;
    const lower = file.name.toLowerCase();
    void prepare(async () => {
      if (lower.endsWith('.zip')) return prepareZip(file, limits);
      if (lower.endsWith('.md')) return prepareSkillMd('file', file.name, file, limits);
      return { problem: { kind: 'unsupported' } };
    });
  }

  function pickFolder(files: File[]) {
    if (files.length === 0) return;
    const { label, entries } = pickedFolder(files);
    void prepare(() => prepareFolder(label, entries, limits));
  }

  function drop(e: DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragOver(false);
    if (uploading) return;
    // Entries can only be taken while the event is live.
    const roots = Array.from(e.dataTransfer.items ?? [])
      .map((item) => (item.kind === 'file' ? item.webkitGetAsEntry() : null))
      .filter((entry): entry is FileSystemEntry => entry !== null);
    const first = e.dataTransfer.files?.[0];
    if (roots.length === 0 || (roots.length === 1 && roots[0].isFile)) {
      pickFile(first);
      return;
    }
    // One folder is read as its contents, as the folder picker reads one;
    // several dropped items are taken together as the top of the skill.
    const folder = roots.length === 1 ? (roots[0] as FileSystemDirectoryEntry) : null;
    void prepare(async () => {
      const top = folder ? await listFolder(folder) : roots;
      const { entries, truncated } = await readDropped(top, WALK_CAP);
      return prepareFolder(folder?.name ?? null, entries, limits, truncated);
    });
  }

  function announce(added: string, replaced: boolean) {
    const detail = detailHref({ kind: 'skill', name: added, workspaceId });
    const open = t('plugins.skillUpload.open');
    toast({
      title: replaced
        ? t('plugins.skillUpload.replaced', { name: added })
        : t('plugins.skillUpload.added', { name: added }),
      // A confirmation with a shortcut, not a question: it leaves on its own.
      duration: 6000,
      action: (
        <ToastAction altText={open} onClick={() => navigate(detail)}>
          {open}
        </ToastAction>
      ),
    });
  }

  async function upload() {
    if (!canUpload) return;
    const replaced = replacing;
    setUploading(true);
    setProblem(null);
    let skill = picked;
    if (mode === 'paste') {
      const result = await prepareSkillMd('paste', null, text, limits).catch(
        (): Prepared => ({ problem: { kind: 'unreadable' } }),
      );
      if (result.problem) {
        setProblem(result.problem);
        setUploading(false);
        return;
      }
      skill = result.skill;
    }
    if (!skill) {
      setUploading(false);
      return;
    }
    try {
      const info = await onUpload(skill.upload, setProgress);
      const added = info?.name ?? skill.name;
      if (added) {
        markJustAdded('skill', added);
        announce(added, replaced);
      }
      onClose();
    } catch (err) {
      setProblem(problemFromError(err));
      setUploading(false);
      setProgress(0);
    }
  }

  const meta = picked
    ? [
        picked.name && picked.name !== picked.label ? picked.name : null,
        picked.fileCount !== null ? t('plugins.skillUpload.files', { count: picked.fileCount }) : null,
        formatBytes(picked.upload.size, locale),
      ]
        .filter(Boolean)
        .join(' · ')
    : '';
  const PickedIcon = picked ? ICON[picked.source] : Upload;

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
        onClick={upload}
        disabled={!canUpload}
        className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
        style={{
          color: 'var(--color-btn-primary-text)',
          backgroundColor: 'var(--color-btn-primary-bg)',
        }}
      >
        {uploading && <Loader size={12} className="text-current" />}
        {uploading
          ? replacing
            ? t('plugins.skillUpload.replacing', { percent: progress })
            : t('plugins.skillUpload.uploading', { percent: progress })
          : replacing
            ? t('plugins.skillUpload.replace')
            : t('plugins.skillUpload.upload')}
      </button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('plugins.skillUpload.title')}
      subtitle={
        limits
          ? t(isMobile ? 'plugins.skillUpload.hintTouch' : 'plugins.skillUpload.hint', {
              // Kept on one line: "2" at a line's end and "MB" on the next reads as two things.
              size: formatBytes(skillZipCap(limits), locale).replace(' ', ' '),
            })
          : t(isMobile ? 'plugins.skillUpload.hintTouchUnsized' : 'plugins.skillUpload.hintUnsized')
      }
      onClose={onClose}
      closeDisabled={uploading}
      width="narrow"
      footer={footer}
    >
      {mode === 'pick' ? (
        <div
          role="button"
          tabIndex={0}
          aria-disabled={uploading || undefined}
          data-testid="skill-upload-zone"
          onClick={() => {
            if (!uploading) fileRef.current?.click();
          }}
          onKeyDown={(e) => {
            if (e.key !== 'Enter' && e.key !== ' ') return;
            e.preventDefault(); // Space on a role=button must not scroll the page
            if (!uploading) fileRef.current?.click();
          }}
          onDragOver={(e) => {
            e.preventDefault();
            if (!uploading) setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={drop}
          // Keyboard focus is the accent edge with its halo drawn inside: the
          // zone opens the dialog's body, which scrolls and has no top padding,
          // so anything drawn outside the zone loses its top edge to the clip.
          // Read off the same pointer record that holds every other ring off.
          className={cn(
            'flex flex-col items-center justify-center gap-1.5 rounded-md py-8 px-4 cursor-pointer text-center',
            'border border-dashed focus-visible:outline-hidden',
            dragOver ? 'border-[color:var(--color-accent-primary)]' : 'border-[color:var(--color-border-muted)]',
            '[:root:not([data-pointer-focus])_&:focus-visible]:border-solid [:root:not([data-pointer-focus])_&:focus-visible]:border-[color:var(--color-accent-primary)] [:root:not([data-pointer-focus])_&:focus-visible]:shadow-[inset_0_0_0_1px_var(--color-accent-soft)]',
          )}
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {reading ? (
            <>
              <Loader size={14} className="text-current" />
              <span className="text-xs">{t('plugins.skillUpload.reading')}</span>
            </>
          ) : picked ? (
            <>
              <PickedIcon className="h-5 w-5" />
              <span className="text-xs max-w-full truncate" style={{ color: 'var(--color-text-primary)' }}>
                {picked.label ?? picked.name ?? t('plugins.skillUpload.droppedItems')}
              </span>
              <span className="text-[0.6875rem] max-w-full truncate">{meta}</span>
            </>
          ) : (
            <>
              <Upload className="h-5 w-5" />
              <span className="text-xs">
                {isMobile ? t('plugins.skillUpload.zoneTouch') : t('plugins.skillUpload.zone')}
              </span>
              {!isMobile && (
                <span className="text-[0.6875rem]">{t('plugins.skillUpload.zoneClick')}</span>
              )}
            </>
          )}
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          <label htmlFor={pasteId} className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {t('plugins.skillUpload.pasteLabel')}
          </label>
          <textarea
            id={pasteId}
            value={text}
            onChange={(e) => {
              setText(e.target.value);
              setProblem(null);
            }}
            placeholder={`${example}\n\n${t('plugins.skillUpload.pasteBody')}`}
            rows={10}
            spellCheck={false}
            disabled={uploading}
            autoFocus
            data-testid="skill-upload-paste"
            className="w-full px-3 py-2 text-xs rounded-md bg-transparent font-mono resize-none"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
          />
        </div>
      )}

      {shown ? (
        <div className="flex flex-col gap-2" data-testid="skill-upload-problem">
          <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
            {describeProblem(shown, t, locale)}
          </p>
          {wantsExample(shown) && (
            <pre
              className="text-[0.6875rem] font-mono px-3 py-2 rounded-md overflow-x-auto"
              style={{
                color: 'var(--color-text-secondary)',
                backgroundColor: 'var(--color-bg-card)',
                border: '1px solid var(--color-border-muted)',
              }}
            >
              {example}
            </pre>
          )}
        </div>
      ) : notice ? (
        <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }} data-testid="skill-upload-notice">
          {notice}
        </p>
      ) : null}

      {/* Under what is said about the pick, not above it: the next step a
          refusal names is usually one of these (another folder, or pasting
          the header), and a refusal belongs against the zone it is about. */}
      <div className="flex flex-wrap items-center gap-1 -mx-2" style={{ color: 'var(--color-text-secondary)' }}>
        {mode === 'pick' ? (
          <>
            {/* Phones have no folder picker to open. */}
            {!isMobile && (
              <button
                type="button"
                className={quietButton}
                disabled={uploading}
                onClick={() => folderRef.current?.click()}
              >
                <FolderOpen className="h-3.5 w-3.5" />
                {t('plugins.skillUpload.chooseFolder')}
              </button>
            )}
            <button
              type="button"
              className={quietButton}
              disabled={uploading}
              onClick={() => {
                setMode('paste');
                setProblem(null);
              }}
            >
              <ClipboardPaste className="h-3.5 w-3.5" />
              {t('plugins.skillUpload.paste')}
            </button>
          </>
        ) : (
          <button
            type="button"
            className={quietButton}
            disabled={uploading}
            onClick={() => {
              setMode('pick');
              setProblem(null);
            }}
          >
            <Upload className="h-3.5 w-3.5" />
            {t('plugins.skillUpload.backToFiles')}
          </button>
        )}
      </div>

      <input
        ref={fileRef}
        type="file"
        accept=".zip,.md,application/zip,text/markdown"
        className="hidden"
        tabIndex={-1}
        data-testid="skill-upload-file"
        onChange={(e) => {
          pickFile(e.target.files?.[0]);
          // Cleared so choosing the same file again, once fixed, is a change.
          e.target.value = '';
        }}
      />
      <input
        ref={folderRef}
        type="file"
        {...FOLDER_PICKER}
        className="hidden"
        tabIndex={-1}
        data-testid="skill-upload-folder"
        onChange={(e) => {
          pickFolder(Array.from(e.target.files ?? []));
          e.target.value = '';
        }}
      />
    </ModalShell>
  );
}
