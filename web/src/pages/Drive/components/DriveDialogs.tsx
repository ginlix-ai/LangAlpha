import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { Folder, HardDrive } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { useDriveFolders } from '@/hooks/useDrive';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import type { DriveEntry } from '@/types/drive';
import { isWithin, parentOf, sortEntries } from '../drivePaths';
import type { NameResult } from '../useDriveActions';
// The page's ground, as ConfirmDialog's `surface="page"` draws it.
import '@/pages/Dashboard/components/dashForms.css';

const SURFACE = 'sm:max-w-md border dash-form';

/** What the name dialog is asked for: a new folder in `dir`, or a new name for `entry`. */
export type NameAsk = { kind: 'folder'; dir: string } | { kind: 'rename'; entry: DriveEntry };

const NAME_COPY: Record<NameAsk['kind'], { title: string; body: string; placeholder: string | null; submit: string }> = {
  rename: { title: 'drive:actions.rename', body: 'drive:rename.description', placeholder: null, submit: 'drive:actions.rename' },
  folder: { title: 'drive:actions.newFolder', body: 'drive:newFolder.description', placeholder: 'drive:newFolder.placeholder', submit: 'drive:newFolder.submit' },
};

/**
 * A name, for a new folder or a rename. On a rename the name's stem is
 * selected, so typing replaces it and keeps the extension.
 */
export function NameDialog({ ask, onClose, onSubmit }: {
  ask: NameAsk | null;
  onClose: () => void;
  /** A refusal with a `field` is shown under the field; any other has had its toast. */
  onSubmit: (name: string) => Promise<NameResult>;
}) {
  const { t } = useTranslation();
  // Keyed by the caller per ask, so the draft starts from the ask it is for.
  const [draft, setDraft] = useState(ask?.kind === 'rename' ? ask.entry.name : '');
  const [busy, setBusy] = useState(false);
  // The server's refusal of the last name sent, until the name changes.
  const [refused, setRefused] = useState<{ name: string; message: string } | null>(null);
  const trimmed = draft.trim();
  const unchanged = ask?.kind === 'rename' && trimmed === ask.entry.name;
  const valid = !!trimmed && !trimmed.includes('/') && trimmed !== '.' && trimmed !== '..';
  const submit = async () => {
    if (!valid || busy) return;
    if (unchanged) {
      onClose();
      return;
    }
    setBusy(true);
    const result = await onSubmit(trimmed);
    setBusy(false);
    if (result.ok) onClose();
    else if (result.field) setRefused({ name: trimmed, message: result.field });
  };
  const fieldError = trimmed.includes('/')
    ? t('drive:errors.slash')
    : refused?.name === trimmed
      ? refused.message
      : null;
  const copy = ask ? NAME_COPY[ask.kind] : NAME_COPY.folder;
  const title = t(copy.title);
  const description = t(copy.body);
  return (
    <Dialog open={!!ask} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent className={SURFACE}>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <Input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={copy.placeholder ? t(copy.placeholder) : undefined}
          aria-label={title}
          aria-invalid={!!fieldError || undefined}
          aria-describedby={fieldError ? 'drive-name-error' : undefined}
          autoFocus
          onFocus={(e) => {
            if (ask?.kind !== 'rename' || ask.entry.kind === 'folder') return;
            const dot = e.target.value.lastIndexOf('.');
            if (dot > 0) e.target.setSelectionRange(0, dot);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !isImeComposing(e)) {
              e.preventDefault();
              void submit();
            }
          }}
        />
        {fieldError && (
          <p id="drive-name-error" role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>{fieldError}</p>
        )}
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>{t('common.cancel')}</Button>
          <Button onClick={() => void submit()} disabled={!valid || busy || refused?.name === trimmed}>
            {busy ? t('common.saving') : t(copy.submit)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

interface FolderNode {
  path: string;
  name: string;
  depth: number;
}

/** The folders as an indented list, each under its parent, sorted by name. */
function folderRows(folders: DriveEntry[], locale: string): FolderNode[] {
  const byParent = new Map<string, DriveEntry[]>();
  for (const f of folders) {
    const parent = parentOf(f.path);
    byParent.set(parent, [...(byParent.get(parent) ?? []), f]);
  }
  const out: FolderNode[] = [];
  const walk = (parent: string, depth: number) => {
    for (const f of sortEntries(byParent.get(parent) ?? [], 'name', locale)) {
      out.push({ path: f.path, name: f.name, depth });
      walk(f.path, depth + 1);
    }
  };
  walk('', 1);
  return out;
}

/** Pick a folder to move an entry into. A folder cannot go inside itself, so it and its own folders are left out. */
export function MoveDialog({ space, entry, onClose, onMove }: {
  space: string | null;
  entry: DriveEntry | null;
  onClose: () => void;
  onMove: (entry: DriveEntry, folder: string) => Promise<NameResult>;
}) {
  const { t } = useTranslation();
  const { data: folders, isLoading, error } = useDriveFolders(space, !!entry);
  const [picked, setPicked] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const current = entry ? parentOf(entry.path) : '';
  const locale = useLocale();
  const rows = folderRows(folders ?? [], locale).filter((f) => !(entry?.kind === 'folder' && isWithin(f.path, entry.path)));
  const target = picked ?? current;
  const submit = async () => {
    if (!entry || target === current) return;
    setBusy(true);
    const moved = await onMove(entry, target);
    setBusy(false);
    if (moved.ok) onClose();
  };
  const row = (path: string, name: string, depth: number, root = false) => {
    const Icon = root ? HardDrive : Folder;
    const here = path === current;
    return (
      <button
        key={path || '/'}
        type="button"
        role="option"
        aria-selected={target === path}
        onClick={() => setPicked(path)}
        className={cn('drive-move-row', target === path && 'is-picked')}
        style={{ paddingLeft: 8 + depth * 14 }}
      >
        <Icon className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} aria-hidden="true" />
        <span className="truncate">{name}</span>
        {here && <span className="drive-move-here">{t('drive:move.here')}</span>}
      </button>
    );
  };
  return (
    <Dialog open={!!entry} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent className={SURFACE}>
        <DialogHeader>
          <DialogTitle>{t('drive:move.title', { name: entry?.name ?? '' })}</DialogTitle>
          <DialogDescription>{t('drive:move.description')}</DialogDescription>
        </DialogHeader>
        <div className="drive-move-list" role="listbox" aria-label={t('drive:move.folders')}>
          {row('', t('drive:title'), 0, true)}
          {isLoading && <p className="drive-move-note">{t('common.loading')}</p>}
          {error && <p className="drive-move-note">{t('drive:errors.loadFailed')}</p>}
          {rows.map((f) => row(f.path, f.name, f.depth))}
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>{t('common.cancel')}</Button>
          <Button onClick={() => void submit()} disabled={busy || target === current}>
            {busy ? t('drive:move.moving') : t('drive:move.submit')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
