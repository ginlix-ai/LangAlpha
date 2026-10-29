/**
 * Set an SSH computer up again on its machine after a start that failed on
 * its folder: take the folder back from whatever computer holds it now, or
 * move to a new folder. Either way the computer starts once the server
 * accepts it, and the status stream reports the rest.
 *
 * Opened over the Computers dialog, which owns the request; mounted only
 * while open, so every opening starts from the computer's own folder.
 */
import { useId, useState, type FormEvent } from 'react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ModalShell } from '@/components/ui/ModalShell';
import type { Computer, ComputerReinstall } from '@/types/api';

import { StartFailureNote } from './computerParts';
import { RadioCardGroup, type RadioCardOption } from './radioCardGroup';
import { restartRefusal, shownStartFailure, suggestedNewFolder } from './startFailure';

type FolderChoice = ComputerReinstall['folder'];

interface ReinstallDialogProps {
  computer: Computer;
  onClose: () => void;
  onSubmit: (body: ComputerReinstall) => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, as the request rejected. */
  error: unknown;
}

const inputStyle = {
  backgroundColor: 'var(--color-bg-input)',
  borderColor: 'var(--color-border-muted)',
  color: 'var(--color-text-primary)',
} as const;

function ReinstallDialog({ computer, onClose, onSubmit, busy, error }: ReinstallDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const formId = useId();
  const folderId = useId();
  const hintId = useId();
  const failure = shownStartFailure(computer);
  const root = computer.root_dir;
  // A folder another computer holds is the case taking it back is for; a
  // folder that cannot be used at all is more likely left for a new one.
  const [choice, setChoice] = useState<FolderChoice>(failure?.code === 'root_dir' ? 'new' : 'take_back');
  const [newFolder, setNewFolder] = useState(() => suggestedNewFolder(root));
  // The body last sent, so a refusal is read against the folder it was about.
  const [sent, setSent] = useState<ComputerReinstall | null>(null);

  const typed = newFolder.trim();
  // Taken back only from the computer shown holding it: one that marked the
  // folder since is refused, for the user to see before choosing again.
  const takeBack: ComputerReinstall = failure?.owner
    ? { folder: 'take_back', take_over: failure.owner }
    : { folder: 'take_back' };
  const body: ComputerReinstall | null =
    choice === 'take_back' ? takeBack : typed && typed !== root ? { folder: 'new', root_dir: typed } : null;

  const send = (next: ComputerReinstall) => {
    setSent(next);
    onSubmit(next);
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (busy || !body) return;
    send(body);
  };

  const refusal = error && !busy ? restartRefusal(error, t, 'reinstall') : null;
  // Taking the new folder is offered only while it is still the one refused.
  const takeNew =
    refusal?.takeOver && sent?.folder === 'new' && choice === 'new' && sent.root_dir === typed
      ? { folder: 'new' as const, root_dir: typed, take_over: refusal.takeOver }
      : null;
  const takeBackAgain =
    refusal?.takeOver && sent?.folder === 'take_back' && choice === 'take_back'
      ? { folder: 'take_back' as const, take_over: refusal.takeOver }
      : null;

  const backupOn = computer.files_backup !== false;
  // A folder that cannot be used at all is no other computer's to take back.
  const canTakeBack = failure?.code !== 'root_dir';
  const choices: RadioCardOption<FolderChoice>[] = [
    {
      value: 'take_back',
      title: t('computer.reinstall.takeBack', 'Take the folder back'),
      description: failure?.owner
        ? t(
            'computer.reinstall.takeBackOwned',
            'Keep using {{folder}}. Another computer works in it now, possibly one from another LangAlpha account or server. That computer stops working on this machine. The files in the folder stay.',
            { folder: root },
          )
        : t(
            'computer.reinstall.takeBackDesc',
            'Keep using {{folder}}. If another computer works in it now, that computer stops working on this machine. The files in the folder stay.',
            { folder: root },
          ),
      disabled: busy,
    },
    {
      value: 'new',
      title: t('computer.reinstall.newFolder', 'Use a new folder'),
      description: t('computer.reinstall.newFolderDesc', 'Start again in another folder on this machine. The old folder is left as it is.'),
      disabled: busy,
    },
  ];
  const options = canTakeBack ? choices : choices.filter((option) => option.value !== 'take_back');

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {t('common.cancel')}
      </Button>
      <Button type="submit" form={formId} disabled={busy || !body} aria-busy={busy}>
        {busy ? t('computer.reinstall.submitting', 'Reinstalling...') : t('computer.reinstall.submit', 'Reinstall')}
      </Button>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.reinstall.title', 'Reinstall {{name}}', { name: computer.name })}
      subtitle={t(
        'computer.reinstall.desc',
        'Choose the folder this computer works in on its machine. It starts again once that is set up.',
      )}
      onClose={onClose}
      closeDisabled={busy}
      width="standard"
      density="form"
      stacked
      footer={footer}
    >
      <form id={formId} onSubmit={submit} className="flex flex-col gap-4">
        {failure && <StartFailureNote failure={failure} className="text-sm" />}
        <RadioCardGroup
          options={options}
          value={choice}
          onChange={setChoice}
          label={t('computer.reinstall.folderLabel', 'Folder')}
        />
        {choice === 'new' && (
          <div className="flex flex-col gap-1">
            <label htmlFor={folderId} className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {t('computer.reinstall.newFolderLabel', 'New folder')}
            </label>
            <Input
              id={folderId}
              value={newFolder}
              onChange={(e) => setNewFolder(e.target.value)}
              disabled={busy}
              autoComplete="off"
              spellCheck={false}
              aria-describedby={hintId}
              className="font-mono"
              style={inputStyle}
            />
            <span id={hintId} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {backupOn
                ? t('computer.reinstall.newFolderBackup', 'Workspace files come back from their backup.')
                : t(
                    'computer.reinstall.newFolderNoBackup',
                    'Backup is off for this computer, so workspace files stay in {{folder}}. Copy them over yourself if you need them.',
                    { folder: root },
                  )}
            </span>
          </div>
        )}
        {refusal && (
          <div
            role="alert"
            className="flex flex-col gap-1.5 rounded-md px-3 py-2 text-sm"
            style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-text-primary)' }}
          >
            <p>{refusal.message}</p>
            {takeNew && (
              <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t(
                  'computer.reinstall.takeNewNote',
                  'Taking it stops that computer working there. The files in it stay. Or enter another folder.',
                )}{' '}
                <button
                  type="button"
                  className="underline font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                  onClick={() => send(takeNew)}
                >
                  {t('computer.reinstall.takeNew', 'Take this folder')}
                </button>
              </p>
            )}
            {takeBackAgain && (
              <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t(
                  'computer.reinstall.takeBackAgainNote',
                  'Taking it back stops that computer working there. The files in it stay.',
                )}{' '}
                <button
                  type="button"
                  className="underline font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                  onClick={() => send(takeBackAgain)}
                >
                  {t('computer.reinstall.takeBackAgain', 'Take it back from that computer')}
                </button>
              </p>
            )}
          </div>
        )}
      </form>
    </ModalShell>
  );
}

export default ReinstallDialog;
