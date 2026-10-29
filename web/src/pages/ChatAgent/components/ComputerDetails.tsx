/**
 * One computer in full, in place of the rail inside the Computers dialog:
 * the drawing beside its name, spec and storage by workspace, its settings,
 * and the rarer things done to it (make it primary, remove it). Kept in the
 * dialog rather than on a page of its own, so managing a machine never takes
 * the reader away from their workspaces, and back is one step.
 */
import { useEffect, useId, useRef, useState, type CSSProperties } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronLeft, Pencil } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { toast } from '@/components/ui/use-toast';
import { SwapCell } from '@/components/ui/crossfade';
import { Loader } from '@/components/ui/loader';
import { ToggleSwitch } from '@/components/ui/switch';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import { useIsPresent } from '@/lib/framer';
import { DURATION } from '@/lib/motion';
import type { Computer } from '@/types/api';

import { useComputerProviders, useComputerStorage } from '../hooks/useComputers';
import type { ThisMac } from '../hooks/useRemoteControl';
import { isComputerPreparing, isComputerStatusTransitional, takesWorkspaces } from '../utils/computerStatus';
import { DiskBar, DiskUsageText } from './computerDiskUi';
import { activeSpecChange, effectiveTier } from './specChangeUi';
import { COMPUTER_NAME_MAX, computerNameTakenMessage } from '../utils/computerName';
import { MakePrimaryButton } from './MakePrimaryDialog';
import { providerCanBePrimary, providerManagesSpec } from './providerUi';
import { ComputerFacts } from './computerFacts';
import {
  ComputerNotes,
  ComputerSpecRows,
  MachineFrame,
  NameRefusal,
  PowerButton,
  SpecRow,
  StartFailureAction,
  type MachineActions,
} from './computerParts';
import { railOrdinal } from './computerRailState';

type Translate = ReturnType<typeof useTranslation>['t'];

interface ComputerDetailsProps {
  computer: Computer;
  /** Its place in the rail, from 1, and how many machines the rail holds. */
  index: number;
  total: number;
  /** The rail's height on screen when it was left, for the details to take; none opened straight onto them. */
  height?: number | null;
  onBack: () => void;
  thisMac: ThisMac | null;
  showProvider: boolean;
  actions: MachineActions;
}

function ComputerDetails({ computer, index, total, height, onBack, thisMac, showProvider, actions }: ComputerDetailsProps) {
  const { t } = useTranslation();
  const prepareHintId = useId();
  const removeHintId = useId();
  const backRef = useRef<HTMLButtonElement>(null);
  const isTransitional = isComputerStatusTransitional(computer.status);
  const disk = computer.disk ?? null;
  const specChange = activeSpecChange(computer);
  const managesSpec = providerManagesSpec(computer.kind);
  // The primary is kept, so it offers no Remove at all. Every other computer
  // offers it, held while the server would refuse, with the reason under it.
  // Workspaces on it do not hold it: the confirmation offers to delete them.
  const removeHeld = computer.is_primary ? null : removeBlock(computer, actions.powerBusy);
  const removeHint = removeHeld ? removeBlockHint(t, removeHeld) : null;
  const canBePrimary = !computer.is_primary && providerCanBePrimary(computer.kind);
  const hasSettings = (managesSpec && computer.is_always_on) || computer.kind === 'ssh';

  // The view replaced the button that opened it, so focus starts on the way back.
  useEffect(() => {
    backRef.current?.focus();
  }, []);

  return (
    <div className="computer-details" style={height ? ({ '--computer-view-h': `${height}px` } as CSSProperties) : undefined}>
      <div className="computer-details-head">
        <button ref={backRef} type="button" className="computer-details-back" onClick={onBack}>
          <ChevronLeft className="h-3.5 w-3.5" />
          {t('computer.computers', 'Computers')}
        </button>
        <span className="computer-rail-count">
          {t('computer.card.position', '{{n}} / {{total}}', { n: railOrdinal(index), total: railOrdinal(total) })}
        </span>
      </div>

      <div className="computer-details-grid">
        {/* The card that was clicked, as on the rail, with what the machine
            is under it and every action on it after that. */}
        <div className="computer-details-side">
          <MachineFrame computer={computer} index={index} thisMac={thisMac} />
          <MachineFacts computer={computer} onChangeSpec={actions.changeSpec} />
          <div className="computer-details-actions">
            <StartFailureAction computer={computer} actions={actions} />
            <PowerButton computer={computer} actions={actions} />
            {(canBePrimary || !computer.is_primary) && (
              <div className="computer-details-rarer">
                {canBePrimary && (
                  <MakePrimaryButton
                    movingHome={actions.movingHome}
                    disabled={actions.makePrimaryHeld || !!specChange || !takesWorkspaces(computer)}
                    onClick={actions.makePrimary}
                  />
                )}
                {!computer.is_primary && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="ml-auto"
                    onClick={actions.remove}
                    disabled={!!removeHeld}
                    // A preparing computer already says why, in its own hint.
                    aria-describedby={removeHeld === 'preparing' ? prepareHintId : removeHint ? removeHintId : undefined}
                  >
                    {t('computer.remove', 'Remove')}
                  </Button>
                )}
              </div>
            )}
            {/* Under the buttons, so its arriving moves none of them. */}
            {removeHint && (
              <p id={removeHintId} className="computer-details-hint">
                {removeHint}
              </p>
            )}
          </div>
        </div>

        <div className="computer-details-main">
          <section className="computer-details-group">
            <EditableName name={computer.name} onCommit={actions.rename} />
            <ComputerFacts computer={computer} showProvider={showProvider} />
            <ComputerNotes computer={computer} thisMac={thisMac} prepareHintId={prepareHintId} primaryNote />
            <ComputerSpecRows computer={computer} spec={false}>
              {/* Where its workspaces are, on a machine the user brings. */}
              {computer.kind === 'ssh' && computer.root_dir && (
                <div>
                  <dt>{t('computer.machine.folder', 'Folder')}</dt>
                  <dd title={computer.root_dir}>
                    <span className="truncate">{computer.root_dir}</span>
                  </dd>
                </div>
              )}
            </ComputerSpecRows>
          </section>

          <section className="computer-details-group">
            <h3 className="computer-details-title">{t('computer.card.storage', 'Storage')}</h3>
            <p className="text-[12.5px]" style={{ color: 'var(--color-text-primary)' }}>
              <DiskUsageText disk={disk} />
            </p>
            {disk && <DiskBar disk={disk} className="mt-1.5" />}
            <StorageBreakdown computer={computer} />
          </section>

          {hasSettings && (
            <section className="computer-details-group">
              <dl className="computer-card-spec mt-0">
                {/* Always-on is no longer offered, so the switch is only there to turn it off. */}
                {managesSpec && computer.is_always_on && (
                  <div>
                    <dt>{t('computer.alwaysOn', 'Always-on')}</dt>
                    <dd>
                      <ToggleSwitch
                        checked
                        onChange={actions.turnOffAlwaysOn}
                        // Mid start or stop the switch would promise a state the machine
                        // is about to leave; it opens again once the machine settles.
                        disabled={actions.alwaysOnBusy || isTransitional || computer.status === 'deleted'}
                        ariaLabel={t('computer.alwaysOn', 'Always-on')}
                      />
                    </dd>
                  </div>
                )}
                {/* Only a machine the user brings offers the choice: a hosted one always backs up. */}
                {computer.kind === 'ssh' && (
                  <div>
                    <dt>{t('computer.filesBackup.label', 'Back up files')}</dt>
                    <dd>
                      <ToggleSwitch
                        checked={computer.files_backup !== false}
                        onChange={() => actions.setFilesBackup(computer.files_backup === false)}
                        disabled={actions.filesBackupBusy || computer.status === 'deleted'}
                        ariaLabel={t('computer.filesBackup.label', 'Back up files')}
                      />
                    </dd>
                  </div>
                )}
              </dl>
            </section>
          )}

        </div>
      </div>

    </div>
  );
}

/**
 * What the machine is: its system, processor and memory. A hosted one is
 * sized by its spec, so its spec leads its numbers, with the way to change
 * it; a machine the user brings says them itself as it starts, and a row it
 * has not said is left out.
 */
function MachineFacts({ computer, onChangeSpec }: { computer: Computer; onChangeSpec: () => void }) {
  const { t } = useTranslation();
  const { data } = useComputerProviders({ enabled: computer.kind !== 'ssh' });
  const rows: { key: string; label: string; value: string }[] = [];
  const add = (key: string, label: string, value: string | null | undefined) => {
    if (value) rows.push({ key, label, value });
  };
  const gb = (count: number) => t('computer.machine.gb', '{{count}} GB', { count });
  const ssh = computer.kind === 'ssh' ? computer.ssh : null;
  if (ssh) {
    add('system', t('computer.machine.system', 'System'), ssh.distro || systemName(ssh.os));
    add('processor', t('computer.machine.processor', 'Processor'), ssh.cpu || ssh.arch);
    add('cores', t('computer.machine.cores', 'Cores'), ssh.cores ? String(ssh.cores) : null);
    add('memory', t('computer.machine.memory', 'Memory'), ssh.memory_bytes ? gb(Math.round(ssh.memory_bytes / 2 ** 30)) : null);
  } else {
    const spec = data?.providers
      .find((p) => p.kind === computer.kind)
      ?.tiers?.find((s) => s.tier === effectiveTier(computer));
    add('system', t('computer.machine.system', 'System'), 'Linux');
    add('processor', t('computer.machine.processor', 'Processor'), spec && t('workspace.tierFact.cpu', '{{count}} vCPU', { count: spec.cpu }));
    add('memory', t('computer.machine.memory', 'Memory'), spec && gb(spec.memory_gib));
  }
  const row = ({ key, label, value }: (typeof rows)[number]) => (
    <div key={key}>
      <dt>{label}</dt>
      <dd title={value}>
        <span className="truncate">{value}</span>
      </dd>
    </div>
  );
  // The system first, then the spec that sizes what follows it.
  return (
    <dl className="computer-card-spec computer-details-machine">
      {rows.filter((r) => r.key === 'system').map(row)}
      <SpecRow computer={computer} onChangeSpec={onChangeSpec} />
      {rows.filter((r) => r.key !== 'system').map(row)}
    </dl>
  );
}

/** The system `uname -s` names, as its maker does. */
function systemName(os: string | null | undefined): string | null {
  if (os === 'Darwin') return 'macOS';
  return os || null;
}

/** Why Remove is held, null when the server would take the request. */
type RemoveBlock = 'preparing' | 'specChange' | 'starting' | 'stopping' | 'busy';

function removeBlock(computer: Computer, busy: boolean): RemoveBlock | null {
  if (isComputerPreparing(computer)) return 'preparing';
  if (activeSpecChange(computer)) return 'specChange';
  if (computer.status === 'creating' || computer.status === 'starting') return 'starting';
  if (computer.status === 'stopping') return 'stopping';
  // A power request in flight lands as 'starting' or 'stopping' a moment
  // later, and a removed row is about to leave the list: neither needs words.
  if (busy || computer.status === 'deleted') return 'busy';
  return null;
}

/** The line under a held Remove. A preparing computer has its own hint already. */
function removeBlockHint(t: Translate, block: RemoveBlock): string | null {
  switch (block) {
    case 'specChange':
      return t('computer.removeBlocked.specChange', 'You can remove it once the spec change finishes.');
    case 'starting':
      return t('computer.removeBlocked.starting', 'You can remove it once it has started.');
    case 'stopping':
      return t('computer.removeBlocked.stopping', 'You can remove it once it has stopped.');
    default:
      return null;
  }
}

/**
 * The name, editable in place. Enter or blur saves; Escape backs out. A name
 * another of the user's computers has comes back to the field, saying so, or
 * to a toast once the details are leaving: Back and Close blur it, which saves.
 */
function EditableName({ name, onCommit }: { name: string; onCommit: (name: string) => Promise<void> }) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(name);
  // Why the server refused the draft as it stands; cleared by any edit.
  const [refusal, setRefusal] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  // Bumped by every save and every keystroke, so a refusal that lands after
  // the user typed another name does not put the refused one back over it.
  const request = useRef(0);
  // Whether the field can still say a refusal: the details fade out on Back
  // or Close while the rename those sent is still out.
  const present = useIsPresent();
  const shown = useRef(false);
  useEffect(() => {
    shown.current = present;
    return () => {
      shown.current = false;
    };
  }, [present]);

  useEffect(() => {
    if (!editing) return;
    inputRef.current?.focus();
    inputRef.current?.select();
  }, [editing]);

  const backOut = () => {
    setDraft(name);
    setRefusal(null);
    setEditing(false);
  };

  const save = () => {
    setEditing(false);
    const next = draft.trim();
    if (!next || next === name) return;
    const ticket = ++request.current;
    // Any other refusal is told by the rename's own toast.
    onCommit(next).catch((err: unknown) => {
      const taken = computerNameTakenMessage(err, t);
      if (!taken || ticket !== request.current) return;
      if (!shown.current) {
        toast({ variant: 'destructive', title: t('computer.renameFailed', 'Could not rename computer'), description: taken });
        return;
      }
      setDraft(next);
      setRefusal(taken);
      setEditing(true);
    });
  };

  if (editing) {
    return (
      <div className="flex min-w-0 flex-col gap-1">
        <NameRefusal refusal={refusal} edge="var(--color-border-default)">
          {(field, borderColor) => (
            <input
              ref={inputRef}
              value={draft}
              maxLength={COMPUTER_NAME_MAX}
              aria-label={t('computer.renameLabel', 'Computer name')}
              {...field}
              className="computer-card-name min-w-0 w-full rounded border bg-transparent px-1.5 py-0.5"
              style={{ borderColor, color: 'var(--color-text-primary)', backgroundColor: 'var(--color-bg-input)' }}
              onChange={(e) => {
                request.current += 1;
                setDraft(e.target.value);
                setRefusal(null);
              }}
              // Left with the refused name still standing, the field backs out.
              onBlur={refusal ? backOut : save}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  // Enter on a refused name keeps the field and its reason.
                  if (!refusal) save();
                } else if (e.key === 'Escape') {
                  // Claimed here so the dialog around it stays open.
                  e.preventDefault();
                  backOut();
                }
              }}
            />
          )}
        </NameRefusal>
      </div>
    );
  }
  return (
    <span className="group/name flex items-center gap-1 min-w-0">
      <span className="computer-card-name truncate">
        {name}
      </span>
      <button
        type="button"
        className="-m-1 shrink-0 rounded p-2 opacity-60 hover:opacity-100 focus-visible:opacity-100"
        style={{ color: 'var(--color-text-tertiary)' }}
        aria-label={t('computer.rename', 'Rename computer')}
        onClick={() => {
          setDraft(name);
          setEditing(true);
        }}
      >
        <Pencil className="h-3 w-3" />
      </button>
    </span>
  );
}

/**
 * What each workspace uses, in a box that scrolls rather than growing the
 * dialog. A reading is a `du` on the machine, so the first one takes a moment:
 * the rows fade in over the wait rather than landing all at once.
 */
function StorageBreakdown({ computer }: { computer: Computer }) {
  const query = useComputerStorage(computer.computer_id, { enabled: true });
  const shown = query.isLoading ? 'measuring' : query.isError || !query.data ? 'failed' : query.data.live ? 'rows' : 'note';
  // The reading goes in as props, so the layer fading out keeps what it showed.
  return (
    <SwapCell swapKey={shown} as="div" enterDuration={DURATION.enter}>
      <StorageReading status={computer.status} data={query.data} isLoading={query.isLoading} isError={query.isError} />
    </SwapCell>
  );
}

interface StorageReadingProps {
  status: Computer['status'];
  data: ReturnType<typeof useComputerStorage>['data'];
  isLoading: boolean;
  isError: boolean;
}

function StorageReading({ status, data, isLoading, isError }: StorageReadingProps) {
  const { t } = useTranslation();
  const locale = useLocale();

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        <Loader size={12} className="text-(--color-accent-primary)" />
        {t('computer.disk.measuring', 'Measuring')}
      </div>
    );
  }
  if (isError || !data) {
    return (
      <p className="py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('computer.disk.breakdownFailed', 'Could not read storage right now.')}
      </p>
    );
  }
  if (!data.live) {
    // A running machine answers stored readings only when it cannot measure:
    // a failed read if it ever reported a disk, a machine that never measures
    // (no storage quota) if not. Neither is fixed by starting it.
    return (
      <p className="py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {status !== 'running'
          ? t('computer.disk.startForBreakdown', 'Start the computer to see what each workspace uses.')
          : data.disk
            ? t('computer.disk.breakdownFailed', 'Could not read storage right now.')
            : t('computer.disk.breakdownUnavailable', "This computer doesn't report storage by workspace.")}
      </p>
    );
  }
  const rows = [...data.workspaces].sort((a, b) => b.bytes - a.bytes);
  return (
    <ul className="computer-details-storage" aria-label={t('computer.disk.breakdown', 'By workspace')}>
      {rows.map((w) => (
        <li key={w.workspace_id}>
          <span className="truncate">{w.name}</span>
          <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>{formatBytes(w.bytes, locale)}</span>
        </li>
      ))}
      {data.other_bytes > 0 && (
        <li>
          <span className="truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('computer.disk.other', 'System, caches and shared files')}
          </span>
          <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>{formatBytes(data.other_bytes, locale)}</span>
        </li>
      )}
    </ul>
  );
}

export default ComputerDetails;
