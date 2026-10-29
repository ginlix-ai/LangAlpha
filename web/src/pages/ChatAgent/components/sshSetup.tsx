/**
 * Adding the user's own machine over SSH.
 *
 * Two ways in. Pairing, the default: the user runs one command on the machine,
 * which authorizes our key and reports how to reach it, and the server dials
 * it. By hand: the user types the address, we connect, and a refusal shows
 * the key to install. Both end on the same summary, and create stays closed
 * until the server has connected to a supported machine, so the host key
 * that gets pinned is the one that answered. The state is `sshSetupState.ts`.
 */
import { useId, useState, type CSSProperties, type KeyboardEvent, type ReactNode } from 'react';
import { AlertTriangle, Check, Copy } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { copyText } from '@/lib/clipboard';
import { CheckboxChoice } from '@/components/ui/choice';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { formatBytes } from '@/lib/format';
import { cn } from '@/lib/utils';
import { useLocale } from '@/hooks/useLocale';
import type { SshTarget } from '@/types/api';

import { apiErrorStatus } from '@/api/errors';
import { failureText, type SshFailure, type SshReadyStep, type SshSetupState } from './sshSetupState';
import { formatSshReached, type SshReady } from './sshTarget';

const SSH_ADDRESS_PLACEHOLDER = 'ubuntu@203.0.113.10';

function CopyBox({ text, label }: { text: string; label: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await copyText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard can be blocked; the text stays selectable in the box.
    }
  };
  return (
    <div
      role="group"
      aria-label={label}
      className="flex items-start gap-2 rounded-md border p-2"
      style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-input)' }}
    >
      <code
        className="min-w-0 flex-1 select-all break-all font-mono text-xs"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {text}
      </code>
      <Button type="button" variant="ghost" size="sm" className="shrink-0 gap-1.5" onClick={copy} aria-label={t('computer.ssh.copyLabel', 'Copy {{what}}', { what: label })}>
        {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
        {copied ? t('common.copied', 'Copied') : t('common.copy', 'Copy')}
      </Button>
      <span role="status" aria-live="polite" className="sr-only">{copied ? t('common.copied', 'Copied') : ''}</span>
    </div>
  );
}

const labelStyle = { color: 'var(--color-text-secondary)' } as const;
const mutedStyle = { color: 'var(--color-text-tertiary)' } as const;
const dangerStyle = { color: 'var(--color-icon-danger)' } as const;
const inputStyle = {
  backgroundColor: 'var(--color-bg-input)',
  borderColor: 'var(--color-border-muted)',
  color: 'var(--color-text-primary)',
} as const;

function LinkButton({
  onClick,
  disabled,
  className,
  children,
}: {
  onClick: () => void;
  disabled?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      className={cn('self-start text-xs underline underline-offset-2 disabled:opacity-50', className)}
      style={labelStyle}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
}

/** A spinning glyph and a line, the app's way of saying work is in progress. */
export function Working({ children }: { children: ReactNode }) {
  return (
    <span className="flex items-center gap-2">
      <span aria-hidden="true" className="inline-flex shrink-0">
        <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
      </span>
      <span>{children}</span>
    </span>
  );
}

/** The "Host or SSH command" field with its Connect button, shared by both ways in. */
function AddressField({
  value,
  onChange,
  onConnect,
  connecting,
  error,
  busy,
  hint,
  inputRef,
}: {
  value: string;
  onChange: (value: string) => void;
  onConnect: () => void;
  connecting: boolean;
  error: string | null;
  busy: boolean;
  hint?: string;
  inputRef?: (el: HTMLElement | null) => void;
}) {
  const { t } = useTranslation();
  const id = useId();
  const hintId = `${id}-hint`;
  const errorId = `${id}-error`;
  const describedBy = [hint && hintId, error && errorId].filter(Boolean).join(' ') || undefined;
  // Enter connects rather than submitting the dialog, which is closed until then anyway.
  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== 'Enter') return;
    e.preventDefault();
    onConnect();
  };
  return (
    <div className="flex flex-col gap-1">
      <label htmlFor={id} className="text-xs" style={labelStyle}>
        {t('computer.ssh.addressLabel', 'Host or SSH command')}
      </label>
      <div className="flex items-center gap-2">
        <Input
          id={id}
          ref={inputRef}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={onKeyDown}
          disabled={busy || connecting}
          autoComplete="off"
          autoCapitalize="off"
          spellCheck={false}
          placeholder={SSH_ADDRESS_PLACEHOLDER}
          aria-invalid={error ? true : undefined}
          aria-describedby={describedBy}
          className="min-w-0 flex-1 font-mono"
          style={inputStyle}
        />
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="shrink-0"
          onClick={onConnect}
          disabled={busy || connecting || !value.trim()}
          aria-busy={connecting}
        >
          {connecting ? t('computer.ssh.connecting', 'Connecting...') : t('computer.ssh.connect', 'Connect')}
        </Button>
      </div>
      {hint && <span id={hintId} className="text-xs" style={mutedStyle}>{hint}</span>}
      {error && <p id={errorId} className="text-xs" role="alert" style={dangerStyle}>{error}</p>}
    </div>
  );
}

function ProblemList({ problems, style }: { problems: string[]; style?: CSSProperties }) {
  if (problems.length === 0) return null;
  return (
    <ul className="list-disc pl-4" style={style}>
      {problems.map((p) => <li key={p}>{p}</li>)}
    </ul>
  );
}

function PairingView({
  state,
  busy,
  focusTarget,
  reconnecting,
}: {
  state: SshSetupState;
  busy: boolean;
  /** Takes focus when the user switches to this view. */
  focusTarget: (el: HTMLElement | null) => void;
  /** The pairing reconnects a computer, whose login it should run as. */
  reconnecting?: { username: string };
}) {
  const { t } = useTranslation();
  const { pairing } = state;
  const status = pairing.status;
  const failed = status?.status === 'failed' && !pairing.expired ? status : null;
  const failure: SshFailure | null = failed?.error
    ? { code: failed.error.code, message: failed.error.message, target: failed.target, username: failed.machine?.username ?? null, os: failed.machine?.os || null }
    : failed
      ? { code: null, message: '', target: null, username: null, os: null }
      : null;
  // A refused key or an unsupported machine was reached at the right address,
  // so typing another one cannot help.
  const canEnterAddress = !!failed && !!pairing.machine
    && failure?.code !== 'unsupported' && failure?.code !== 'auth_failed';
  // What the status row says. A connect from the failed state says so on its
  // own button, so the row stays quiet then.
  const progressText = (): string | null => {
    if (!pairing.command) return t('computer.ssh.pairLoading', 'Getting your command...');
    if (failed) return null;
    if (status?.status === 'tunneling') {
      return t('computer.ssh.tunneling', 'Connecting {{hostname}} through a tunnel. It could not be reached from the internet, so it is connecting out instead. This takes a few seconds.', {
        hostname: pairing.machine?.hostname || pairing.machine?.addresses[0] || '',
      });
    }
    if (status?.status !== 'checking') return t('computer.ssh.waiting', 'Waiting for your machine...');
    const machine = pairing.machine;
    if (!machine) return t('computer.ssh.connecting', 'Connecting...');
    return t('computer.ssh.checking', '{{username}}@{{hostname}} checked in. Connecting...', {
      username: machine.username,
      hostname: machine.hostname || machine.addresses[0] || '',
    });
  };
  const working = progressText();

  return (
    <div
      ref={focusTarget}
      tabIndex={-1}
      className="flex flex-col gap-2 text-xs"
      data-testid="ssh-pairing"
    >
      <p style={labelStyle}>
        {pairing.localOnly
          ? t('computer.ssh.pairIntroLocal', 'Run this in a terminal on the computer running LangAlpha, as the user the agent should work as. To add another machine, set it up by hand.')
          : reconnecting
            ? t('computer.reconnect.pairIntro', 'Run this on the machine, as {{username}}:', { username: reconnecting.username })
            : t('computer.ssh.pairIntro', 'Run this on the machine you are adding (Linux or macOS), as the user the agent should work as:')}
      </p>
      {pairing.command && (
        <>
          <CopyBox text={pairing.command} label={t('computer.ssh.commandLabel', 'command')} />
          {pairing.scriptUrl && (
            <a
              href={pairing.scriptUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="self-start underline underline-offset-2"
              style={mutedStyle}
            >
              {t('computer.ssh.viewScript', 'View script')}
            </a>
          )}
        </>
      )}

      {/* Mounted throughout so each change of state is announced. */}
      <div role="status" aria-live="polite" style={mutedStyle}>
        {working && <Working>{working}</Working>}
      </div>

      {failure && (
        <div className="flex flex-col gap-2">
          <p role="alert" style={dangerStyle}>{failureText(failure, t)}</p>
          {failure.code === 'unsupported' && <ProblemList problems={failed?.probe?.problems ?? []} style={dangerStyle} />}
          {canEnterAddress ? (
            <>
              <p style={labelStyle}>
                {t('computer.ssh.pairFailedNext', 'Fix it and run the command again, or enter the address yourself:')}
              </p>
              <AddressField
                value={pairing.address}
                onChange={pairing.editAddress}
                onConnect={pairing.connect}
                connecting={pairing.connecting}
                error={pairing.connectError}
                busy={busy}
              />
            </>
          ) : (
            <p style={labelStyle}>{t('computer.ssh.pairRerun', 'Fix it and run the command again.')}</p>
          )}
        </div>
      )}

      <LinkButton onClick={() => state.switchTo('manual')} disabled={busy}>
        {t('computer.ssh.manualLink', 'Set it up by hand instead')}
      </LinkButton>
    </div>
  );
}

function KeyStep({ state, target }: { state: SshSetupState; target: SshTarget }) {
  const { t } = useTranslation();
  const { key } = state.manual;
  return (
    <div className="flex flex-col gap-1.5" data-testid="ssh-key-step">
      <p role="alert" style={labelStyle}>
        {t('computer.ssh.keyStep', "Our key isn't on {{username}}@{{host}} yet. Run this there as {{username}}, then Connect again:", {
          username: target.username,
          host: target.host,
        })}
      </p>
      {key.isPending && <Working>{t('computer.ssh.keyLoading', 'Getting your key...')}</Working>}
      {key.isError && (
        <p role="alert" style={dangerStyle}>
          {apiErrorStatus(key.error) === 503
            ? t('computer.ssh.keyUnavailable', 'This server cannot store SSH keys, so it cannot connect to your machine.')
            : t('computer.ssh.keyFailed', 'Could not get your key. Close this dialog and try again.')}
        </p>
      )}
      {key.data && (
        <>
          <CopyBox text={key.data.authorized_keys_command} label={t('computer.ssh.commandLabel', 'command')} />
          <details style={mutedStyle}>
            <summary className="cursor-pointer">{t('computer.ssh.keyManual', 'Or add the public key yourself')}</summary>
            <div className="mt-1.5 flex flex-col gap-1">
              <CopyBox text={key.data.public_key} label={t('computer.ssh.publicKeyLabel', 'public key')} />
              <span className="break-all font-mono">{key.data.fingerprint}</span>
            </div>
          </details>
        </>
      )}
    </div>
  );
}

function ManualView({ state, busy }: { state: SshSetupState; busy: boolean }) {
  const { t } = useTranslation();
  const { manual } = state;
  const failure = manual.failure;
  return (
    <div className="flex flex-col gap-2 text-xs" data-testid="ssh-manual">
      {state.pairing.unavailable && (
        <p role={state.pairing.limitMessage ? 'alert' : undefined} style={state.pairing.limitMessage ? dangerStyle : mutedStyle}>
          {state.pairing.limitMessage ??
            t('computer.ssh.pairUnavailable', 'The one-line command is not available on this server, so set it up by hand.')}
        </p>
      )}
      <AddressField
        value={manual.address}
        onChange={manual.editAddress}
        onConnect={manual.connect}
        connecting={manual.connecting}
        error={manual.addressError}
        busy={busy}
        hint={t('computer.ssh.addressHint', 'Add :2222 for another port.')}
        inputRef={state.setEntry}
      />
      {failure && failure.code === 'auth_failed' && failure.target ? (
        <KeyStep state={state} target={failure.target} />
      ) : failure ? (
        <p role="alert" style={dangerStyle}>{failureText(failure, t)}</p>
      ) : null}
      <LinkButton onClick={() => state.switchTo('pairing')} disabled={busy}>
        {t('computer.ssh.pairingLink', 'Use the one-line command instead')}
      </LinkButton>
    </div>
  );
}

export function ReadySummary({
  step,
  ready,
  busy,
  keeps,
  moving = false,
}: {
  step: SshReadyStep;
  ready: SshReady;
  busy: boolean;
  /**
   * The folder of the computer this machine reconnects, which it keeps: the
   * summary says so in place of the folder and backup choices, and offers no
   * other machine.
   */
  keeps?: string;
  /** The machine holds nothing of that computer, which moves there: its folder is filled from backup. */
  moving?: boolean;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const folderId = useId();
  const backupHintId = useId();
  const { target, probe } = ready;
  // A helper is reached through its tunnel by construction and logs in as the
  // account it works as, so it is named the way its row will name it.
  const reached = probe.helper
    ? t('computer.remoteControl.asAccount', 'as {{account}}', { account: target.username })
    : target.tunnel_id
      ? t('computer.ssh.readyTunnel', '{{hostname}}, through a tunnel', { hostname: formatSshReached(target, true) })
      : formatSshReached(target, false);
  const startOver = !keeps && step.startOver && (
    <LinkButton onClick={step.startOver} disabled={busy} className="ml-auto shrink-0 self-auto">
      {t('computer.ssh.differentMachine', 'Use a different machine')}
    </LinkButton>
  );

  if (!probe.supported) {
    return (
      <div className="flex flex-col gap-1.5 text-xs" data-testid="ssh-unsupported">
        <div className="flex items-baseline gap-2">
          <span className="min-w-0 break-all font-medium" style={{ color: 'var(--color-text-primary)' }}>{reached}</span>
          {startOver}
        </div>
        <div className="flex flex-col gap-1" role="alert" style={dangerStyle}>
          <span>{t('computer.ssh.error.unsupported', 'This machine is not supported.')}</span>
          <ProblemList problems={probe.problems} />
        </div>
      </div>
    );
  }

  const facts = [
    probe.distro || probe.os,
    probe.arch,
    probe.disk_free_bytes != null ? t('computer.ssh.diskFree', '{{free}} free', { free: formatBytes(probe.disk_free_bytes, locale) }) : null,
    !probe.is_root && probe.has_sudo ? t('computer.ssh.sudoYes', 'sudo available') : null,
  ].filter(Boolean);
  const access = probe.is_root
    ? t('computer.ssh.sudoRoot', 'Connecting as root.')
    : probe.has_sudo
      ? null
      : t('computer.ssh.sudoNo', 'sudo is not available. Some tools may not install.');
  const shownFolder = step.folder.trim() || probe.suggested_root_dir;

  return (
    <div
      className="flex flex-col gap-3 rounded-md border p-3 text-xs"
      style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
      data-testid="ssh-ready"
    >
      <div className="flex flex-col gap-1">
        <div className="flex items-baseline gap-2">
          <Check aria-hidden="true" className="h-3.5 w-3.5 shrink-0 self-center" style={{ color: 'var(--color-success)' }} />
          <span className="min-w-0 break-all text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {reached}
          </span>
          {startOver}
        </div>
        {facts.length > 0 && <span>{facts.join(' · ')}</span>}
        {access && <span>{access}</span>}
      </div>

      {keeps ? (
        <span className="min-w-0 break-all">
          {moving
            ? t('computer.reconnect.folderRestored', 'Workspaces come back in {{folder}} from their backup.', { folder: keeps })
            : t('computer.reconnect.folderKept', 'Workspaces stay in {{folder}}.', { folder: keeps })}
        </span>
      ) : step.folderOpen ? (
        <div className="flex flex-col gap-1">
          <label htmlFor={folderId} style={labelStyle}>{t('computer.ssh.folder', 'Folder')}</label>
          <Input
            id={folderId}
            value={step.folder}
            onChange={(e) => step.setFolder(e.target.value)}
            disabled={busy}
            autoComplete="off"
            spellCheck={false}
            // An emptied field sends no folder, so the server picks this one.
            placeholder={probe.suggested_root_dir}
            // Opened by the user's own click on Change, so the field is where they are headed.
            autoFocus
            className="font-mono"
            style={inputStyle}
          />
          <span style={mutedStyle}>{t('computer.ssh.folderHint', 'Where workspaces are kept on your machine.')}</span>
        </div>
      ) : (
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span className="min-w-0 break-all">
            {t('computer.ssh.folderShown', 'Workspaces go in {{folder}}.', { folder: shownFolder })}
          </span>
          <LinkButton onClick={step.openFolder} disabled={busy} className="self-auto">
            {t('computer.ssh.changeFolder', 'Change')}
          </LinkButton>
        </div>
      )}

      {!keeps && (
        <div className="flex flex-col gap-1">
          <CheckboxChoice
            checked={step.filesBackup}
            onCheckedChange={step.setFilesBackup}
            disabled={busy}
            aria-describedby={backupHintId}
            className="items-center gap-2 rounded-md text-xs"
            style={labelStyle}
          >
            <span>{t('computer.ssh.filesBackup', 'Back up workspace files')}</span>
          </CheckboxChoice>
          <span id={backupHintId} style={mutedStyle}>
            {t(
              'computer.ssh.filesBackupHint',
              "Keeps a copy of each workspace's files on the server, so a lost folder can be restored. Version control and dependency folders are left out.",
            )}
          </span>
        </div>
      )}

      <span className="break-all" style={mutedStyle}>
        {t('computer.ssh.hostKey', 'Host key {{fingerprint}}', { fingerprint: probe.fingerprint })}
      </span>
      <ProblemList problems={probe.problems} style={mutedStyle} />
    </div>
  );
}

/** Same callout as the experiments trade-off: tinted, never an error. */
export function PreviewNotice({ children, className, testId }: { children: ReactNode; className?: string; testId?: string }) {
  const { t } = useTranslation();
  return (
    <div
      className={cn('flex items-start gap-2 rounded-md px-3 py-2 text-xs leading-relaxed', className)}
      style={{ backgroundColor: 'var(--color-warning-soft)' }}
      data-testid={testId}
    >
      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-warning)' }} aria-hidden="true" />
      <p style={labelStyle}>
        <span className="font-medium" style={{ color: 'var(--color-warning)' }}>
          {t('computer.ssh.previewLabel', 'Preview')}
        </span>{' '}
        {children}
      </p>
    </div>
  );
}

export function SshSetup({
  state,
  busy,
  reconnecting,
}: {
  state: SshSetupState;
  busy: boolean;
  /**
   * The computer this setup reconnects: its login and the folder it keeps.
   * It was added with its warning read, so that is not shown again, and
   * the ready step offers no choices it already made.
   */
  reconnecting?: { username: string; folder: string; moving?: boolean };
}) {
  const { t } = useTranslation();
  return (
    <div data-testid="ssh-setup">
      {!reconnecting && (
        <PreviewNotice className="mb-3" testId="ssh-preview-notice">
          {t(
            'computer.ssh.previewNotice',
            'Connecting your own machine is new, so use it with care. The agent can do anything the account you connect can do, so connect an account made for it rather than your own.',
          )}
        </PreviewNotice>
      )}
      {state.ready ? (
        <ReadySummary
          step={state.readyStep}
          ready={state.ready}
          busy={busy}
          keeps={reconnecting?.folder}
          moving={reconnecting?.moving}
        />
      ) : state.view === 'pairing' ? (
        <PairingView state={state} busy={busy} focusTarget={state.setEntry} reconnecting={reconnecting} />
      ) : (
        <ManualView state={state} busy={busy} />
      )}
    </div>
  );
}
