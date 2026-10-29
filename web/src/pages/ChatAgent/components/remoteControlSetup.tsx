/**
 * Adding this Mac by remote control, in the desktop app.
 *
 * Nothing happens until the user has read what it means and said so: the
 * agent gets everything their macOS account can do, keeps it after the app
 * closes, and takes direction from whoever signs in to their account. Then the
 * helper bundled with the app pairs the Mac and holds a tunnel open, and the
 * flow ends on the same summary as the SSH pairing. The state is
 * `remoteControlSetupState.ts`.
 */
import { useId } from 'react';
import { KeyRound, Power, Settings, Terminal, Users, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';

import { Pill } from './pill';
import type { RemoteControlPhase, RemoteControlSetupState } from './remoteControlSetupState';
import { PreviewNotice, ReadySummary, Working } from './sshSetup';

const primaryStyle = { color: 'var(--color-text-primary)' } as const;
const labelStyle = { color: 'var(--color-text-secondary)' } as const;
const mutedStyle = { color: 'var(--color-text-tertiary)' } as const;
const dangerStyle = { color: 'var(--color-icon-danger)' } as const;

/** Each point the user agrees to, in the order the risk reaches them. */
const POINTS: { key: string; fallback: string; icon: LucideIcon }[] = [
  {
    key: 'computer.remoteControl.meansAccess',
    fallback: 'It can run any command and read or change any file {{account}} can, in any folder. Its work stays in the folder you pick, but nothing on this Mac holds it there.',
    icon: Terminal,
  },
  {
    key: 'computer.remoteControl.meansBackground',
    fallback: 'It keeps working after you close LangAlpha. Remote control runs in the background and starts again when you log in, until you turn it off.',
    icon: Power,
  },
  {
    key: 'computer.remoteControl.meansCredentials',
    fallback: 'It can read what command line tools keep for {{account}}, such as Git and cloud sign-ins, and reach your home or office network.',
    icon: KeyRound,
  },
  {
    key: 'computer.remoteControl.meansAccount',
    fallback: 'Anyone who signs in to your LangAlpha account can direct it.',
    icon: Users,
  },
];

function Disclosure({ state, account, busy }: { state: RemoteControlSetupState; account: string; busy: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3" data-testid="remote-control-disclosure">
      <p className="text-sm" style={labelStyle}>
        {t(
          'computer.remoteControl.intro',
          "LangAlpha's agent will work on this Mac as {{account}}, with everything that account can do.",
          { account },
        )}
      </p>

      <div className="flex flex-col gap-1.5">
        <h4 className="font-medium" style={primaryStyle}>{t('computer.remoteControl.meansLabel', 'What it means')}</h4>
        <ul className="flex flex-col gap-2" style={labelStyle}>
          {POINTS.map(({ key, fallback, icon: Icon }) => (
            <li key={key} className="flex items-start gap-2">
              <Icon aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" style={mutedStyle} />
              <span>{t(key, fallback, { account })}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="flex flex-col gap-1">
        <h4 className="font-medium" style={primaryStyle}>{t('computer.remoteControl.stopLabel', 'Stopping it')}</h4>
        <p style={labelStyle}>
          {t(
            'computer.remoteControl.stop',
            'Pause it any time from the LangAlpha menu, or in System Settings under General, Login Items. Removing this computer in LangAlpha turns it off and deletes its keys from this Mac.',
          )}
        </p>
      </div>

      <p style={mutedStyle}>
        {t(
          'computer.remoteControl.folderPrompts',
          "macOS may ask whether LangAlpha can open folders such as Documents or Desktop. You can say no: the agent's own folder does not need it.",
        )}
      </p>

      <PreviewNotice testId="remote-control-preview-notice">
        {t(
          'computer.remoteControl.previewNotice',
          'Remote control is in preview while we build more safeguards. Until then the agent has the same access you do, so turn it on only on a Mac where that is all right.',
        )}
      </PreviewNotice>

      <CheckboxChoice
        checked={state.acknowledged}
        onCheckedChange={state.setAcknowledged}
        disabled={busy}
        size="md"
        markClassName="mt-0.5"
        className="items-start gap-2 rounded-md text-sm"
        style={primaryStyle}
      >
        <span>
          {t(
            'computer.remoteControl.acknowledge',
            "I understand LangAlpha's agent can run commands and change files on this Mac as {{account}}.",
            { account },
          )}
        </span>
      </CheckboxChoice>
    </div>
  );
}

/** The line under the heading once the user has said yes, until the Mac is ready. */
function Progress({ phase }: { phase: RemoteControlPhase }) {
  const { t } = useTranslation();
  return (
    <>
      {/* Mounted from the disclosure on, so the first change of state is announced too. */}
      <div role="status" aria-live="polite" style={labelStyle}>
        {phase.kind === 'working' && (
          <Working>
            {phase.stage === 'connecting'
              ? t('computer.remoteControl.connecting', 'Connecting this Mac…')
              : t('computer.remoteControl.turningOn', 'Turning on remote control…')}
          </Working>
        )}
        {phase.kind === 'approval' && (
          <span className="flex items-start gap-2" data-testid="remote-control-approval">
            <Settings aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" style={mutedStyle} />
            <span>
              {t(
                'computer.remoteControl.needsApproval',
                'macOS needs your OK. In System Settings, under General, Login Items, allow LangAlpha in the background, then continue.',
              )}
            </span>
          </span>
        )}
      </div>
      {phase.kind === 'failed' && (
        <p role="alert" style={dangerStyle}>
          {phase.message ?? t('computer.remoteControl.failed', "Couldn't turn on remote control. Try again.")}
        </p>
      )}
      {phase.kind === 'insecure' && (
        <p role="alert" style={dangerStyle}>
          {t(
            'computer.remoteControl.insecure',
            "Remote control needs this server's address to start with https, so this Mac can trust the pairing it is sent.",
          )}
        </p>
      )}
    </>
  );
}

export function RemoteControlSetup({
  state,
  account,
  busy,
  keeps,
  moving = false,
}: {
  state: RemoteControlSetupState;
  /** The macOS account the agent will work as, named in the disclosure. */
  account: string;
  busy: boolean;
  /** The folder of the computer this reconnects, which the ready step says it keeps. */
  keeps?: string;
  /** This Mac holds nothing of that computer, which moves here. */
  moving?: boolean;
}) {
  const { t } = useTranslation();
  const headingId = useId();
  const { phase } = state;
  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-3 text-xs" data-testid="remote-control-setup">
      <div className="flex items-center gap-2">
        <h3 id={headingId} className="text-sm font-medium" style={primaryStyle}>
          {t('computer.remoteControl.title', 'Remote control of this Mac')}
        </h3>
        <Pill tone="warning">{t('computer.ssh.previewLabel', 'Preview')}</Pill>
      </div>
      {phase.kind === 'ready' ? (
        // No "Use a different machine": this Mac is the only one this way adds.
        <ReadySummary step={state.readyStep} ready={phase.ready} busy={busy} keeps={keeps} moving={moving} />
      ) : (
        <>
          {phase.kind === 'disclosure' && <Disclosure state={state} account={account} busy={busy} />}
          <Progress phase={phase} />
        </>
      )}
    </section>
  );
}

/** The footer's button where the flow waits on the user; elsewhere the dialog shows Add computer. */
export function RemoteControlActionButton({ state, busy }: { state: RemoteControlSetupState; busy: boolean }) {
  const { t } = useTranslation();
  if (!state.action) return null;
  const label = state.action === 'turnOn'
    ? t('computer.remoteControl.turnOn', 'Turn on remote control')
    : state.action === 'continue'
      ? t('computer.remoteControl.continue', 'Continue')
      : t('computer.remoteControl.tryAgain', 'Try again');
  return (
    <Button type="button" onClick={state.start} disabled={busy || !state.acknowledged}>
      {label}
    </Button>
  );
}
