/**
 * What the pair prompt shows for each stage of `useThisMacPairing`: this Mac
 * unpaired with what pairing means, then the app's own steps (Remote control,
 * Login Items, pairing), then this Mac as a computer. The page around it
 * owns the title and the footer, so the computer step can show it too.
 */
import { useTranslation } from 'react-i18next';
import { AlertTriangle, Check, Laptop } from 'lucide-react';
import { CreateRefusal } from '@/pages/ChatAgent/components/addComputer';
import { ArtFrame } from '@/pages/ChatAgent/components/machineArt/ArtFrame';
import { MachineArt } from '@/pages/ChatAgent/components/machineArt/MachineArt';
import { Group, ROW_CARD } from '@/components/modelAccess/parts';
import { cn } from '@/lib/utils';
import { AccessRow } from '@/components/modelAccess/AccessRow';
import { RowButton, SetupNote } from '../answers/parts';
import { ProgressList, ProgressStep, type ProgressState } from '../answers/ProgressSteps';
import type { PairStage, ThisMacPairing } from './useThisMacPairing';

const T = 'setup.thisMac';

/** Where the Login Items and pairing steps stand at each stage past the ask. */
const STEP_STATES: Record<Exclude<PairStage, 'ask' | 'confirm'>, { login: ProgressState; pair: ProgressState }> = {
  approval: { login: 'active', pair: 'todo' },
  pairing: { login: 'done', pair: 'active' },
  failed: { login: 'done', pair: 'failed' },
  paired: { login: 'done', pair: 'done' },
};

const elapsed = (seconds: number) => `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;

function Points({ account }: { account: string }) {
  const { t } = useTranslation();
  return (
    <>
      <ul className={cn(ROW_CARD, 'mt-3 overflow-hidden text-sm')} style={{ color: 'var(--color-text-primary)' }}>
        {(['run', 'closed', 'off'] as const).map((key) => (
          <li key={key} className="flex items-start gap-3 px-4 py-3">
            <Check aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            <span>{t(`${T}.point.${key}`)}</span>
          </li>
        ))}
      </ul>
      <SetupNote icon={<AlertTriangle />}>
        {account ? t(`${T}.account`, { account }) : t(`${T}.accountUnnamed`)}
      </SetupNote>
    </>
  );
}

export function ThisMacPairingBody({ pairing, account }: { pairing: ThisMacPairing; account: string }) {
  const { t } = useTranslation();
  const { stage, error, computer } = pairing;

  if (stage === 'ask' || stage === 'confirm') {
    return (
      <div data-testid="this-mac-ask">
        <ArtFrame className="is-preview" tl={t(`${T}.art.thisMac`)} tr={t(`${T}.art.notPaired`)}>
          <MachineArt art="macbook" state="off" label={t(`${T}.art.thisMac`)} />
        </ArtFrame>
        <Points account={account} />
      </div>
    );
  }

  const { login, pair } = STEP_STATES[stage];
  return (
    <div data-testid={`this-mac-${stage}`}>
      <ProgressList>
        <ProgressStep n={1} state="done" title={t(`${T}.step.on`)} />
        <ProgressStep n={2} state={login} title={t(`${T}.step.loginItems`)}>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t(`${T}.step.loginItemsDetail`)}</p>
          <Group>
            <AccessRow
              icon={<Laptop />}
              name="LangAlpha"
              detail={t(`${T}.step.loginItemsPath`)}
              wrapDetail
              action={<RowButton onClick={pairing.retry}>{t(`${T}.openLoginItems`)}</RowButton>}
            />
          </Group>
        </ProgressStep>
        <ProgressStep n={3} state={pair} title={t(`${T}.step.pairing`)}>
          {stage === 'failed' ? (
            error && <CreateRefusal message={error} onTakeOver={pairing.takeOver ? pairing.takeOverFolder : undefined} />
          ) : (
            <p role="status" className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t(`${T}.step.pairingDetail`, { elapsed: elapsed(pairing.elapsedSeconds) })}
            </p>
          )}
        </ProgressStep>
      </ProgressList>
      {computer && (
        <div className="mt-3">
          <Group>
            <AccessRow
              testId="this-mac-computer"
              icon={<Laptop />}
              name={computer.name}
              detail={[computer.ssh?.username, computer.ssh?.distro].filter(Boolean).join(' · ')}
              status="connected"
            />
          </Group>
        </div>
      )}
    </div>
  );
}
