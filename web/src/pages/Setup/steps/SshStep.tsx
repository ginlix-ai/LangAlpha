import { useId } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Laptop } from 'lucide-react';
import { WizardStepHeader } from '@/components/ui/wizard';
import { CreateRefusal, createRefusalMessage, ownMachineCreate } from '@/pages/ChatAgent/components/addComputer';
import { SshSetup } from '@/pages/ChatAgent/components/sshSetup';
import { useSshSetup } from '@/pages/ChatAgent/components/sshSetupState';
import { folderMarkedOwner } from '@/pages/ChatAgent/utils/sshLimit';
import { SetupFooter } from '../SetupFooter';
import { useAddOwnComputer } from '../answers/addOwnComputer';
import { useAnswerFlow } from '../answers/flow';
import { useUser } from '@/hooks/useUser';
import { SetupNote, StepError, StepLoading } from '../answers/parts';

/**
 * A computer over SSH, added inside setup: the same pairing and ready
 * summary the Computers panel's form shows (`SshSetup`), and the same
 * create, with Home placed as the answers say, after which setup goes on to
 * its next step.
 */
export default function SshStep() {
  const { user } = useUser();
  // Keyed by the account: the machine probed and added here is its own.
  return <SshPage key={user?.user_id ?? ''} />;
}

function SshPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const headingId = useId();
  const flow = useAnswerFlow('computer');
  const ssh = useSshSetup(true);
  const add = useAddOwnComputer(flow.answers?.runsOn);

  const send = (takeOver: string | null = null) => {
    if (!ssh.create) return;
    add.mutate(ownMachineCreate(ssh.create, { takeOver }), { onSuccess: () => void flow.finish() });
  };
  const error = add.error ? createRefusalMessage(add.error, t, 'standard') : null;
  const takeOver = folderMarkedOwner(add.error);
  const busy = add.isPending || flow.finishing;
  // Home goes where the answers say, so nothing is added before they are read.
  if (flow.isLoading) return <StepLoading />;

  return (
    <div>
      <WizardStepHeader titleId={headingId} title={t('setup:answers.ssh.title')} helper={t('setup:answers.ssh.lead')} />
      <SshSetup state={ssh} busy={busy} />
      <SetupNote icon={<Laptop />}>
        {ssh.ready && !ssh.readyStep.filesBackup
          ? t('setup:answers.computer.noBackupNote')
          : ssh.defaultName
            ? t('setup:answers.computer.onlineNoteNamed', { name: ssh.defaultName })
            : t('setup:answers.computer.onlineNote')}
      </SetupNote>
      {error && !busy && (
        <CreateRefusal
          className="mt-4"
          message={error}
          onTakeOver={takeOver && ssh.create ? () => send(takeOver) : undefined}
        />
      )}
      {flow.finishFailed && <StepError>{t('setup:answers.saveFailed')}</StepError>}
      <SetupFooter
        back={{ label: t('setup:back'), onClick: () => navigate('/setup/computer') }}
        primary={{
          label: busy ? t('computer.adding') : t('computer.add'),
          // Added already, only the record's write is tried again.
          onClick: () => (add.isSuccess ? void flow.finish() : send()),
          disabled: (!ssh.create && !add.isSuccess) || busy,
          busy,
        }}
      />
    </div>
  );
}
