import { useId } from 'react';
import { Navigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { WizardStepHeader } from '@/components/ui/wizard';
import { SetupFooter } from '../SetupFooter';
import { useAnswerFlow } from '../answers/flow';
import { AnswersStrip, StepError, StepLoading } from '../answers/parts';
import { ServerSetupForm } from '../components/ServerSetupForm';

/**
 * The server's own step, ahead of the account's on a self-hosted server not set
 * up yet: where it runs agent code, for an answer that runs tasks on a computer
 * the server runs, and the keys its tools use, for every answer. Answered once
 * for everyone there, so it saves to the server rather than to the account.
 */
export default function ServerStep() {
  const flow = useAnswerFlow('server');
  const { t } = useTranslation();
  const headingId = useId();
  if (flow.isLoading) return <StepLoading />;
  // Not on the rail where the server has no step: a hosted build, or a server
  // set up before it.
  if (!flow.answers || !flow.rail.includes('server')) return <Navigate to="/setup" replace />;
  const choosesComputer = flow.answers.runsOn === 'cloud';
  return (
    <div>
      {flow.first && <AnswersStrip answers={flow.answers} />}
      <WizardStepHeader
        titleId={headingId}
        title={t('setup:server.title')}
        helper={t(choosesComputer ? 'setup:server.helper' : 'setup:server.helperKeys')}
      />
      <ServerSetupForm
        layout="step"
        choosesComputer={choosesComputer}
        onFailed={<SetupFooter primary={{ label: t('setup:continue'), onClick: () => void flow.finish() }} />}
        footer={(step) => (
          <>
            {flow.finishFailed && <StepError>{t('setup:answers.saveFailed')}</StepError>}
            <SetupFooter
              back={flow.back ? { label: t('setup:back'), onClick: flow.back } : null}
              primary={{
                label: t('setup:continue'),
                disabled: !step.ready || step.saving || flow.finishing,
                busy: step.saving || flow.finishing,
                onClick: () => void step.run().then((saved) => (saved ? flow.finish() : false)),
              }}
            />
          </>
        )}
      />
    </div>
  );
}
