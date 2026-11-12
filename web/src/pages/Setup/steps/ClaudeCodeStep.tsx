import { useId, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { WizardStepHeader } from '@/components/ui/wizard';
import { RemoveClaudeCodeTokenDialog } from '@/components/modelAccess/ClaudeCodeToken';
import { useClaudeCodeTokenRows } from '@/components/modelAccess/tokenLine';
import type { TokenTarget } from '@/components/modelAccess/tokenTarget';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { CLAUDE_CODE, CLAUDE_CODE_CONNECTION } from '@/lib/claudeCode';
import { ClaudeCodeSheet } from '@/pages/Settings/panels/modelAccess/ClaudeCodeSheet';
import { SetupFooter } from '../SetupFooter';
import { useProviderConnected } from './connectStep/shared';

const noCover = () => {};

/**
 * Claude Code inside setup: the view Settings opens for it, where a token is
 * added and where it runs is chosen, then on to its models (or back to the
 * page that sent the user here) once it is connected. Its own address, so
 * the account portal can link straight to it.
 */
export default function ClaudeCodeStep() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const connected = useProviderConnected();
  const headingId = useId();
  const { setup, isLoading } = useClaudeCodeSetup();
  const tokenRows = useClaudeCodeTokenRows();
  const [removing, setRemoving] = useState<TokenTarget | null>(null);
  const name = CLAUDE_CODE_CONNECTION.display_name;
  const ready = !!setup?.on && setup.state === 'connected';

  return (
    <div>
      <WizardStepHeader titleId={headingId} title={t('setup.connectTitle', { provider: name })} />
      {isLoading && !setup ? (
        <div className="flex items-center justify-center py-12">
          <Loader size={20} className="text-(--color-text-tertiary)" label={t('common.loading')} />
        </div>
      ) : (
        <ClaudeCodeSheet setup={setup} tokenRows={tokenRows} onRemoveToken={setRemoving} onCover={noCover} />
      )}
      {removing && <RemoveClaudeCodeTokenDialog target={removing} onClose={() => setRemoving(null)} />}
      <SetupFooter
        back={{ label: t('setup.back'), onClick: () => navigate('/setup/provider', { state: { list: 'oauth' } }) }}
        primary={{
          label: t('setup.continue'),
          disabled: !ready,
          onClick: () => connected({
            method: 'oauth', list: 'oauth', provider: CLAUDE_CODE, displayName: name, modelsFrom: CLAUDE_CODE, justConnected: true,
          }),
        }}
      />
    </div>
  );
}
