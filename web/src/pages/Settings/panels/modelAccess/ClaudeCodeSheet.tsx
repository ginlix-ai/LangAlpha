import { useEffect, useId, useState } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { ClaudeCodeTokenModal, TokenActions } from '@/components/modelAccess/ClaudeCodeToken';
import { ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import type { TokenRow } from '@/components/modelAccess/tokenLine';
import { SAVED_TOKEN, type SheetAction } from '@/components/modelAccess/tokenTarget';
import type { KeyOption, ProviderGroup } from '@/components/modelAccess/providerOptions';
import { CLAUDE_CODE, CLAUDE_CODE_CONNECTION } from '@/lib/claudeCode';
import { AccessRow, Group } from './AccessRows';
import { ClaudeCodeHostedChoice } from './ClaudeCodeHostedChoice';
import { KeySheet } from './KeySheet';

/**
 * Claude Code's sheet in Model access, where the server lets the user choose
 * how it runs on cloud computers: that choice, then the setup token it saved
 * for them, with its own buttons. The Anthropic key and the token each open
 * over the sheet, which stays mounted under them, so a pick that waits on one
 * (`ClaudeCodeHostedChoice`) carries on once it is saved; while the key has
 * no form to open (the provider catalog unread), a pick that waits on it goes
 * on at once. `onCover` says
 * whether something is open over it. What opens over it is portaled out of
 * its panel, which goes inert and fades while covered.
 */
export function ClaudeCodeSheet({ savedRow, anthropicKey, configured, onRemoveToken, onCover }: {
  savedRow: TokenRow | undefined;
  anthropicKey: { group: ProviderGroup; option: KeyOption } | null;
  configured: ConfiguredProvider[];
  onRemoveToken: () => void;
  onCover: (covered: boolean) => void;
}) {
  const { t } = useTranslation();
  const keyTitleId = useId();
  const [keyThen, setKeyThen] = useState<(() => void) | null>(null);
  const [tokenAction, setTokenAction] = useState<SheetAction | null>(null);
  const [asking, setAsking] = useState(false);
  const covered = keyThen !== null || tokenAction !== null || asking;
  useEffect(() => {
    onCover(covered);
    return () => onCover(false);
  }, [covered, onCover]);

  return (
    <div className="flex flex-col gap-5">
      <ClaudeCodeHostedChoice
        onAddApiKey={(then) => (anthropicKey ? setKeyThen(() => then) : then())}
        onAddToken={() => setTokenAction('add')}
        onAsking={setAsking}
      />
      {savedRow && (
        <Group title={t('modelAccess.claudeCode.savedTag')} empty="">
          {[(
            <AccessRow
              key="saved"
              provider={CLAUDE_CODE}
              name={CLAUDE_CODE_CONNECTION.display_name}
              detail={savedRow.line.detail}
              wrapDetail
              status={savedRow.line.status}
              action={<TokenActions actions={savedRow.line.actions} onOpen={setTokenAction} onRemove={onRemoveToken} />}
            />
          )]}
        </Group>
      )}

      {keyThen && anthropicKey && createPortal(
        <ModalShell
          labelId={keyTitleId}
          title={(
            <ProviderTitle
              name={anthropicKey.group.name}
              markProvider={anthropicKey.group.markProvider}
              title={t('setup.connectTitle', { provider: anthropicKey.group.name })}
            />
          )}
          onClose={() => setKeyThen(null)}
          width="wide"
          stacked
        >
          <KeySheet
            option={anthropicKey.option}
            group={anthropicKey.group}
            configured={configured}
            onSaved={() => {
              keyThen();
              setKeyThen(null);
            }}
            onCancel={() => setKeyThen(null)}
          />
        </ModalShell>,
        document.body,
      )}
      {tokenAction && createPortal(
        <ClaudeCodeTokenModal target={SAVED_TOKEN} action={tokenAction} onClose={() => setTokenAction(null)} />,
        document.body,
      )}
    </div>
  );
}
