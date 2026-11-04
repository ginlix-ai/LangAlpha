import { useEffect, useId, useState } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import type { KeyOption, ProviderGroup } from '@/components/modelAccess/providerOptions';
import { ClaudeCodeHostedChoice } from './ClaudeCodeHostedChoice';
import { KeySheet } from './KeySheet';

/**
 * Claude Code's sheet in Model access, where the server lets the user choose
 * how it runs on cloud computers. The Anthropic key opens over the sheet,
 * which stays mounted under it, so a pick that waits on the key
 * (`ClaudeCodeHostedChoice`) carries on once it is saved; while the key has
 * no form to open (the provider catalog unread), the pick is made as it is. `onCover` says
 * whether something is open over it. What opens over it is portaled out of
 * its panel, which goes inert and fades while covered.
 */
export function ClaudeCodeSheet({ anthropicKey, configured, onCover }: {
  anthropicKey: { group: ProviderGroup; option: KeyOption } | null;
  configured: ConfiguredProvider[];
  onCover: (covered: boolean) => void;
}) {
  const { t } = useTranslation();
  const keyTitleId = useId();
  const [keyThen, setKeyThen] = useState<(() => void) | null>(null);
  const covered = keyThen !== null;
  useEffect(() => {
    onCover(covered);
    return () => onCover(false);
  }, [covered, onCover]);

  return (
    <div className="flex flex-col gap-5">
      <ClaudeCodeHostedChoice onAddApiKey={(then) => (anthropicKey ? setKeyThen(() => then) : then())} />

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
    </div>
  );
}
