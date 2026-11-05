import { useEffect, useId, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router';
import { ExternalLink, KeyRound, Laptop, SquareTerminal } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { buttonClass } from '@/components/ui/button-class';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ClaudeCodeComputer, ClaudeCodeSetup } from '@/api/model';
import { DESKTOP_DOWNLOAD_URL } from '@/config/supportLinks';
import { isPlatformMode } from '@/config/hostMode';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useNow } from '@/hooks/useNow';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { ClaudeCodeTokenModal, TokenActions } from '@/components/modelAccess/ClaudeCodeToken';
import { ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import { QuietNote, ROW_BUTTON, StatusLabel, type RowStatus } from '@/components/modelAccess/parts';
import type { TokenAction, TokenRow } from '@/components/modelAccess/tokenLine';
import { SAVED_TOKEN, computerTokenTarget, type SheetAction, type TokenTarget } from '@/components/modelAccess/tokenTarget';
import type { KeyOption, ProviderGroup } from '@/components/modelAccess/providerOptions';
import { canRemoteControl } from '@/lib/desktop';
import { cn } from '@/lib/utils';
import { CLAUDE_CODE_DOCS_URL, CLAUDE_CODE_FEATURE, CLAUDE_CODE_REQUIREMENTS_URL } from '@/lib/claudeCode';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import { openComputersPanel } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { useSaveFailedToast } from '../usePreferenceWrite';
import { Group } from './AccessRows';
import { ClaudeCodeHostedChoice } from './ClaudeCodeHostedChoice';
import { claudeCodeHeadline } from './claudeCodeHeadline';
import { KeySheet } from './KeySheet';

const S = 'modelAccess.claudeCode.setup';

/** The states where nothing the user has can run Claude Code yet. */
const CANNOT_RUN_YET = new Set<ClaudeCodeSetup['state']>(['no_computer', 'unsupported', 'offline']);

const glyph = (Icon: typeof Laptop) => <Icon aria-hidden="true" className="h-4 w-4" style={{ color: 'var(--color-text-secondary)' }} />;
const iconOf = (kind: ClaudeCodeComputer['kind'] | undefined) => glyph(kind === 'helper' ? Laptop : SquareTerminal);

/**
 * A row of the view (a computer, the saved token, a way to connect a
 * computer) on one line: the tile, the name over its detail, the status,
 * then its button. The rows of the cards stacked in one `ROWS` grid share
 * its columns, so every row's status and button start at the same x, and
 * only the detail wraps.
 */
function SetupRow({ icon, name, detail, status, action }: {
  icon: ReactNode;
  name: string;
  detail: ReactNode;
  status?: RowStatus;
  action: ReactNode;
}) {
  return (
    <li className="col-span-full grid grid-cols-[auto_minmax(0,1fr)_auto_auto] items-center gap-x-3 px-4 py-3 supports-[grid-template-columns:subgrid]:grid-cols-subgrid">
      <IdentityTile name={name} size="sm">{icon}</IdentityTile>
      <div className="flex min-w-0 flex-col gap-0.5">
        <span className="text-sm font-medium wrap-break-word" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
        {detail && <span className="text-xs wrap-break-word" style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>}
      </div>
      {status ? <StatusLabel status={status} className="whitespace-nowrap" /> : <span />}
      <div className="flex">{action}</div>
    </li>
  );
}

/** The grid the cards of `SetupRow`s stack in: tile, text, status, button.
 *  The status and button columns take the widest the view shows, so the
 *  text gives way to them and no row's controls wrap. */
const ROWS = 'grid grid-cols-[auto_minmax(0,1fr)_auto_auto] gap-x-3 gap-y-5';

/**
 * Claude Code in Model access, opened from its row under Connect a
 * subscription or in the list: what Claude Code mode is, where it stands
 * as a whole (`setup`), and what to do next. Before the
 * user turns it on, each computer reads only reachable or not, and adding a
 * token there turns it on first, which is how it is turned on; Turn off
 * turns it off again. Once on, each computer's token line (`tokenRows`)
 * shows with its own buttons, the saved token beside them where this server
 * runs turns on it, and the choice of how it runs on cloud computers where
 * the server offers one. Where nothing the user has can run it, it says why
 * and offers the ways to connect a computer.
 *
 * Where the server no longer offers Claude Code at all (`setup` null), only
 * tokens left behind show, each offered for removal.
 *
 * The Anthropic key, a token's sheet and the turn-off question each open
 * over the view, which stays mounted under them, so a choice that waits on
 * the key carries on once it is saved. `onCover` says whether something is
 * open over it; what opens is portaled out of its panel, which goes inert
 * and fades while covered.
 */
export function ClaudeCodeSheet({ setup, tokenRows, anthropicKey, configured, onRemoveToken, onCover }: {
  setup: ClaudeCodeSetup | null;
  tokenRows: TokenRow[];
  anthropicKey: { group: ProviderGroup; option: KeyOption } | null;
  configured: ConfiguredProvider[];
  onRemoveToken: (target: TokenTarget) => void;
  onCover: (covered: boolean) => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const keyTitleId = useId();
  const offTitleId = useId();
  const toggle = useSetFeatureOverride();
  const { custody, isLoading: custodyLoading } = useClaudeCodeCustody();
  const claudeCodeStatus = useClaudeCodeStatus();
  // Turning off deletes a token stored for cloud computers, whether or not a
  // choice is still offered: with none read, warn of it wherever a hosted
  // server keeps the user's token.
  const storedMayGo = custody ? custody.mode === 'stored' : isPlatformMode && claudeCodeStatus.saved;
  const now = useNow();
  // An operator may gate the flag so the user cannot turn it on or off
  // (`none`, or a plan); only an opt-in or opt-out gate takes their pick,
  // so a gate not yet read takes none.
  const gate = useFeatures().data?.find((f) => f.key === CLAUDE_CODE_FEATURE)?.gate;
  const toggleable = gate === 'opt_in' || gate === 'opt_out';
  const toastSaveFailed = useSaveFailedToast();
  const [keyThen, setKeyThen] = useState<(() => void) | null>(null);
  const [token, setToken] = useState<{ target: TokenTarget; action: SheetAction } | null>(null);
  const [asking, setAsking] = useState(false);
  const [turningOff, setTurningOff] = useState(false);
  const covered = keyThen !== null || token !== null || asking || turningOff;
  useEffect(() => {
    onCover(covered);
    return () => onCover(false);
  }, [covered, onCover]);

  const turn = (enabled: boolean) => toggle.mutateAsync({ key: CLAUDE_CODE_FEATURE, enabled }).then(() => true, () => {
    toastSaveFailed();
    return false;
  });
  // Adding the first token is what turns Claude Code on: on first, then its sheet.
  // With the setup unread, the status read says whether it is on.
  const on = setup?.on ?? claudeCodeStatus.enabled;
  const canAdd = on || toggleable;
  const openToken = async (target: TokenTarget, action: SheetAction) => {
    if (!on && !(await turn(true))) return;
    setToken({ target, action });
  };
  const onComputers = (panel: Parameters<typeof openComputersPanel>[0]) => {
    navigate('/chat');
    openComputersPanel(panel);
  };
  const viewComputer = (computerId: string) => (
    <Button type="button" variant="outline" size="sm" className={ROW_BUTTON} onClick={() => onComputers({ view: 'details', computerId })}>
      {t(S + '.viewComputer')}
    </Button>
  );
  const actions = (list: TokenAction[], target: TokenTarget) => list.length > 0 && (
    <TokenActions actions={list} onOpen={(action) => void openToken(target, action)} onRemove={() => onRemoveToken(target)} />
  );

  const headline = setup && claudeCodeHeadline(t, setup, now);
  const kindOf = new Map((setup?.computers ?? []).map((c) => [c.computer_id, c.kind]));
  // A saved token left behind after Claude Code was turned off still shows, to be removed.
  const savedRow = tokenRows.find(({ key }) => key === 'saved');
  // Once on, each computer's own line, read as offline where the setup read
  // found it out of reach; before, only whether it can be reached. With
  // Claude Code gone from the server, the lines of tokens left behind.
  const unreached = new Set((setup?.computers ?? []).filter((c) => c.state === 'offline').map((c) => c.computer_id));
  const offlineDetail = (kind: ClaudeCodeComputer['kind'] | undefined) => t(kind === 'helper' ? S + '.offline.helper' : S + '.offline.ssh');
  const computerRows = on || !setup
    ? tokenRows.filter((row) => row.target.kind === 'computer').map(({ key, target, tag, line }) => {
      const offline = line.status === 'offline' || unreached.has(key);
      return (
        <SetupRow
          key={key}
          icon={iconOf(kindOf.get(key))}
          name={tag ?? ''}
          detail={offline && line.status !== 'offline' ? offlineDetail(kindOf.get(key)) : line.detail}
          status={offline ? 'offline' : line.status}
          action={offline ? viewComputer(key) : actions(line.actions, target)}
        />
      );
    })
    : setup.computers.map((c) => {
      if (c.state === 'offline') {
        return (
          <SetupRow
            key={c.computer_id}
            icon={iconOf(c.kind)}
            name={c.name}
            detail={offlineDetail(c.kind)}
            status="offline"
            action={viewComputer(c.computer_id)}
          />
        );
      }
      // A token left on the computer is offered for removal; Turn on uses it again.
      const kept = c.source === 'machine';
      return (
        <SetupRow
          key={c.computer_id}
          icon={iconOf(c.kind)}
          name={c.name}
          detail={t(kept ? 'modelAccess.claudeCode.optOut.computer' : 'modelAccess.claudeCode.computer.noToken')}
          status={kept ? 'off' : 'not_connected'}
          action={actions(kept ? ['remove'] : canAdd ? ['add'] : [], computerTokenTarget(c))}
        />
      );
    });
  // Before it is on, a server that runs turns on a saved token offers to add one.
  const offeredSaved = !!setup && !on && canAdd && setup.saved_token_runs && !savedRow;
  // Turning it on is the step where a token was left on a computer, which it
  // uses again, where cloud computers offer this user a way to run it, whose
  // choice shows once on, or where nothing can be added (only a cloud
  // computer would run it).
  const offerTurnOn = !!setup && !on && canAdd && setup.state === 'not_connected' && (
    setup.computers.some((c) => c.source === 'machine')
    || setup.cloud
    || (!offeredSaved && !setup.computers.some((c) => c.state === 'off' && c.source !== 'machine'))
  );
  const remoteControl = canRemoteControl();

  return (
    <div className="flex flex-col gap-5">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t(S + '.lead')}{' '}
        <a
          href={setup && CANNOT_RUN_YET.has(setup.state) ? CLAUDE_CODE_REQUIREMENTS_URL : CLAUDE_CODE_DOCS_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="tap-target inline-flex items-center gap-0.5 underline underline-offset-2 hover:opacity-80"
        >
          {t(S + '.learnMore')}
          <ExternalLink aria-hidden="true" className="h-3 w-3" />
        </a>
      </p>

      {headline && (
        <div className="flex flex-col gap-1">
          <h3 className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{headline.title}</h3>
          <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{headline.body}</div>
        </div>
      )}

      {!setup && tokenRows.length === 0 && <QuietNote>{t('modelAccess.status.unknownLong')}</QuietNote>}

      {(computerRows.length > 0 || savedRow || offeredSaved) && (
        <div className={ROWS}>
          {computerRows.length > 0 && <Group subgrid title={t(S + '.yourComputers')} empty="">{computerRows}</Group>}
          {(savedRow || offeredSaved) && (
            <Group subgrid title={t('modelAccess.claudeCode.savedTag')} empty="">
              {[(
                <SetupRow
                  key="saved"
                  icon={glyph(KeyRound)}
                  name={t(S + '.savedName')}
                  detail={savedRow ? savedRow.line.detail : t('modelAccess.claudeCode.detail')}
                  status={savedRow ? savedRow.line.status : 'not_connected'}
                  action={actions(savedRow ? savedRow.line.actions : ['add'], SAVED_TOKEN)}
                />
              )]}
            </Group>
          )}
        </div>
      )}

      {on && (
        <ClaudeCodeHostedChoice
          onAddApiKey={(then) => (anthropicKey ? setKeyThen(() => then) : then())}
          onAddToken={() => setToken({ target: SAVED_TOKEN, action: 'add' })}
          onAsking={setAsking}
        />
      )}

      {(setup?.state === 'no_computer' || setup?.state === 'unsupported') && (
        <div className={ROWS}>
          <Group subgrid title={t(S + '.waysTitle')} empty="">
            {[
              <SetupRow
                key="mac"
                icon={iconOf('helper')}
                name={t(S + '.mac.name')}
                detail={t(remoteControl ? S + '.mac.noteDesktop' : S + '.mac.noteBrowser')}
                action={remoteControl ? (
                  <Button type="button" variant="outline" size="sm" className={ROW_BUTTON} onClick={() => onComputers({ view: 'create', pick: 'thisMac' })}>
                    {t(S + '.mac.turnOn')}
                  </Button>
                ) : (
                  <a
                    href={DESKTOP_DOWNLOAD_URL}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={buttonClass({ variant: 'outline', size: 'sm', className: cn('gap-1.5', ROW_BUTTON) })}
                  >
                    {t(S + '.mac.getApp')}
                    <ExternalLink aria-hidden="true" className="h-3.5 w-3.5" />
                  </a>
                )}
              />,
              <SetupRow
                key="ssh"
                icon={iconOf('ssh')}
                name={t(S + '.ssh.name')}
                detail={t(S + '.ssh.note')}
                action={(
                  <Button type="button" variant="outline" size="sm" className={ROW_BUTTON} onClick={() => onComputers({ view: 'create' })}>
                    {t(S + '.ssh.add')}
                  </Button>
                )}
              />,
            ]}
          </Group>
        </div>
      )}

      {setup?.state === 'no_computer' && isPlatformMode && !setup.cloud && <QuietNote>{t(S + '.cloudNotYet')}</QuietNote>}

      {offerTurnOn && (
        <Button type="button" className="tap-target self-start" size="sm" disabled={toggle.isPending} onClick={() => void turn(true)}>
          {t(S + '.turnOn')}
        </Button>
      )}
      {on && toggleable && (
        <Button type="button" variant="outline" size="sm" className={cn('self-start', ROW_BUTTON)} onClick={() => setTurningOff(true)}>
          {t(S + '.turnOff.action')}
        </Button>
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
      {token && createPortal(
        <ClaudeCodeTokenModal target={token.target} action={token.action} onClose={() => setToken(null)} />,
        document.body,
      )}
      {turningOff && createPortal(
        <ModalShell
          labelId={offTitleId}
          title={t(S + '.turnOff.title')}
          onClose={() => setTurningOff(false)}
          width="narrow"
          stacked
          footer={(
            <div className="flex justify-end gap-2">
              <Button type="button" variant="ghost" size="sm" className="tap-target" onClick={() => setTurningOff(false)}>{t('common.cancel')}</Button>
              <Button
                type="button"
                size="sm"
                className="tap-target"
                disabled={toggle.isPending || custodyLoading}
                onClick={() => void turn(false).then((done) => done && setTurningOff(false))}
              >
                {t(S + '.turnOff.confirm')}
              </Button>
            </div>
          )}
        >
          <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
            {t(storedMayGo ? S + '.turnOff.bodyStored' : S + '.turnOff.body')}
          </div>
        </ModalShell>,
        document.body,
      )}
    </div>
  );
}
