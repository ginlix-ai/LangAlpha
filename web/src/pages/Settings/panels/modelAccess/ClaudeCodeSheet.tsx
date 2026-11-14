import { useContext, useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { Cloud, ExternalLink, KeyRound, Laptop, SquareTerminal } from 'lucide-react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { buttonClass } from '@/components/ui/button-class';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ClaudeCodeComputer, ClaudeCodePlan, ClaudeCodeSetup } from '@/api/model';
import { DESKTOP_DOWNLOAD_URL } from '@/config/supportLinks';
import { isPlatformMode } from '@/config/hostMode';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useNow } from '@/hooks/useNow';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { ClaudeCodeTokenModal, TokenActions } from '@/components/modelAccess/ClaudeCodeToken';
import { CheckNowButton, CheckNowMenuItem } from '@/components/modelAccess/PlanCheck';
import { UsageCells, UsageDetail, UsageHeaderRow } from '@/components/modelAccess/PlanUsage';
import { UsageTableContext, VIEW_WIDTHS, useUsageSteps } from '@/components/modelAccess/usageSteps';
import { usePlanCheck, type CheckTarget } from '@/components/modelAccess/usePlanCheck';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import { credentialKey, planWindows } from '@/lib/claudeCodePlan';
import { QuietNote, ROW_BUTTON, StatusLabel, type RowStatus } from '@/components/modelAccess/parts';
import type { TokenAction, TokenRow } from '@/components/modelAccess/tokenLine';
import { SAVED_TOKEN, computerTokenTarget, type SheetAction, type TokenTarget } from '@/components/modelAccess/tokenTarget';
import { canRemoteControl } from '@/lib/desktop';
import { cn } from '@/lib/utils';
import { useAllModels } from '@/hooks/useAllModels';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useDefaultModel } from '@/hooks/useSlotDefaultModels';
import { CLAUDE_CODE, CLAUDE_CODE_DOCS, CLAUDE_CODE_FEATURE, claudeCodeStartModel } from '@/lib/claudeCode';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import { MediumActionButton } from '@/pages/ChatAgent/components/MediumActionLink';
import { openComputersPanel, useComputersPanel } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { useMediumAction, type MediumAction } from '@/pages/ChatAgent/hooks/useMediumAction';
import { useSaveFailedToast } from '../usePreferenceWrite';
import { Group } from './AccessRows';
import { ClaudeCodeHostedChoice } from './ClaudeCodeHostedChoice';
import { ClaudeCodePlanOutChoice } from './ClaudeCodePlanOutChoice';
import { ComputerClis } from './ComputerClis';
import { claudeCodeHeadline } from './claudeCodeHeadline';

const S = 'modelAccess.claudeCode.setup';
const M = 'modelAccess.claudeCode.medium';

/** The docs section the view's Learn more opens, by what the view asks of
 *  the user: what a computer needs, where it runs while one is out of reach,
 *  else how it runs. */
const LEARN_MORE: Partial<Record<ClaudeCodeSetup['state'], string>> = {
  no_computer: CLAUDE_CODE_DOCS.requirements,
  unsupported: CLAUDE_CODE_DOCS.requirements,
  offline: CLAUDE_CODE_DOCS.whereItRuns,
};

const glyph = (Icon: typeof Laptop) => <Icon aria-hidden="true" className="h-4 w-4" style={{ color: 'var(--color-text-secondary)' }} />;
const iconOf = (kind: ClaudeCodeComputer['kind'] | undefined) => glyph(kind === 'helper' ? Laptop : SquareTerminal);

/**
 * A row of the view (a computer, the saved token, a way to connect a
 * computer) on one line: the tile, the name over its detail, the status,
 * then its button. The rows of the cards stacked in one `ROWS` grid share
 * its columns, so every row's status and button start at the same x, and
 * only the detail wraps.
 *
 * In the token cards' usage table (`VIEW_WIDTHS`) a row whose token runs
 * (`plan` given) keeps its detail on one line and lays its plan's use out
 * on the table's columns, or, where its token reported no use yet, says
 * so. A row with no use to show takes the usage columns for its text, so
 * its detail has the room it has outside the table, and wraps only where it
 * would there. A computer's own setting (`setting`) sits under its detail,
 * and in the table on a line under the row (`below`), since the usage
 * columns leave the text too narrow for it.
 */
function SetupRow({ icon, name, detail, status, action, plan, setting }: {
  icon: ReactNode;
  name: string;
  detail: ReactNode;
  status?: RowStatus;
  action: ReactNode;
  plan?: ClaudeCodePlan | null;
  setting?: ReactNode;
}) {
  const steps = useContext(UsageTableContext);
  const clock = usePlanClock();
  if (steps) {
    const runs = plan !== undefined;
    const used = planWindows(plan, clock.now).length > 0;
    const pending = runs && status === 'connected' && !used;
    return (
      <li className={cn('col-span-full flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3', steps.row)}>
        <IdentityTile name={name} size="sm">{icon}</IdentityTile>
        <div className={cn('flex min-w-0 flex-1 flex-col gap-0.5', !used && steps.spanUsage)}>
          <span className="text-sm font-medium wrap-break-word" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
          {(detail || pending) && (
            <span data-row-detail className={cn('text-xs', !runs && 'text-pretty wrap-break-word')} style={{ color: 'var(--color-text-tertiary)' }}>
              {runs ? <UsageDetail text={detail} plan={plan} tail={pending ? 'pending' : status === 'connected' ? 'bars' : null} whole={status === 'limit_reached'} /> : detail}
            </span>
          )}
        </div>
        {used && <UsageCells plan={plan} />}
        <div className={cn('flex basis-full items-center justify-end gap-3', steps.tail)}>
          {status ? <StatusLabel status={status} className="whitespace-nowrap" /> : <span />}
          <div className="flex">{action}</div>
        </div>
        {setting && <div className={steps.below}>{setting}</div>}
      </li>
    );
  }
  return (
    <li className="col-span-full grid grid-cols-[auto_minmax(0,1fr)_auto_auto] items-center gap-x-3 px-4 py-3 supports-[grid-template-columns:subgrid]:grid-cols-subgrid">
      <IdentityTile name={name} size="sm">{icon}</IdentityTile>
      <div className="flex min-w-0 flex-col gap-0.5">
        <span className="text-sm font-medium wrap-break-word" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
        {detail && <span className="text-xs text-pretty wrap-break-word" style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>}
        {setting}
      </div>
      {status ? <StatusLabel status={status} className="whitespace-nowrap" /> : <span />}
      {/* On a phone the button goes under the text, which keeps the width. */}
      <div className="flex max-sm:col-[2/-1] max-sm:mt-2">{action}</div>
    </li>
  );
}

/**
 * A new Chief of Staff thread, whose turns run where the headline says, with
 * its composer on the Claude Code model `claudeCodeStartModel` picks. Where
 * Home cannot be read, the chat page.
 */
function StartChatButton({ onLeave }: { onLeave?: () => void }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  // Until the models are read the pick is unknown, and a chat would start
  // on the Default.
  const { models, metadata, isLoading } = useAllModels();
  const accountDefault = useDefaultModel();
  const model = claudeCodeStartModel(models[CLAUDE_CODE]?.models ?? [], metadata, accountDefault);
  // A Home read that lands after the user left this view takes them nowhere.
  const shown = useRef(true);
  useEffect(() => {
    shown.current = true;
    return () => {
      shown.current = false;
    };
  }, []);
  // No Claude Code model to start on (the catalog could not be read, or
  // lists none): a chat here would start on the Default.
  if (!isLoading && !model) return null;
  const start = () => {
    queryClient.fetchQuery(homeWorkspaceQuery(queryClient)).then(
      (home) => {
        if (!shown.current) return;
        onLeave?.();
        navigate('/chat/t/__default__', {
          state: { workspaceId: home.workspace_id, workspaceName: t('agents.chiefOfStaff'), model },
        });
      },
      () => {
        if (!shown.current) return;
        onLeave?.();
        navigate('/chat');
      },
    );
  };
  return (
    <Button type="button" size="sm" className="tap-target mt-2 self-start" disabled={isLoading} onClick={start}>
      {t(S + '.connected.startChat')}
    </Button>
  );
}

/** What a cloud computer too small for Claude Code says under its name, by
 *  what gets it to Medium (`useMediumAction`). */
function mediumDetail(t: TFunction, action: MediumAction | null): string {
  if (!action) return t(M + '.row');
  if (action.kind === 'plan') return t(M + '.rowPlan');
  return t(action.kind === 'resizing' ? M + '.rowResizing' : M + '.rowResize');
}

/** A cloud computer too small for Claude Code, with what gets it to Medium. */
function TooSmallRow({ computerId, name }: { computerId: string; name: string }) {
  const { t } = useTranslation();
  const action = useMediumAction(computerId);
  return (
    <SetupRow
      icon={glyph(Cloud)}
      name={name}
      detail={mediumDetail(t, action)}
      action={action ? <MediumActionButton action={action} className={ROW_BUTTON} /> : <span />}
    />
  );
}

/** A token row's buttons, with Check now in its More actions menu where
 *  the usage table is too narrow for its header row (`checkTarget`). */
function RowActions({ actions, checkTarget, onOpen, onRemove }: {
  actions: TokenAction[];
  checkTarget: CheckTarget | null;
  onOpen: (action: SheetAction) => void;
  onRemove: () => void;
}) {
  const check = usePlanCheck(checkTarget ? [checkTarget] : []);
  return (
    <TokenActions actions={actions} onOpen={onOpen} onRemove={onRemove} menuItems={checkTarget && <CheckNowMenuItem check={check} />} />
  );
}

/** Whether a row's token runs, so Check now can read its plan: it works or
 *  is at a limit, on a computer that is not offline, and a check would not
 *  be billed to usage credits. */
const checkable = ({ line, checkBills }: TokenRow, offline: boolean) =>
  !offline && !checkBills && (line.status === 'connected' || line.status === 'limit_reached');

/** One target per credential: computers on the relayed saved token share its plan. */
function checkTargets(rows: { row: TokenRow; offline: boolean }[]): CheckTarget[] {
  const seen = new Set<string>();
  return rows.flatMap(({ row, offline }) => {
    const { credential, checkAfter } = row;
    if (!credential || !checkable(row, offline) || seen.has(credentialKey(credential))) return [];
    seen.add(credentialKey(credential));
    return [{ credential, checkAfter }];
  });
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
 * and offers the ways to connect a computer. Each cloud computer too small
 * for it is named, with where to get a larger one (`biggerComputer`): the
 * plan page where the build names one, else Change spec on the computer.
 *
 * Where the server no longer offers Claude Code at all (`setup` null), only
 * tokens left behind show, each offered for removal.
 *
 * A token's sheet and the turn-off question each open over the view, which
 * stays mounted under them. `onCover` says whether something is open over
 * it; what opens is portaled out of its panel, which goes inert and fades
 * while covered.
 */
export function ClaudeCodeSheet({ setup, tokenRows, onRemoveToken, onCover, onLeave }: {
  setup: ClaudeCodeSetup | null;
  tokenRows: TokenRow[];
  onRemoveToken: (target: TokenTarget) => void;
  onCover: (covered: boolean) => void;
  /** Called before a computer action leaves for the computers panel in the
   *  app: setup passes its exit, so the user is not sent back to it. */
  onLeave?: () => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const offTitleId = useId();
  const toggle = useSetFeatureOverride();
  const { custody, isLoading: custodyLoading } = useClaudeCodeCustody();
  const claudeCodeStatus = useClaudeCodeStatus();
  // Turning off deletes a token stored for cloud computers, whether or not a
  // choice is still offered: with none read, warn of it wherever a hosted
  // server keeps the user's token.
  const storedMayGo = custody ? custody.mode === 'stored' : isPlatformMode && claudeCodeStatus.saved;
  // An operator may gate the flag so the user cannot turn it on or off
  // (`none`, or a plan); only an opt-in or opt-out gate takes their pick,
  // so a gate not yet read takes none.
  const gate = useFeatures().data?.find((f) => f.key === CLAUDE_CODE_FEATURE)?.gate;
  const toggleable = gate === 'opt_in' || gate === 'opt_out';
  const toastSaveFailed = useSaveFailedToast();
  const [token, setToken] = useState<{ target: TokenTarget; action: SheetAction } | null>(null);
  const [asking, setAsking] = useState(false);
  const [turningOff, setTurningOff] = useState(false);
  // Change spec opens over the view from the page's own host.
  const resizing = useComputersPanel().specComputerId !== null;
  const covered = token !== null || asking || turningOff || resizing;
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
  const toChat = () => {
    onLeave?.();
    navigate('/chat');
  };
  const onComputers = (panel: Parameters<typeof openComputersPanel>[0]) => {
    toChat();
    openComputersPanel(panel);
  };
  const viewComputer = (computerId: string) => (
    <Button type="button" variant="outline" size="sm" className={ROW_BUTTON} onClick={() => onComputers({ view: 'details', computerId })}>
      {t(S + '.viewComputer')}
    </Button>
  );
  // The token cards are a usage table once Claude Code is on, and Check
  // now leaves its header for each row's menu where the table is too narrow
  // for that header.
  const [table, setTable] = useState<HTMLDivElement | null>(null);
  const steps = useUsageSteps(table, on ? VIEW_WIDTHS : null);
  const actions = (list: TokenAction[], target: TokenTarget, row?: { row: TokenRow; offline: boolean }) => list.length > 0 && (
    <RowActions
      actions={list}
      checkTarget={steps && !steps.columns && row ? checkTargets([row])[0] ?? null : null}
      onOpen={(action) => void openToken(target, action)}
      onRemove={() => onRemoveToken(target)}
    />
  );

  const { cliComputer, computers, clisMax } = useClaudeCodeComputers();
  // How many agents each computer runs at once, set on its row once Claude
  // Code is on and the server offers the setting.
  const clisOf = (computerId: string) => {
    const clis = computers.find((c) => c.computer_id === computerId)?.clis;
    return on && clisMax && clis != null ? <ComputerClis computerId={computerId} clis={clis} max={clisMax} /> : undefined;
  };
  const now = useNow();
  const headline = setup && claudeCodeHeadline(t, setup, now, cliComputer);
  const kindOf = new Map((setup?.computers ?? []).map((c) => [c.computer_id, c.kind]));
  // A saved token left behind after Claude Code was turned off still shows, to be removed.
  const savedRow = tokenRows.find(({ key }) => key === 'saved');
  // Once on, each computer's own line, read as offline where the setup read
  // found it out of reach; before, only whether it can be reached. With
  // Claude Code gone from the server, the lines of tokens left behind.
  const unreached = new Set((setup?.computers ?? []).filter((c) => c.state === 'offline').map((c) => c.computer_id));
  const computerTokenRows = tokenRows
    .filter((row) => row.target.kind === 'computer')
    .map((row) => ({ row, offline: row.line.status === 'offline' || unreached.has(row.key) }));
  // Each card's header offers Check now while a token in it runs.
  const computerTargets = checkTargets(computerTokenRows);
  const savedTargets = savedRow ? checkTargets([{ row: savedRow, offline: false }]) : [];
  const computersCheck = usePlanCheck(computerTargets);
  const savedCheck = usePlanCheck(savedTargets);
  // A card names the windows only where one of its rows has use to show in them.
  const clock = usePlanClock();
  const used = (plan: ClaudeCodePlan | null | undefined) => planWindows(plan, clock.now).length > 0;
  const computersUsed = computerTokenRows.some(({ row, offline }) => !offline && used(row.line.plan));
  const offlineDetail = (kind: ClaudeCodeComputer['kind'] | undefined) => t(kind === 'helper' ? S + '.offline.helper' : S + '.offline.ssh');
  const computerRows = on || !setup
    ? computerTokenRows.map(({ row, offline }) => {
      const { key, target, tag, line } = row;
      return (
        <SetupRow
          key={key}
          icon={iconOf(kindOf.get(key))}
          name={tag ?? ''}
          detail={offline && line.status !== 'offline' ? offlineDetail(kindOf.get(key)) : line.detail}
          status={offline ? 'offline' : line.status}
          plan={offline ? undefined : line.plan}
          action={offline ? viewComputer(key) : actions(line.actions, target, { row, offline })}
          setting={clisOf(key)}
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
  // The ways to connect a computer, where none can run it yet, and where the
  // user chose to run it on their own computers and has none.
  const offerComputers = setup?.state === 'no_computer' || setup?.state === 'unsupported'
    || (on && custody?.mode === 'local' && setup?.computers.length === 0);
  const remoteControl = canRemoteControl();
  // Named wherever its turns would run inside a cloud computer: where the
  // user chose that, or where the server offers no choice. Before they
  // choose it, the choice itself warns of them.
  const tooSmall = custody && custody.mode !== 'stored' ? [] : setup?.too_small ?? [];
  const savedOnCloud = isPlatformMode ? setup?.cloud : setup?.cli_in_sandbox;
  const savedDetail = savedOnCloud ? t(S + '.savedDetailCloud') : !isPlatformMode ? t(S + '.savedDetailServer') : null;

  return (
    <div className="flex flex-col gap-5">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t(S + '.lead')}{' '}
        <a
          href={(setup && LEARN_MORE[setup.state]) ?? CLAUDE_CODE_DOCS.howItRuns}
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
          {headline.runsOn && <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{headline.runsOn}</div>}
          {headline.ready && (
            <StartChatButton onLeave={onLeave} />
          )}
        </div>
      )}

      {!setup && tokenRows.length === 0 && <QuietNote>{t('modelAccess.status.unknownLong')}</QuietNote>}

      {(computerRows.length > 0 || savedRow || offeredSaved) && (
        <div ref={setTable}>
          <UsageTableContext.Provider value={steps}>
            <div className={steps ? cn('grid gap-x-3 gap-y-5', steps.grid) : ROWS}>
              {computerRows.length > 0 && (
                <Group subgrid title={t(S + '.yourComputers')} empty="">
                  {[
                    <UsageHeaderRow
                      key="header"
                      first={t('claudeCodePlan.table.computer')}
                      windows={computersUsed}
                      aside={computerTargets.length > 0 && <CheckNowButton check={computersCheck} />}
                    />,
                    ...computerRows,
                  ]}
                </Group>
              )}
              {(savedRow || offeredSaved) && (
                <Group subgrid title={t('modelAccess.claudeCode.savedTag')} empty="">
                  {[
                    <UsageHeaderRow
                      key="header"
                      first={t('claudeCodePlan.table.token')}
                      windows={!!savedRow && used(savedRow.line.plan)}
                      aside={savedTargets.length > 0 && <CheckNowButton check={savedCheck} />}
                    />,
                    <SetupRow
                      key="saved"
                      icon={glyph(KeyRound)}
                      name={t(S + '.savedName')}
                      detail={savedRow?.line.detail ?? savedDetail}
                      status={savedRow ? savedRow.line.status : 'not_connected'}
                      plan={savedRow?.line.plan}
                      action={actions(savedRow ? savedRow.line.actions : ['add'], SAVED_TOKEN, savedRow && { row: savedRow, offline: false })}
                    />,
                  ]}
                </Group>
              )}
            </div>
          </UsageTableContext.Provider>
        </div>
      )}

      {on && (computerRows.length > 0 || savedRow) && <ClaudeCodePlanOutChoice tokenRows={tokenRows} />}

      {on && (
        <ClaudeCodeHostedChoice
          tooSmall={setup?.too_small ?? []}
          onAddToken={() => setToken({ target: SAVED_TOKEN, action: 'add' })}
          onAsking={setAsking}
        />
      )}

      {tooSmall.length > 0 && (
        <div className={ROWS}>
          <Group subgrid title={t(S + '.tooSmallTitle')} empty="">
            {tooSmall.map((c) => <TooSmallRow key={c.computer_id} computerId={c.computer_id} name={c.name} />)}
          </Group>
        </div>
      )}

      {offerComputers && (
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
