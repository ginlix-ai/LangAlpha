import { useId, useState } from 'react';
import { Plus } from 'lucide-react';
import { useNavigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';
import { isPlatformMode } from '@/config/hostMode';
import { formatUnitList } from '@/lib/format';
import { useConfiguredProviders, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useClaudeCodeSetup, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { useClaudeSignInEnd } from '@/hooks/useClaudeSignInEnd';
import { useWorkspaceClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import {
  CLAUDE_CODE, CLAUDE_CODE_CHOICE, CLAUDE_CODE_CONNECTION, CLAUDE_CODE_MODE_LABELS, CLAUDE_CODE_SETUP_STATUS,
  CLAUDE_CODE_TOKEN_PARAM, CLAUDE_CODE_WORKSPACE_PARAM, computerTokenWorks,
} from '@/lib/claudeCode';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import type { ProviderCatalogEntry } from '@/components/model/types';
import { isSubscriptionProvider, subscriptionKey, type SubscriptionProvider } from '@/api/oauth';
import { RemoveAccessDialog } from '@/components/modelAccess/RemoveAccessDialog';
import { ClaudeCodeTokenSheet, ClaudeCodeTokenTitle, RemoveClaudeCodeTokenDialog } from '@/components/modelAccess/ClaudeCodeToken';
import { linkedSheetAction, useClaudeCodeTokenRows } from '@/components/modelAccess/tokenLine';
import type { SheetAction, TokenTarget } from '@/components/modelAccess/tokenTarget';
import { planLabel } from '@/components/modelAccess/planLabel';
import { regionTag } from '@/components/modelAccess/optionLabels';
import { StatusLabel, type AccessStatus } from '@/components/modelAccess/parts';
import { UsageHeaderRow } from '@/components/modelAccess/PlanUsage';
import { SUMMARY_WIDTHS, UsageTableContext, useUsageSteps } from '@/components/modelAccess/usageSteps';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import { fullerWindow, limitDetail, rowLimit } from '@/lib/claudeCodePlan';
import { cn } from '@/lib/utils';
import { ProviderChoices } from '@/components/modelAccess/ProviderChoices';
import { SubscriptionChoices } from '@/components/modelAccess/SubscriptionChoices';
import { KeyProviderList, KeyStepFrame, ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import { NO_FILTER, optionsOf, startingOption, type KeyFilter, type KeyOption, type ProviderGroup } from '@/components/modelAccess/providerOptions';
import { AccessRow, Group, KeyRow } from './modelAccess/AccessRows';
import { KeySheet } from './modelAccess/KeySheet';
import { ManageSheet, SubscriptionFlow } from './modelAccess/ManageSheet';
import { ClaudeCodeSheet } from './modelAccess/ClaudeCodeSheet';
import { claudeCodeHeadline } from './modelAccess/claudeCodeHeadline';

/** The Subscriptions card as a usage table, once Claude Code's row has a
 *  plan's use to show: its rows on one grid under a header row, stepping
 *  down as the card narrows (`SUMMARY_WIDTHS`). */
function SummaryTable({ on, children }: { on: boolean; children: React.ReactNode }) {
  const [table, setTable] = useState<HTMLDivElement | null>(null);
  const steps = useUsageSteps(table, on ? SUMMARY_WIDTHS : null);
  if (!on) return <>{children}</>;
  return (
    <div ref={setTable}>
      <div className={cn('grid gap-x-3', steps?.grid)}>
        <UsageTableContext.Provider value={steps}>{children}</UsageTableContext.Provider>
      </div>
    </div>
  );
}

const subscriptionName = (t: (k: string) => string, p: SubscriptionProvider) =>
  t(`modelAccess.${subscriptionKey(p)}.name`);

/** The list a key flow was opened from, which Back returns to: the API keys,
 *  or the subscriptions for a coding plan. */
type KeyOrigin = 'pick-key' | 'pick-subscription';

/** What the sheet is showing. */
type Sheet =
  | { kind: 'pick-subscription' }
  | { kind: 'connect-subscription'; provider: SubscriptionProvider }
  | { kind: 'pick-key' }
  | { kind: 'pick-local' }
  | { kind: 'key'; group: ProviderGroup; option: KeyOption; origin: KeyOrigin }
  | { kind: 'local-key'; entry: ProviderCatalogEntry }
  | { kind: 'claude-code-token'; target: TokenTarget; action: SheetAction }
  /** Claude Code itself: where it stands, what sets it apart, and each
   *  computer's token, the saved one, and the choice for cloud computers. */
  | { kind: 'claude-code' }
  /** `from` the add-key flow keeps that flow's width for the key it just saved.
   *  `justConnected`: the flow before it made the connection, so its models
   *  start picked. */
  | { kind: 'manage'; provider: string; from: 'list' | 'key-flow'; justConnected?: boolean };

/** The add-key flow is one size from the list to the saved key's models; see
 *  KeyStepFrame. Claude Code's view is a table of each token's plan use
 *  beside its status and button, so it is wider still. */
function sheetWidth(sheet: Sheet): 'standard' | 'wide' | 'table' {
  switch (sheet.kind) {
    case 'claude-code':
      return 'table';
    case 'pick-key':
    case 'key':
      return 'wide';
    case 'manage':
      return sheet.from === 'key-flow' ? 'wide' : 'standard';
    default:
      return 'standard';
  }
}

/** The step Back returns to, for the steps that have one. */
function backFrom(sheet: Sheet): Sheet | null {
  switch (sheet.kind) {
    case 'connect-subscription': return { kind: 'pick-subscription' };
    case 'key': return { kind: sheet.origin };
    case 'local-key': return { kind: 'pick-local' };
    default: return null;
  }
}

/**
 * Settings → Model → Model access: every way the account reaches a model
 * (subscriptions, API keys, local servers), each with its live status, and
 * one sheet where each is connected and managed. The connect flows are the
 * ones the setup wizard renders.
 */
export function ModelAccessSection() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const sheetTitleId = useId();
  const { keyGroups, planGroups, localEntries, placeOf, optionFor } = useProviderCatalog();
  const { providers, withdrawn, configuredSet, statusLoading, statusError } = useConfiguredProviders();
  const tokenRows = useClaudeCodeTokenRows();
  const { custody, isLoading: custodyLoading, unread: custodyUnread } = useClaudeCodeCustody();
  const { setup, unread: setupUnread } = useClaudeCodeSetup();
  const signInEnd = useClaudeSignInEnd();
  const locale = useLocale();
  const now = useNow();
  const [opened, setSheet] = useState<Sheet | null>(null);
  const [removing, setRemoving] = useState<ConfiguredProvider | null>(null);
  const [removingToken, setRemovingToken] = useState<TokenTarget | null>(null);
  const [choiceCovered, setChoiceCovered] = useState(false);
  // A link elsewhere (the model picker, a refused turn) opens the sheet its
  // row's first button would, once Claude Code is on, for the token the
  // workspace it names runs its turns on: its computer's, where that is one
  // of the user's own, else the saved one. Until the workspace and the
  // computers are read (Home's placeholder row is no read) it opens none,
  // so the saved token's never opens in its computer's place. A workspace gone or not the user's (403, 404) runs
  // on no computer of theirs; any other failed read leaves which computer
  // unknown, so it opens none and keeps the link for a reload. A row offering
  // only Remove opens none either. Closing it drops the link.
  const [searchParams, setSearchParams] = useSearchParams();
  const claudeCode = useClaudeCodeStatus();
  const linkedWorkspaceId = searchParams.get(CLAUDE_CODE_WORKSPACE_PARAM);
  const linked = useWorkspaceClaudeCode(linkedWorkspaceId);
  const linkResolved = !linkedWorkspaceId || linked.known;
  const linkedRow = searchParams.get(CLAUDE_CODE_TOKEN_PARAM) === CLAUDE_CODE && claudeCode.enabled && linkResolved
    ? tokenRows.find(({ target }) => (target.kind === 'computer' ? target.computerId === linked.computerId : !linked.computerId))
    : undefined;
  const linkedAction = linkedRow && linkedSheetAction(linkedRow.line);
  const linkedSheet: Sheet | null = linkedRow && linkedAction
    ? { kind: 'claude-code-token', target: linkedRow.target, action: linkedAction }
    : searchParams.get(CLAUDE_CODE_TOKEN_PARAM) === CLAUDE_CODE_CHOICE && (setup || setupUnread) ? { kind: 'claude-code' } : null;
  const sheet: Sheet | null = opened ?? linkedSheet;
  const dropLink = () => {
    if (searchParams.has(CLAUDE_CODE_TOKEN_PARAM)) setSearchParams((params) => {
      params.delete(CLAUDE_CODE_TOKEN_PARAM);
      params.delete(CLAUDE_CODE_WORKSPACE_PARAM);
      return params;
    }, { replace: true });
  };
  const closeSheet = () => {
    setSheet(null);
    dropLink();
  };
  // Kept here, not in the list, so Back from a provider returns to the same narrowed list.
  const [keyFilter, setKeyFilter] = useState<KeyFilter>(NO_FILTER);

  const subscriptionRows = providers.filter((p) => p.access_type === 'oauth' || p.access_type === 'coding_plan');
  const keyRows = providers.filter((p) => p.access_type === 'api_key');
  const localRows = providers.filter((p) => p.access_type === 'local');

  // The key form opens on the region a key is saved for, and offers the rest.
  const pickKeyGroup = (origin: KeyOrigin) => (group: ProviderGroup) =>
    setSheet({ kind: 'key', group, option: startingOption(group, configuredSet), origin });
  const manage = (provider: string) => setSheet({ kind: 'manage', provider, from: 'list' });
  const back = (from: Sheet) => () => setSheet(backFrom(from));

  const managed = sheet?.kind === 'manage' ? providers.find((p) => p.provider === sheet.provider) ?? null : null;
  /** How a row and its dialogs name a connection: a coding plan offered in
   *  several regions says which one, since each is its own connection. */
  const nameOf = (p: ConfiguredProvider) => {
    if (isSubscriptionProvider(p.provider)) return subscriptionName(t, p.provider);
    const region = p.access_type === 'coding_plan' ? regionTag(t, placeOf(p.provider)) : null;
    return region ? `${p.display_name} · ${region}` : p.display_name;
  };

  const sheetTitle = (() => {
    if (!sheet) return null;
    const providerTitle = (name: string, markProvider: string) => (
      <ProviderTitle name={name} markProvider={markProvider} title={t('setup.connectTitle', { provider: name })} />
    );
    switch (sheet.kind) {
      case 'pick-subscription': return t('modelAccess.connectSubscription');
      case 'connect-subscription': return t('setup.connectTitle', { provider: subscriptionName(t, sheet.provider) });
      case 'pick-key': return t('modelAccess.addKey');
      case 'pick-local': return t('modelAccess.addLocal');
      case 'key': return providerTitle(sheet.group.name, sheet.group.markProvider);
      case 'local-key': return providerTitle(sheet.entry.display_name, sheet.entry.provider);
      case 'manage': return managed ? nameOf(managed) : null;
      case 'claude-code-token': return <ClaudeCodeTokenTitle target={sheet.target} action={sheet.action} />;
      case 'claude-code': return <ProviderTitle name={CLAUDE_CODE_CONNECTION.display_name} markProvider={CLAUDE_CODE} title={CLAUDE_CODE_CONNECTION.display_name} />;
    }
  })();

  const subscriptionDetail = (p: ConfiguredProvider) =>
    [p.email || p.account_id, planLabel(p.provider, p.plan_type)].filter(Boolean).join(' · ') || null;

  // The token whose plan Claude Code's row shows: of those that work and
  // reported use, the one nearest a limit, a limit held first.
  const clock = usePlanClock();
  const summary = setup?.on && setup.state === 'connected' ? [
    ...setup.computers.filter((c) => computerTokenWorks(c, now)).map((c) => ({ name: c.name, email: c.email, plan: c.plan })),
    ...(claudeCode.runsHere && claudeCode.state !== 'rejected'
      ? [{ name: t('modelAccess.claudeCode.setup.savedName'), email: claudeCode.email, plan: claudeCode.plan }] : []),
  ].flatMap(({ plan, ...who }) => {
    const limit = rowLimit(plan, clock.now);
    const fuller = fullerWindow(plan, clock.now);
    return plan && (limit || fuller) ? [{ ...who, plan, limit, used: fuller?.used ?? 0 }] : [];
  }).sort((a, b) => Number(!!b.limit) - Number(!!a.limit) || b.used - a.used)[0] ?? null : null;

  /** What Claude Code's row says: the account, or the limit it is held at,
   *  of the token whose plan it shows; else the computers a token works on,
   *  or what it runs on there, else what the view leads with, or that it is
   *  off with a token left behind. */
  const claudeCodeDetail = () => {
    if (summary) return summary.limit ? limitDetail(t, summary.limit, clock) : summary.email ?? summary.name;
    if (!setup && setupUnread) return null;
    if (!setup?.on) return t(tokenRows.some(({ key }) => key === 'saved') ? 'modelAccess.claudeCode.optOut.saved' : 'modelAccess.claudeCode.optOut.computer');
    if (setup.state === 'checking') return null;
    if (setup.state !== 'connected') return claudeCodeHeadline(t, setup, now).title;
    const working = setup.computers.filter((c) => computerTokenWorks(c, now)).map((c) => c.name);
    if (working.length > 0) return t('modelAccess.claudeCode.setup.listSummary', { names: formatUnitList(working, locale) });
    if (custody?.runs_on) return t(CLAUDE_CODE_MODE_LABELS[custody.mode]);
    // Until the choice is read, it may be the stored token that connects it.
    if (custodyLoading || custodyUnread) return null;
    return t('modelAccess.claudeCode.detail');
  };

  /** The Claude sign-in's row says the day it stops working here, with a
   *  way to Claude Code's view where the server offers Claude Code. */
  const signInNotice = (p: ConfiguredProvider) => signInEnd && p.provider === 'claude-oauth' && (
    <>
      {t('modelAccess.claude.endsOn', { date: signInEnd.date })}
      {(setup || setupUnread) && (
        <>
          {' '}
          <button
            type="button"
            className="tap-target underline underline-offset-2 hover:opacity-80"
            style={{ color: 'var(--color-text-primary)' }}
            onClick={() => setSheet({ kind: 'claude-code' })}
          >
            {t('modelAccess.claude.switchToClaudeCode')}
          </button>
        </>
      )}
    </>
  );

  const rowStatus = (p: ConfiguredProvider): AccessStatus =>
    p.state === 'needs_reauth' ? 'needs_reauth' : 'connected';

  return (
    <div className="flex flex-col gap-4">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.description')}</p>

      <SummaryTable on={!!summary}>
      <Group subgrid={!!summary} title={t('modelAccess.subscriptions')} empty={statusLoading ? '' : statusError ? t('modelAccess.status.unknownLong') : t('modelAccess.noneSubscriptions')}>
        {[
          ...(summary ? [<UsageHeaderRow key="header" first={t('claudeCodePlan.table.account')} />] : []),
          ...subscriptionRows.map((p) => p.access_type === 'coding_plan' ? (
            <KeyRow key={p.provider} p={p} place={placeOf(p.provider)} onManage={() => manage(p.provider)} />
          ) : (
            <AccessRow
              key={p.provider}
              provider={p.provider}
              name={nameOf(p)}
              detail={subscriptionDetail(p)}
              notice={signInNotice(p)}
              status={rowStatus(p)}
              action={
                <Button type="button" variant={p.state === 'needs_reauth' ? 'outline' : 'ghost'} size="sm" onClick={() => manage(p.provider)}>
                  {p.state === 'needs_reauth' ? t('modelAccess.reconnect') : t('modelAccess.manage')}
                </Button>
              }
            />
          )),
          // Claude Code is one row once it is on, or while a token is left
          // behind after it was turned off (or the server stopped offering
          // it), which its view offers to remove.
          ...(setup?.on || tokenRows.length > 0 || (setupUnread && claudeCode.enabled) ? [(
            <AccessRow
              key={CLAUDE_CODE}
              provider={CLAUDE_CODE}
              name={CLAUDE_CODE_CONNECTION.display_name}
              detail={claudeCodeDetail()}
              wrapDetail={!summary}
              plan={summary?.plan}
              limited={!!summary?.limit}
              status={summary?.limit ? 'limit_reached' : setup?.on ? CLAUDE_CODE_SETUP_STATUS[setup.state] : setupUnread ? 'unknown' : 'off'}
              action={(
                <Button type="button" variant="ghost" size="sm" className="tap-target" onClick={() => setSheet({ kind: 'claude-code' })}>
                  {t('modelAccess.manage')}
                </Button>
              )}
            />
          )] : []),
        ]}
      </Group>
      </SummaryTable>

      <Group title={t('modelAccess.apiKeys')} empty={t('modelAccess.noneKeys')}>
        {[
          ...keyRows.map((p) => (
            <KeyRow key={p.provider} p={p} place={placeOf(p.provider)} onManage={() => manage(p.provider)} />
          )),
          // Nothing to manage on a key no provider takes any more: only removing it.
          ...withdrawn.map((p) => (
            <AccessRow
              key={p.provider}
              provider={p.provider}
              name={nameOf(p)}
              detail={p.masked_key}
              status="withdrawn"
              action={
                <Button type="button" variant="outline" size="sm" onClick={() => setRemoving(p)}>
                  {t('modelAccess.remove.remove')}
                </Button>
              }
            />
          )),
        ]}
      </Group>

      {!isPlatformMode && (
        <Group title={t('modelAccess.localServers')} empty={t('modelAccess.noneLocal')}>
          {localRows.map((p) => (
            <KeyRow key={p.provider} p={p} onManage={() => manage(p.provider)} />
          ))}
        </Group>
      )}

      <div className="flex flex-wrap gap-2">
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setSheet({ kind: 'pick-subscription' })}>
          <Plus aria-hidden="true" className="h-3.5 w-3.5" />
          {t('modelAccess.connectSubscription')}
        </Button>
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => {
          setKeyFilter(NO_FILTER);
          setSheet({ kind: 'pick-key' });
        }}>
          <Plus aria-hidden="true" className="h-3.5 w-3.5" />
          {t('modelAccess.addKey')}
        </Button>
        {!isPlatformMode && (
          <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setSheet({ kind: 'pick-local' })}>
            <Plus aria-hidden="true" className="h-3.5 w-3.5" />
            {t('modelAccess.addLocal')}
          </Button>
        )}
      </div>

      {sheet && (
        <ModalShell
          labelId={sheetTitleId}
          title={sheetTitle}
          onClose={closeSheet}
          width={sheetWidth(sheet)}
          covered={!!removing || (!!removingToken && sheet.kind === 'claude-code') || choiceCovered}
        >
          {sheet.kind === 'pick-subscription' && (
            <SubscriptionChoices
              planGroups={planGroups}
              configured={configuredSet}
              onPickSignIn={(p) => setSheet(configuredSet.has(p) ? { kind: 'manage', provider: p, from: 'list' } : { kind: 'connect-subscription', provider: p })}
              onPickPlan={pickKeyGroup('pick-subscription')}
              claudeCode={setup || setupUnread
                ? { status: setup ? CLAUDE_CODE_SETUP_STATUS[setup.state] : 'unknown', onPick: () => setSheet({ kind: 'claude-code' }) }
                : undefined}
            />
          )}
          {sheet.kind === 'connect-subscription' && (
            <SubscriptionFlow
              key={sheet.provider}
              provider={sheet.provider}
              onConnected={() => setSheet({ kind: 'manage', provider: sheet.provider, from: 'list', justConnected: true })}
              onCancel={back(sheet)}
            />
          )}
          {sheet.kind === 'pick-local' && (
            <ProviderChoices
              entries={localEntries.map((e) => ({ provider: e.provider, name: e.display_name }))}
              configured={configuredSet}
              onPick={(provider) => {
                const entry = localEntries.find((e) => e.provider === provider);
                if (entry) setSheet({ kind: 'local-key', entry });
              }}
            />
          )}
          {sheet.kind === 'pick-key' && (
            <KeyStepFrame
              footer={(
                <button
                  type="button"
                  onClick={() => navigate('/setup/connect', { state: { method: 'api_key', isCustom: true } })}
                  className="text-xs underline underline-offset-2 hover:opacity-80"
                  style={{ color: 'var(--color-text-secondary)' }}
                >
                  {t('modelAccess.customProvider')}
                </button>
              )}
            >
              <KeyProviderList
                groups={keyGroups}
                filter={keyFilter}
                onFilterChange={setKeyFilter}
                configured={configuredSet}
                onPick={pickKeyGroup('pick-key')}
              />
            </KeyStepFrame>
          )}
          {sheet.kind === 'key' && (
            <KeySheet
              key={sheet.group.group}
              option={sheet.option}
              group={sheet.group}
              configured={providers}
              onSaved={(provider, fresh) => {
                dropLink();
                setSheet({ kind: 'manage', provider, from: 'key-flow', justConnected: fresh });
              }}
              onBack={back(sheet)}
            />
          )}
          {sheet.kind === 'local-key' && (
            <KeySheet
              key={sheet.entry.provider}
              option={optionsOf(sheet.entry)[0]}
              configured={providers}
              onSaved={(provider, fresh) => setSheet({ kind: 'manage', provider, from: 'list', justConnected: fresh })}
              onBack={back(sheet)}
            />
          )}
          {sheet.kind === 'manage' && managed && (
            <ManageSheet
              key={managed.provider}
              p={managed}
              option={optionFor(managed.provider)}
              configured={providers}
              onClose={() => setSheet(null)}
              onRemove={() => setRemoving(managed)}
              justConnected={sheet.justConnected === true}
            />
          )}
          {sheet.kind === 'manage' && !managed && <StatusLabel status="working" />}
          {sheet.kind === 'claude-code' && (setup || setupUnread || tokenRows.length > 0 ? (
            <ClaudeCodeSheet
              setup={setup}
              tokenRows={tokenRows}
              onRemoveToken={setRemovingToken}
              onCover={setChoiceCovered}
            />
          ) : <StatusLabel status="working" />)}
          {sheet.kind === 'claude-code-token' && <ClaudeCodeTokenSheet target={sheet.target} onSaved={closeSheet} onCancel={closeSheet} />}
        </ModalShell>
      )}

      {removing && (
        <RemoveAccessDialog
          provider={removing}
          name={nameOf(removing)}
          stacked={!!sheet}
          onClose={() => setRemoving(null)}
          onRemoved={() => setSheet(null)}
        />
      )}

      {removingToken && <RemoveClaudeCodeTokenDialog target={removingToken} stacked={!!sheet} onClose={() => setRemovingToken(null)} />}
    </div>
  );
}
