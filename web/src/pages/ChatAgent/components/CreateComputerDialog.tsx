/**
 * Create a computer: a name, where it runs, and its spec, under a drawing of
 * the machine that follows the picks.
 *
 * Opened over the Computers dialog, which owns the request and brings the
 * new card to the middle of its rail once it lands. Mounted only while open, so every opening starts
 * from a blank form. The provider and the tier are locked by plan the same
 * way, and either lock brings up the same way out as the spec dialog. At the
 * plan's computer cap only a machine the user brings is left open, and the
 * Computers dialog opens this form only when one is. In the desktop app on a
 * Mac, this Mac itself leads the choices, added by remote control as an SSH
 * computer.
 */
import { useEffect, useId, useRef, useState, type FormEvent } from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ModalShell } from '@/components/ui/ModalShell';
import { isPlatformMode } from '@/config/hostMode';
import { clientOs } from '@/lib/clientOs';
import { isDesktopShell } from '@/lib/desktop';
import { DesktopDownloadLink } from '@/pages/Plugins/components/DesktopDownloadLink';
import type { ComputerCreate, ComputerProviderOption, ResourceTier, WorkspaceQuota } from '@/types/api';

import { useComputerProviders, useComputers } from '../hooks/useComputers';
import { serverTakesRemoteControl, useThisMacAvailability } from '../hooks/useRemoteControl';
import { useTierQuota } from '../hooks/useTierQuota';
import { COMPUTER_NAME_MAX, nextComputerName } from '../utils/computerName';
import { NameRefusal } from './computerParts';
import { SshSetup } from './sshSetup';
import { useSshSetup } from './sshSetupState';
import { RemoteControlActionButton, RemoteControlSetup } from './remoteControlSetup';
import { useRemoteControlSetup } from './remoteControlSetupState';
import { PlanUpgradeHint } from './planUpgradeHint';
import {
  computerCapReached,
  defaultProviderKind,
  isProviderLocked,
  providerCountsTowardCap,
  providerLabel,
  providerManagesSpec,
  providerOptions,
  THIS_MAC_OPTION,
  thisMacOption,
  type ThisMacCard,
} from './providerUi';
import { RadioCardGroup, type RadioCardOption } from './radioCardGroup';
import { MachinePreview } from './machineArt/MachinePreview';
import { hostedPreview, ownPreview } from './machineArt/machine';
import {
  TIER_ORDER,
  TierRadioGroup,
  isTierBlocked,
  needsBiggerPlan,
  offeredTiers,
  tierCapacity,
  tierLabel,
} from './tierUi';

/**
 * Where the new computer comes from: this Mac, the user's own machine over
 * SSH, or a provider that runs it, null while every one on offer is locked.
 */
type Source = 'thisMac' | 'ssh' | { hosted: string | null };

/**
 * The source from the user's pick. Derived rather than seeded: the lists and
 * the quota land after the form opens and can refetch while it is open, and a
 * pick they have since locked falls back to what is open instead of being sent
 * to earn a 403.
 */
function sourceOf(
  picked: string | null,
  providers: readonly ComputerProviderOption[],
  thisMac: ThisMacCard | null,
  atCap: boolean,
): Source {
  if (picked === THIS_MAC_OPTION && thisMac === 'offered') return 'thisMac';
  const option = providers.find((p) => p.kind === picked);
  const kind = option && !isProviderLocked(option, atCap) ? option.kind : defaultProviderKind(providers, atCap);
  return kind === 'ssh' ? 'ssh' : { hosted: kind };
}

/**
 * The panel's height while a pick can change its body: a hosted pick's form,
 * the spec list and the plan line under it, fits whole, and a longer body
 * (this Mac's disclosure, the SSH steps) scrolls.
 */
const FORM_HEIGHT = 800;

interface CreateComputerDialogProps {
  onClose: () => void;
  onSubmit: (body: ComputerCreate) => void;
  /** The request is in flight. */
  busy: boolean;
  /** The last refusal, already worded for the user. */
  error: string | null;
  /** The last refusal of the name itself, said under the name field. */
  nameError?: string | null;
  /** The name was edited, which answers `nameError`. */
  onNameEdit?: () => void;
  /** The computer whose mark refused the folder, one this account does not
   *  have: the user may take the folder from it. */
  takeOver?: string | null;
  /** The source picked as it opens; one not on offer falls back as any pick does. */
  initialPick?: string | null;
}

function CreateComputerDialog({
  onClose,
  onSubmit,
  busy,
  error,
  nameError = null,
  onNameEdit,
  takeOver = null,
  initialPick = null,
}: CreateComputerDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const formId = useId();
  const nameId = useId();
  // What the user typed, null until they do: the field shows the name the
  // computer would get until then.
  const [typed, setTyped] = useState<string | null>(null);
  const nameRef = useRef<HTMLInputElement>(null);
  const [picked, setPicked] = useState<string | null>(initialPick);
  const [pickedTier, setPickedTier] = useState<ResourceTier>('standard');
  const errorRef = useRef<HTMLDivElement>(null);

  // The SSH summary makes the form taller than the dialog, so a refusal would
  // land below the fold with nothing visible changing.
  useEffect(() => {
    if (error && !busy) errorRef.current?.scrollIntoView?.({ block: 'nearest' });
  }, [error, busy]);
  // The field sits above a body that may have scrolled it away.
  useEffect(() => {
    if (nameError) nameRef.current?.focus();
  }, [nameError]);

  const { data: providerData } = useComputerProviders({ enabled: true });
  const { data: quota } = useTierQuota({ enabled: true });
  const { data: computerList } = useComputers({ enabled: true });
  const providers = providerData?.providers ?? [];
  // One provider is no choice: the request then names none and the server
  // uses its default, as it does while the list is still loading.
  const choosesProvider = providers.length > 1;

  const atCap = computerCapReached(quota);

  // This Mac, where the desktop app can pair it and this server can reach it.
  // Once a computer is this Mac the card says so instead of pairing it again,
  // so the card waits for the list to answer that. Where this server could
  // take it but this app cannot pair it, the card still shows, held, saying
  // what it would take: the desktop app, on a Mac in a browser; on Windows,
  // support that is still to come. The panel opens this form once it has read
  // whether this Mac is one, so the form lays out on that read, not a new one
  // that could land mid-entrance.
  const thisMac = useThisMacAvailability({ enabled: true, reread: false });
  let thisMacCard: ThisMacCard | null = null;
  if (thisMac.offered) {
    if (thisMac.added !== undefined) thisMacCard = thisMac.added ? 'added' : 'offered';
  } else if (serverTakesRemoteControl(providerData)) {
    const os = clientOs();
    if (os === 'windows') thisMacCard = 'windows';
    else if (os === 'mac' && !isDesktopShell()) thisMacCard = 'needsApp';
  }

  const source = sourceOf(picked, providers, thisMacCard, atCap);
  const hosted = typeof source === 'object' ? source.hosted : null;
  // A size the picked provider does not run falls back the same way.
  const offer = offeredTiers(providers.find((p) => p.kind === hosted));
  const tier: ResourceTier =
    offer.tiers.includes(pickedTier) && !isTierBlocked(tierCapacity(quota, pickedTier)) ? pickedTier : 'standard';
  const ssh = useSshSetup(source === 'ssh');
  const mac = useRemoteControlSetup(source === 'thisMac');
  // A machine the user brings is sized by them, so its tier is never a pick,
  // and left unnamed it goes by its hostname once it has answered.
  const own = source === 'thisMac' ? mac : source === 'ssh' ? ssh : null;
  // The name a computer left unnamed gets: the source's default, numbered past
  // the user's names as the server numbers it. The field holds it until the
  // user types, and follows the source until then.
  const base = own ? own.defaultName : providers.find((p) => p.kind === (hosted ?? providers[0]?.kind))?.default_name;
  const suggested = base && computerList ? nextComputerName(base, computerList.computers.map((c) => c.name)) : null;
  const name = typed ?? suggested ?? '';

  // A size the plan allows that this provider does not run but another open
  // one does (Extra Large, on E2B alone) is named with the way there, in
  // place of the upgrade line: a bigger plan is not what the user lacks.
  const elsewhere = hosted !== null ? tierElsewhere(providers, hosted, offer.tiers, quota, atCap) : null;
  const capLocked = atCap && providers.some((p) => providerCountsTowardCap(p.kind));
  const providerLocked = choosesProvider && providers.some((p) => isProviderLocked(p));
  const tierLocked = !own && !elsewhere && offer.tiers.some((id) => needsBiggerPlan(quota, id));
  // The cap is the lock in the way of the most, so it is the one named.
  const upgradeLead = capLocked
    ? t('computer.capPlanLimited', 'Need more computers than your plan includes?')
    : providerLocked && tierLocked
      ? t('computer.planLimitedBoth', "Need a provider or spec your plan doesn't include?")
      : providerLocked
        ? t('computer.planLimitedProvider', "Need a provider your plan doesn't include?")
        : tierLocked
          ? t('computer.spec.planLimited', 'Need a bigger spec than your plan includes?')
          : null;
  // The body the form would send, or null while it cannot send one. A machine
  // the user brings, this Mac included, sends its setup's part and always names
  // its kind, since the server takes a target only with it. Only a name the
  // user typed is sent: the one the field suggests the server gives anyway,
  // numbering past a computer that took it meanwhile, where a name sent is
  // refused when one of them has it.
  const trimmedName = (typed ?? '').trim();
  const fields: ComputerCreate | null = own
    ? own.create && { ...own.create, ...(trimmedName ? { name: trimmedName } : {}), resource_tier: 'standard', kind: 'ssh' }
    : {
      ...(trimmedName ? { name: trimmedName } : {}),
      resource_tier: tier,
      ...(choosesProvider && hosted ? { kind: hosted } : {}),
    };
  // Every provider on offer locked leaves nothing to create on. The cap locks
  // even a lone provider, which the form shows no choice for.
  const blocked = ((choosesProvider || atCap) && !own && hosted === null) || !fields;

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (busy || blocked) return;
    onSubmit(fields);
  };

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {t('common.cancel')}
      </Button>
      {/* Where this Mac's setup waits on the user (the disclosure, an approval,
          a retry), the button is its next step rather than Add computer. */}
      {source === 'thisMac' && mac.action ? (
        <RemoteControlActionButton state={mac} busy={busy} />
      ) : (
        <Button type="submit" form={formId} disabled={busy || blocked} aria-busy={busy}>
          {own
            ? (busy ? t('computer.adding', 'Adding...') : t('computer.add', 'Add computer'))
            : (busy ? t('computer.creating', 'Creating...') : t('computer.create', 'Create computer'))}
        </Button>
      )}
    </div>
  );

  // The hosted sandboxes come first, then the user's own machines: This Mac,
  // then SSH, both in preview.
  const providerCards = providerOptions(t, providers, atCap).map((o) =>
    // Beside the title, where a lock note would sit; a lock note wins.
    o.value === 'ssh' && !o.note ? { ...o, note: t('computer.ssh.previewLabel', 'Preview') } : o,
  );
  const options: RadioCardOption<string>[] = [
    ...providerCards.filter((o) => providerManagesSpec(o.value)),
    ...(thisMacCard ? [thisMacOption(t, thisMacCard)] : []),
    ...providerCards.filter((o) => !providerManagesSpec(o.value)),
  ];

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.newComputer', 'New computer')}
      subtitle={t(
        'computer.createDesc',
        'A separate machine with its own files. Workspaces you put on it start and stop with it.',
      )}
      onClose={onClose}
      closeDisabled={busy}
      // Wide enough for three providers side by side, each name on one line.
      width="wide"
      density="form"
      // Every provider shows the same name field and cards over a body of its
      // own, so a pick changes only that body: the panel holds its size and a
      // longer body (this Mac's disclosure) scrolls. Sized for the spec list.
      fixedHeight={options.length > 1 ? FORM_HEIGHT : undefined}
      stacked
      footer={footer}
    >
      <form id={formId} onSubmit={submit} className="flex flex-col gap-4">
        <MachinePreview
          machine={
            own
              ? ownPreview(t, own.ready, source === 'thisMac')
              : hostedPreview(t, hosted ?? providers[0]?.kind ?? null, offer, tier)
          }
        />
        <div className="flex flex-col gap-1.5">
          <label htmlFor={nameId} className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
            {t('computer.nameLabel', 'Name')}
            {/* A field holding a name is not one to leave blank. */}
            {suggested === null && (
              <>
                {' '}
                <span className="font-normal" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.optional')}</span>
              </>
            )}
          </label>
          <NameRefusal refusal={nameError} edge="var(--color-border-muted)">
            {(field, borderColor) => (
              <Input
                ref={nameRef}
                id={nameId}
                value={name}
                maxLength={COMPUTER_NAME_MAX}
                onChange={(e) => {
                  setTyped(e.target.value);
                  onNameEdit?.();
                }}
                // The suggested name is replaced by typing, as a rename field's is;
                // the click that focused it would otherwise drop a caret into it.
                onFocus={(e) => {
                  if (typed === null) e.currentTarget.select();
                }}
                onMouseUp={(e) => {
                  if (typed === null) e.preventDefault();
                }}
                disabled={busy}
                autoComplete="off"
                spellCheck={false}
                {...field}
                // Cleared, it still says what the computer will be called; a machine
                // the user brings says so before it has answered with its name.
                placeholder={suggested ?? (own ? t('computer.nameFromMachine', "Defaults to the machine's name") : undefined)}
                style={{ backgroundColor: 'var(--color-bg-input)', borderColor, color: 'var(--color-text-primary)' }}
              />
            )}
          </NameRefusal>
        </div>

        {options.length > 1 && (
          <RadioCardGroup
            options={options}
            value={source === 'thisMac' ? THIS_MAC_OPTION : source === 'ssh' ? 'ssh' : hosted}
            onChange={setPicked}
            label={t('computer.provider.label', 'Provider')}
            // Side by side keeps the form short; a third option joins the row,
            // and four, This Mac among them, go two by two rather than leave
            // one alone or squeeze each name onto two lines.
            columns={options.length === 3 ? 3 : 2}
          />
        )}

        {thisMacCard === 'needsApp' && (
          <p className="-mt-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }} data-testid="this-mac-needs-app">
            <Trans
              i18nKey="computer.remoteControl.needsAppHint"
              defaults="To add this Mac, <download>get the desktop app</download> and open this form there."
              components={{ download: <DesktopDownloadLink /> }}
            />
          </p>
        )}

        {/* Each Mac pairs itself, from the app running on it. */}
        {thisMacCard === 'added' && (
          <p className="-mt-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }} data-testid="this-mac-another">
            <Trans
              i18nKey="computer.remoteControl.anotherMacHint"
              defaults="To add another Mac, <download>get the desktop app</download> on it and open this form there."
              components={{ download: <DesktopDownloadLink /> }}
            />
          </p>
        )}

        {source === 'thisMac' ? (
          <RemoteControlSetup state={mac} account={thisMac.status?.account ?? ''} busy={busy} />
        ) : source === 'ssh' ? (
          <SshSetup state={ssh} busy={busy} />
        ) : (
          <TierRadioGroup
            offer={offer}
            value={tier}
            onChange={setPickedTier}
            label={t('computer.tierLabel', 'Computer spec')}
            quota={quota}
          />
        )}

        {elsewhere && (
          <p className="text-xs" data-testid="create-computer-tier-elsewhere" style={{ color: 'var(--color-text-secondary)' }}>
            {t('computer.tierElsewhere', '{{tier}} is available on {{provider}}.', {
              tier: tierLabel(t, elsewhere.tier),
              provider: providerLabel(elsewhere.kind, t),
            })}{' '}
            <button
              type="button"
              className="underline font-medium"
              style={{ color: 'var(--color-text-primary)' }}
              disabled={busy}
              onClick={() => {
                setPicked(elsewhere.kind);
                setPickedTier(elsewhere.tier);
              }}
            >
              {t('computer.switchProvider', 'Switch to {{provider}}', { provider: providerLabel(elsewhere.kind, t) })}
            </button>
          </p>
        )}

        {upgradeLead && <PlanUpgradeHint lead={upgradeLead} testId="create-computer-plan-cta" />}

        {error && !busy && (
          <div ref={errorRef} role="alert" className="flex flex-col gap-1.5">
            <p className="text-sm" style={{ color: 'var(--color-icon-danger)' }}>
              {error}
            </p>
            {takeOver && fields?.ssh && (
              <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t('computer.takeOverFolderNote', 'The other computer stops working in it. The files in it stay.')}{' '}
                <button
                  type="button"
                  className="underline font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                  onClick={() => onSubmit({ ...fields, ssh: { ...fields.ssh!, take_over: takeOver } })}
                >
                  {t('computer.takeOverFolder', 'Take over this folder')}
                </button>
              </p>
            )}
          </div>
        )}
      </form>
    </ModalShell>
  );
}

/**
 * The biggest size the plan has a slot of that `kind` does not run, and the
 * first other open provider that does. A size it lacks or has spent is no
 * reason to switch.
 */
function tierElsewhere(
  providers: readonly ComputerProviderOption[],
  kind: string,
  offered: readonly ResourceTier[],
  quota: WorkspaceQuota | null | undefined,
  atCap: boolean,
): { tier: ResourceTier; kind: string } | null {
  if (!isPlatformMode) return null;
  for (const tier of [...TIER_ORDER].reverse()) {
    const capacity = tierCapacity(quota, tier);
    if (!capacity || isTierBlocked(capacity) || offered.includes(tier)) continue;
    const other = providers.find(
      (p) =>
        p.kind !== kind &&
        providerManagesSpec(p.kind) &&
        !isProviderLocked(p, atCap) &&
        offeredTiers(p).tiers.includes(tier),
    );
    if (other) return { tier, kind: other.kind };
  }
  return null;
}

export default CreateComputerDialog;
