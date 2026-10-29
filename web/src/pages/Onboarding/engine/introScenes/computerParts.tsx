import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Cloud, KeyRound, Plus, Power, Server, Settings, Terminal, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { MachineArt } from '@/pages/ChatAgent/components/machineArt/MachineArt';
import { providerLabel } from '@/pages/ChatAgent/components/providerUi';
import { BadgePill, MACHINES, StatusGlyph, badgeWords } from './galleryParts';
import { MenuSurface, rv } from './kit';

/* Adding a computer from the gallery's toolbar: the menu the computer
   filter opens, and the New computer form its last row opens
   (`CreateComputerDialog`). Same tokens and labels as the app. */

/** The toolbar's computer menu, open (`GalleryToolbar`): All computers with
    the check at its end, each computer with its state in the menu's glyph
    column and its badge flush right, then Manage computers and New
    computer. `hover` is the row under the pointer. */
export function ComputerMenu({ hover }: { hover: boolean }) {
  const { t } = useTranslation();
  const row = 'flex items-center gap-1 rounded-[3px] px-[5px] py-[3.5px]';
  const glyph = 'grid w-[7px] shrink-0 place-items-center';
  const sep = <span className="-mx-[2px] my-[2px] h-px" style={{ background: 'var(--color-border-muted)' }} />;
  return (
    <MenuSurface
      className="flex w-[146px] flex-col rounded-[4px] p-[2px] text-[6.5px] leading-none"
      style={{ color: 'var(--color-text-primary)' }}
    >
      <span className={row}>
        <Server className="h-[7px] w-[7px] shrink-0" />
        <span className="min-w-0 flex-1">{t('workspace.allComputers')}</span>
        <Check className="h-[7px] w-[7px] shrink-0" />
      </span>
      {sep}
      {(['primary', 'second'] as const).map((id) => (
        <span key={id} className={row}>
          <span className={glyph}>
            <StatusGlyph state={MACHINES[id].state} />
          </span>
          <span className="min-w-0 flex-1 truncate">{MACHINES[id].name}</span>
          <BadgePill icon={Cloud} words={badgeWords(t, id)} />
        </span>
      ))}
      {sep}
      <span className={row}>
        <Settings className="h-[7px] w-[7px] shrink-0" />
        {t('workspace.manageComputers')}
      </span>
      <span
        className={row}
        style={
          hover
            ? { background: 'color-mix(in srgb, var(--color-accent-primary) 15%, transparent)' }
            : undefined
        }
      >
        <Plus className="h-[7px] w-[7px] shrink-0" />
        {t('workspace.newComputerMenu')}
      </span>
    </MenuSurface>
  );
}

/** A radio mark (`RadioChoice`): a ring, filled in the primary button's
    colour when picked. */
function RadioMark({ on }: { on: boolean }) {
  return (
    <span
      className="grid h-2 w-2 shrink-0 place-items-center rounded-full border"
      style={{ borderColor: on ? 'var(--color-btn-primary-bg)' : 'var(--color-text-quaternary)' }}
    >
      {on && <span className="h-1 w-1 rounded-full" style={{ background: 'var(--color-btn-primary-bg)' }} />}
    </span>
  );
}

/** A picked or unpicked option's box (`RadioCardGroup`): a stronger edge and
    a tint when picked, never the accent. */
function optionStyle(on: boolean): CSSProperties {
  return {
    borderColor: on ? 'var(--color-border-strong)' : 'var(--color-border-muted)',
    background: on ? 'var(--color-bg-hover)' : undefined,
  };
}

/** A field's label in the form, as `CreateComputerDialog` sets it. */
function FormLabel({ children }: { children: ReactNode }) {
  return (
    <span className="text-[6px] font-medium leading-none" style={{ color: 'var(--color-text-secondary)' }}>
      {children}
    </span>
  );
}

/** The sizes Daytona runs, as the server describes them. */
const SPECS = [
  { tier: 'standard', cpu: 1, memory: 2, disk: 5 },
  { tier: 'performance', cpu: 2, memory: 4, disk: 10 },
  { tier: 'max', cpu: 2, memory: 8, disk: 10 },
] as const;

/**
 * The New computer form (`CreateComputerDialog` in `ModalShell`): the title
 * and what a computer is; the drawing of the machine it will add
 * (`MachinePreview`, the app's own `MachineArt`); the name; the providers,
 * Daytona, This Mac and your own machine over SSH, three across; and under
 * them the picked provider's part, Daytona's spec or This Mac's remote
 * control setup (`RemoteControlSetup`), which holds its button until the
 * user has read what it means. The form keeps its height and its body
 * scrolls, so what does not fit is cut at the footer.
 */
export function NewComputerForm({ mac }: { mac: boolean }) {
  const { t } = useTranslation();
  const facts = (s: (typeof SPECS)[number]) =>
    [
      t('workspace.tierFact.cpu', { count: s.cpu }),
      t('workspace.tierFact.memory', { count: s.memory }),
      t('workspace.tierFact.disk', { count: s.disk }),
    ].join(' · ');
  const daytona = providerLabel('daytona', t);
  const corner = 'absolute top-[6px] font-mono text-[5px] font-medium uppercase leading-none tracking-[0.08em]';
  const providers = [
    { id: 'daytona', title: daytona, description: t('computer.provider.daytonaDesc'), on: !mac },
    {
      id: 'mac',
      title: t('computer.remoteControl.cardTitle'),
      description: t('computer.remoteControl.cardDesc'),
      note: t('computer.ssh.previewLabel'),
      on: mac,
    },
    {
      id: 'ssh',
      title: t('computer.provider.sshLabel'),
      description: t('computer.provider.sshDesc'),
      note: t('computer.ssh.previewLabel'),
      on: false,
    },
  ];
  const points = [
    { key: 'computer.remoteControl.meansAccess', icon: Terminal },
    { key: 'computer.remoteControl.meansBackground', icon: Power },
    { key: 'computer.remoteControl.meansCredentials', icon: KeyRound },
  ];
  return (
    <div
      className="flex h-[300px] flex-col overflow-hidden rounded-[8px] border"
      style={{ background: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
    >
      <div className="relative shrink-0 px-[11px] pb-[8px] pt-[11px]" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
        <span className="block text-[9.5px] font-semibold leading-none" style={{ color: 'var(--color-text-primary)' }}>
          {t('computer.newComputer')}
        </span>
        <span className="mt-[4px] block text-[5.5px] leading-none" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('computer.createDesc')}
        </span>
        <X className="absolute right-[10px] top-[10px] h-[7px] w-[7px]" style={{ color: 'var(--color-text-tertiary)' }} />
      </div>
      <div className="flex min-h-0 flex-1 flex-col gap-[8px] overflow-hidden px-[11px] pt-[9px]">
        {/* the machine it will add, standing on the frame's floor */}
        <div
          className="relative h-[62px] shrink-0 rounded-[4px] border"
          style={{ background: 'var(--color-art-ground)', borderColor: 'var(--color-border-muted)' }}
        >
          <span key={mac ? 'mac' : 'daytona'} className="intro-rv contents" style={rv(0)}>
            <span className={cn(corner, 'left-[7px]')} style={{ color: 'var(--color-text-secondary)' }}>
              {mac ? t('computer.remoteControl.thisMacBadge') : daytona}
            </span>
            {!mac && (
              <span className={cn(corner, 'right-[7px]')} style={{ color: 'var(--color-text-tertiary)' }}>
                {facts(SPECS[0])}
              </span>
            )}
            <span className="absolute inset-x-[12px] bottom-[6px] top-[14px] flex items-end justify-center">
              {mac ? (
                <MachineArt art="macbook" state="off" label={t('computer.remoteControl.thisMacBadge')} />
              ) : (
                <MachineArt art="cloud" state="off" brand={daytona} tier="standard" label={daytona} />
              )}
            </span>
          </span>
        </div>
        <div className="flex flex-col gap-[3px]">
          <FormLabel>
            {t('computer.nameLabel')}{' '}
            <span className="font-normal" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('common.optional')}
            </span>
          </FormLabel>
          <span
            className="h-[13px] rounded-[4px] border"
            style={{ background: 'var(--color-bg-input)', borderColor: 'var(--color-border-muted)' }}
          />
        </div>
        <div className="flex flex-col gap-[3px]">
          <FormLabel>{t('computer.provider.label')}</FormLabel>
          <div className="grid grid-cols-3 gap-[4px]">
            {providers.map((p) => (
              <span
                key={p.id}
                className="flex items-start gap-[4px] rounded-[4px] border p-[5px] transition-colors"
                style={optionStyle(p.on)}
              >
                <RadioMark on={p.on} />
                <span className="flex min-w-0 flex-col gap-[2px]">
                  <span className="text-[6.5px] font-medium leading-none" style={{ color: 'var(--color-text-primary)' }}>
                    {p.title}
                  </span>
                  <span className="text-[5px] leading-[1.3]" style={{ color: 'var(--color-text-tertiary)' }}>
                    {p.description}
                  </span>
                  {p.note && (
                    <span className="text-[5px] leading-none" style={{ color: 'var(--color-text-tertiary)' }}>
                      {p.note}
                    </span>
                  )}
                </span>
              </span>
            ))}
          </div>
        </div>
        {mac ? (
          <div key="mac" className="intro-rv flex flex-col gap-[5px] text-[5.5px] leading-[1.4]" style={rv(0)}>
            <span className="flex items-center gap-[4px]">
              <span className="text-[6.5px] font-medium leading-none" style={{ color: 'var(--color-text-primary)' }}>
                {t('computer.remoteControl.title')}
              </span>
              <span
                className="rounded-full px-[3px] py-[1.5px] text-[5px] font-medium leading-none"
                style={{ background: 'var(--color-warning-soft)', color: 'var(--color-warning)' }}
              >
                {t('computer.ssh.previewLabel')}
              </span>
            </span>
            <span style={{ color: 'var(--color-text-secondary)' }}>
              {t('computer.remoteControl.intro', { account: MAC_ACCOUNT })}
            </span>
            <span className="text-[6px] font-medium leading-none" style={{ color: 'var(--color-text-primary)' }}>
              {t('computer.remoteControl.meansLabel')}
            </span>
            {points.map(({ key, icon: Glyph }) => (
              <span key={key} className="flex items-start gap-[4px]" style={{ color: 'var(--color-text-secondary)' }}>
                <Glyph className="mt-[1px] h-[6px] w-[6px] shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                <span>{t(key, { account: MAC_ACCOUNT })}</span>
              </span>
            ))}
          </div>
        ) : (
          <div key="daytona" className="flex flex-col gap-[3px]">
            <FormLabel>{t('computer.tierLabel')}</FormLabel>
            {SPECS.map((s, i) => (
              <span
                key={s.tier}
                className="flex items-center gap-[5px] rounded-[4px] border px-[5px] py-[4px] leading-none"
                style={optionStyle(i === 0)}
              >
                <RadioMark on={i === 0} />
                <span className="w-[34px] shrink-0 text-[6.5px] font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t(`workspace.tier.${s.tier}`)}
                </span>
                <span className="text-[5.5px] tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
                  {facts(s)}
                </span>
              </span>
            ))}
          </div>
        )}
      </div>
      <div
        className="flex shrink-0 items-center justify-end gap-[5px] px-[11px] py-[7px]"
        style={{ borderTop: '1px solid var(--color-border-muted)' }}
      >
        <span className="px-[6px] py-[4px] text-[6.5px] font-medium leading-none" style={{ color: 'var(--color-text-primary)' }}>
          {t('common.cancel')}
        </span>
        {/* held until the user has read what remote control means */}
        <span style={{ opacity: mac ? 0.5 : 1 }}>
          <span
            key={mac ? 'mac' : 'daytona'}
            className="intro-rv block rounded-[4px] px-[7px] py-[4px] text-[6.5px] font-medium leading-none"
            style={{
              ...rv(0),
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            {mac ? t('computer.remoteControl.turnOn') : t('computer.create')}
          </span>
        </span>
      </div>
    </div>
  );
}

/** The macOS account the disclosure names. */
const MAC_ACCOUNT = 'alex';

/** The New computer form's width (px), centered on the page. */
export const FORM_W = 300;
