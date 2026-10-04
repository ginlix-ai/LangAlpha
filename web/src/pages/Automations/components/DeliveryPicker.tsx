import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, Pin, X } from 'lucide-react';
import { Button as AriaButton, Menu, MenuItem, MenuTrigger } from 'react-aria-components';
import { Popover } from '@/components/ui/aria-popover';
import {
  Select,
  SelectHeader,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectSection,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/aria-select';
import { FAIL_FAST_OFFLINE } from '@/lib/network';
import { queryKeys } from '@/lib/queryKeys';
import { cn } from '@/lib/utils';
import { Favicon } from '@/pages/ChatAgent/components/Favicon';
import { messagingAppDomain, messagingAppName, platformOf } from '@/pages/ChatAgent/components/messaging/messageDelivery';
import type { DeliveryApp, DeliveryChat, DeliveryDefault, DeliveryOptions } from '@/types/automation';
import { setDeliveryDefault } from '../utils/api';
import {
  type DeliveryNames,
  type DeliveryProblem,
  deliveryDefaultError,
  deliveryEntryName,
  mergeDeliverySelection,
  namesChat,
} from '../utils/delivery';

/** The apps in the order the reader knows them; any other after, by name. */
const APP_ORDER = ['slack', 'discord', 'telegram', 'imessage'];

const VIA_KEY: Record<DeliveryDefault['via'], string> = {
  workspace: 'automation.deliveryViaWorkspace',
  preferred: 'automation.deliveryViaPreferred',
  dm: 'automation.deliveryViaDm',
};

const HEADING = 'pb-1 font-mono text-[11px] font-normal text-(--color-text-tertiary)';

const MENU_ITEM =
  'relative flex w-full cursor-default select-none items-center rounded-sm px-2 py-1.5 text-sm outline-hidden data-focused:bg-accent/15 data-hovered:bg-accent/15 data-disabled:opacity-50';

function orderedApps(apps: Record<string, DeliveryApp>): [string, DeliveryApp][] {
  const rank = (app: string) => {
    const i = APP_ORDER.indexOf(app);
    return i === -1 ? APP_ORDER.length : i;
  };
  return Object.entries(apps).sort(([a], [b]) => rank(a) - rank(b) || a.localeCompare(b));
}

/** The DM first, then the channels in the order the app lists them. */
function orderedChats(chats: DeliveryChat[] | undefined): DeliveryChat[] {
  return [...(chats ?? [])].sort((a, b) => Number(b.kind === 'dm') - Number(a.kind === 'dm'));
}

function withoutDefaults(options: DeliveryOptions): DeliveryOptions {
  return {
    ...options,
    apps: Object.fromEntries(Object.entries(options.apps ?? {}).map(([app, data]) => [app, { ...data, default: null }])),
  };
}

function AppMark({ app }: { app: string | null }) {
  const domain = messagingAppDomain(app);
  return domain ? (
    <span aria-hidden="true" className="inline-flex shrink-0">
      <Favicon domain={domain} size={14} />
    </span>
  ) : null;
}

/** Problems a save was refused over, one line per entry. */
export function DeliveryProblems({ problems, nameOf }: { problems: DeliveryProblem[]; nameOf: (entry: string) => string }) {
  const { t } = useTranslation();
  if (problems.length === 0) return null;
  return (
    <ul className="automation-delivery-problems" role="alert">
      {problems.map((p) => (
        <li key={`${p.entry}|${p.message}`}>
          {t('automation.deliveryNamedProblem', { name: nameOf(p.entry), message: p.message })}
        </li>
      ))}
    </ul>
  );
}

type ChipAction = 'use' | 'clear';

interface DeliveryPickerProps {
  methods: string[];
  onChange: (methods: string[]) => void;
  options: DeliveryOptions;
  /** The options are another workspace's, shown while this one's load: its
   *  defaults are not this workspace's, so none is shown or changed. */
  stale?: boolean;
  /** The workspace the runs deliver from. Without one there is no default to set. */
  workspaceId: string | null;
  names: DeliveryNames;
  /** What the last save was refused over, by entry. */
  problems: DeliveryProblem[];
  labelledBy: string;
}

/**
 * Where an automation's results go, picked from the chats each linked app
 * offers. An app's Default follows the workspace's default output, then the
 * app's preferred chat, then the DM. Entries the list can't show, a chat no
 * longer listed or an app not linked, stay as chips until removed.
 */
export default function DeliveryPicker({
  methods,
  onChange,
  options: loaded,
  stale = false,
  workspaceId,
  names,
  problems,
  labelledBy,
}: DeliveryPickerProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [defaultError, setDefaultError] = useState<string | null>(null);

  const options = useMemo(() => (stale ? withoutDefaults(loaded) : loaded), [loaded, stale]);
  const apps = useMemo(() => orderedApps(options.apps ?? {}), [options.apps]);
  const known = useMemo(() => {
    const keys = new Set<string>();
    for (const [app, data] of apps) {
      keys.add(app);
      for (const chat of data.chats ?? []) keys.add(chat.address);
    }
    return keys;
  }, [apps]);
  const selected = methods.filter((m) => known.has(m));
  const nameOf = (entry: string) => deliveryEntryName(entry, t, options, names);

  const setDefault = useMutation({
    ...FAIL_FAST_OFFLINE,
    mutationFn: (vars: { workspace_id: string; platform: string; address: string | null }) =>
      setDeliveryDefault(vars),
    onMutate: () => setDefaultError(null),
    // The workspace the change was made for, not the one the form is on now.
    onSuccess: (res, vars) => {
      const key = queryKeys.automationDelivery.options(vars.workspace_id);
      const saved = res.data;
      // Write the answer in, so a refetch that fails leaves the new default
      // and not the old one. A cleared default is unknown until the refetch.
      queryClient.setQueryData<DeliveryOptions>(key, (old) => {
        const app = old?.apps?.[vars.platform];
        if (!old || !app) return old;
        const address = saved?.address ?? vars.address;
        const chat = app.chats?.find((c) => c.address === address);
        const next: DeliveryDefault | null = address
          ? { address, name: saved?.name ?? chat?.name ?? address, via: 'workspace' }
          : null;
        return { ...old, apps: { ...old.apps, [vars.platform]: { ...app, default: next } } };
      });
      return queryClient.invalidateQueries({ queryKey: key });
    },
    onError: (err) => setDefaultError(deliveryDefaultError(err, t)),
  });

  const chipActions = (entry: string): ChipAction[] => {
    const app = platformOf(entry);
    const def = app ? options.apps?.[app]?.default : null;
    // Only a chat the app lists now can become the default.
    if (stale || !workspaceId || !app || !known.has(entry)) return [];
    const isWorkspaceDefault = def?.via === 'workspace';
    if (!namesChat(entry)) return isWorkspaceDefault ? ['clear'] : [];
    return isWorkspaceDefault && def?.address === entry ? ['clear'] : ['use'];
  };

  const runAction = (entry: string, action: ChipAction) => {
    const platform = platformOf(entry);
    if (!platform || !workspaceId) return;
    setDefault.mutate({ workspace_id: workspaceId, platform, address: action === 'use' ? entry : null });
  };

  const problemOf = new Map(problems.map((p) => [p.entry, p.message]));
  const appErrors = apps.filter(([, data]) => data.error);

  return (
    <div className="automation-delivery">
      {methods.length > 0 && (
        <ul className="automation-delivery-chips">
          {methods.map((entry) => {
            const app = platformOf(entry);
            const def = app ? options.apps?.[app]?.default : null;
            const label = nameOf(entry);
            const actions = chipActions(entry);
            const isDefault = namesChat(entry) && def?.via === 'workspace' && def.address === entry;
            return (
              <li
                key={entry}
                className="automation-delivery-chip"
                data-invalid={problemOf.has(entry) ? '' : undefined}
                title={messagingAppName(app) ?? undefined}
              >
                <AppMark app={app} />
                {actions.length > 0 ? (
                  <MenuTrigger>
                    <AriaButton className="automation-delivery-chip-label" isDisabled={setDefault.isPending}>
                      <span className="truncate">{label}</span>
                      {isDefault && (
                        <Pin className="size-3 shrink-0" aria-label={t('automation.deliveryViaWorkspace')} />
                      )}
                      <ChevronDown className="size-3 shrink-0 opacity-60" aria-hidden="true" />
                    </AriaButton>
                    <Popover placement="bottom start">
                      <Menu
                        aria-label={label}
                        className="min-w-48 p-1 outline-hidden"
                        onAction={(key) => runAction(entry, key as ChipAction)}
                      >
                        {actions.map((action) => (
                          <MenuItem key={action} id={action} className={MENU_ITEM}>
                            {t(action === 'use' ? 'automation.deliveryUseAsDefault' : 'automation.deliveryClearDefault')}
                          </MenuItem>
                        ))}
                      </Menu>
                    </Popover>
                  </MenuTrigger>
                ) : (
                  <span className="automation-delivery-chip-label">
                    <span className="truncate">{label}</span>
                  </span>
                )}
                <button
                  type="button"
                  className="automation-delivery-chip-remove"
                  aria-label={t('automation.deliveryRemove', { name: label })}
                  onClick={() => onChange(methods.filter((m) => m !== entry))}
                >
                  <X className="size-3" aria-hidden="true" />
                </button>
              </li>
            );
          })}
        </ul>
      )}

      {apps.length === 0 ? (
        <p className="automation-form-readout mt-0">{t('automation.deliveryNoApps')}</p>
      ) : (
        <Select
          selectionMode="multiple"
          aria-labelledby={labelledBy}
          value={selected}
          onChange={(keys) => onChange(mergeDeliverySelection(methods, known, keys.map(String)))}
          className="inline-flex"
        >
          <SelectTrigger className="h-8 w-auto gap-1.5 px-2.5 text-[0.8125rem]">
            {/* The trigger always reads Add: what is picked shows as the chips. */}
            <SelectValue>{() => t('automation.deliveryAdd')}</SelectValue>
          </SelectTrigger>
          <SelectPopover
            placement="bottom start"
            offset={6}
            className="w-[max(280px,var(--trigger-width,0px))] max-w-[calc(100vw-32px)]"
          >
            <SelectListBox className="max-h-80">
              {apps.map(([app, data], i) => {
                const def = data.default;
                const defaultLabel = def?.name
                  ? t('automation.deliveryDefaultOptionLands', { name: def.name })
                  : t('automation.deliveryDefaultOption');
                return (
                  <SelectSection
                    key={app}
                    id={`app:${app}`}
                    className={cn(i > 0 && 'mt-1 border-t border-(--color-border-muted) pt-1')}
                  >
                    <SelectHeader className={HEADING}>{messagingAppName(app)}</SelectHeader>
                    <SelectItem id={app} textValue={defaultLabel} className="gap-2 text-[0.8125rem]">
                      <span className="min-w-0 flex-1 truncate">{defaultLabel}</span>
                      {def && <span className="automation-delivery-tag">{t(VIA_KEY[def.via])}</span>}
                    </SelectItem>
                    {orderedChats(data.chats).map((chat) => (
                      <SelectItem
                        key={chat.address}
                        id={chat.address}
                        textValue={chat.name || chat.address}
                        className="text-[0.8125rem]"
                      >
                        <span className="min-w-0 flex-1 truncate">{chat.name || nameOf(chat.address)}</span>
                      </SelectItem>
                    ))}
                  </SelectSection>
                );
              })}
            </SelectListBox>
          </SelectPopover>
        </Select>
      )}

      {/* The service names what failed with a code, not words for the reader. */}
      {appErrors.map(([app]) => (
        <p key={app} className="automation-form-readout mt-0">
          {t('automation.deliveryNamedProblem', { name: messagingAppName(app), message: t('automation.deliveryOptionsUnavailable') })}
        </p>
      ))}
      {defaultError && (
        <p className="automation-delivery-error" role="alert">
          {defaultError}
        </p>
      )}
      <DeliveryProblems problems={problems} nameOf={nameOf} />
    </div>
  );
}
