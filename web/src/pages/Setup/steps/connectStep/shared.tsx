/* eslint-disable react-refresh/only-export-components -- mixed helper/component module shared by the ConnectStep branch components */
import { useCallback } from 'react';
import { useNavigate } from 'react-router';
import { CheckboxChoice } from '@/components/ui/choice';
import type { AccessType } from '@/components/model/types';
import { returnFromSetup } from '@/lib/setupReturn';
import { useModels } from '@/hooks/useModels';
import { runsNatively } from '@/lib/connectionModels';
import { isPlatformMode } from '@/config/hostMode';
import { SERVER_KEY, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { startingOption, type ProviderGroup } from '@/components/modelAccess/providerOptions';

/** The provider step's lists: the subscriptions, the API keys, the local servers. */
export type ProviderList = Exclude<AccessType, 'coding_plan'>;

/**
 * What the wizard's steps hand each other. A catalog provider is named by the
 * key it saves under and nothing else: its plan, region, endpoint and format
 * are looked up in the catalog where they are needed.
 */
export interface LocationState {
  /** Which connect form the connect step renders. */
  method?: AccessType;
  /** The provider step's list the user came from, which Back returns to: a
   *  coding plan is listed with the subscriptions, though its form is a key's. */
  list?: ProviderList;
  provider?: string;
  /** For the model step's heading. */
  displayName?: string;
  /** Whose manifest models the model step offers (a coding plan shares its brand's). */
  modelsFrom?: string;
  /** Defining a provider of the user's own. */
  isCustom?: boolean;
  /** Connected by this visit's connect step, so the model step starts on
   *  every model it offers rather than on what is saved. */
  justConnected?: boolean;
  /** Sent here by the answers' model step: each step passes it on, and a
   *  connection made goes back there. */
  answering?: boolean;
}

/**
 * Where a step goes once a provider is connected: back to the page that sent
 * the user into setup, naming the provider by the key it was saved under;
 * back to the answers' model step when this visit came from it, which makes
 * it the default; or on to the model step for it. A connection no
 * catalog model runs on as it is (a coding plan whose models are its brand's,
 * a local server, a provider of the user's own) goes to the model step while
 * from the answers too, since only picking there makes its models runnable. Called
 * after the save and the cache refresh, so the page returned to reads the new
 * connection.
 *
 * Either way the step is left (the model step unmounts it, the return leaves
 * the app), so a caller keeps its busy state on success and clears it only on
 * failure: the button can't send twice while the page goes.
 */
export function useProviderConnected(answering: boolean) {
  const navigate = useNavigate();
  const { models } = useModels();
  const manifest = models?.model_metadata;
  const keyCandidates = models?.key_candidates;
  return useCallback(
    (state: LocationState & { provider: string }) => {
      // The answers' setup finishes first; the return is honored after.
      if (!answering && returnFromSetup(state.provider)) return;
      const native = !!manifest && Object.keys(manifest).some((m) => runsNatively(m, state.provider, manifest, keyCandidates));
      if (answering && native) navigate('/setup/model', { state: { connected: state.provider } });
      else navigate('/setup/models', { state: answering ? { ...state, answering } : state });
    },
    [navigate, answering, manifest, keyCandidates],
  );
}

/**
 * The providers that already work, which a pick goes on from rather than
 * connecting again: not a subscription that has to sign in again, which runs
 * no turn, nor a key while BYOK is off where it can be, which its form turns
 * back on, as Settings does. From the answers, not a key the server holds
 * either, since their Continue counts only a connection of the user's own.
 */
export function connectedProviders(
  providers: readonly ConfiguredProvider[],
  { answering, byokEnabled }: { answering: boolean; byokEnabled: boolean },
): Set<string> {
  return new Set(
    providers.filter((p) => p.state !== 'needs_reauth'
      && !(answering && p.masked_key === SERVER_KEY)
      && (p.access_type === 'oauth' || !isPlatformMode || byokEnabled)).map((p) => p.provider),
  );
}

/**
 * Picking a provider from a list: one already connected goes where a new
 * connection would (`useProviderConnected`), any other on to its key or
 * sign-in, which reads the rest of what it needs from the catalog by the
 * provider's name. A key group opens on the region a key is saved for, and
 * its Back returns to the list it was picked from (`from`).
 */
export function useProviderPick({ configured, answering }: { configured: ReadonlySet<string>; answering: boolean }) {
  const navigate = useNavigate();
  const connected = useProviderConnected(answering);
  const goOn = useCallback((state: LocationState & { provider: string }) => {
    if (configured.has(state.provider)) connected(state);
    else navigate('/setup/connect', { state: { ...state, answering } });
  }, [configured, connected, navigate, answering]);
  const pickKeyGroup = useCallback((from: ProviderList) => (group: ProviderGroup) => {
    const option = startingOption(group, configured);
    goOn({ method: 'api_key', list: from, provider: option.provider, displayName: group.name, modelsFrom: option.models_from });
  }, [configured, goOn]);
  return { connected, goOn, pickKeyGroup };
}

/** A quiet way on under a list, such as a provider of the user's own. */
export const QUIET_LINK = 'text-xs text-(--color-text-secondary) underline underline-offset-2 hover:opacity-80';

/** A model to pick from a list, by its name or id: a checkbox row, tinted and
 *  outlined once picked. */
export function ModelCheckRow({
  checked,
  onToggle,
  label,
}: {
  checked: boolean;
  onToggle: () => void;
  label: string;
}) {
  return (
    <CheckboxChoice
      size="md"
      checked={checked}
      onCheckedChange={onToggle}
      className="items-center gap-3 rounded-md px-3 py-2 transition-colors"
      style={{
        background: checked ? 'var(--color-bg-tag)' : 'transparent',
        border: `1px solid ${checked ? 'var(--color-text-primary)' : 'var(--color-border-default)'}`,
      }}
    >
      <span className="text-sm font-mono" style={{ color: 'var(--color-text-primary)' }}>
        {label}
      </span>
    </CheckboxChoice>
  );
}
