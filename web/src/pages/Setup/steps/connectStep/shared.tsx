/* eslint-disable react-refresh/only-export-components -- mixed helper/component module shared by the ConnectStep branch components */
import { useCallback } from 'react';
import { useNavigate } from 'react-router';
import { CheckboxChoice } from '@/components/ui/choice';
import type { AccessType } from '@/components/model/types';
import { returnFromSetup } from '@/lib/setupReturn';

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
}

/**
 * Where a step goes once a provider is connected: back to the page that sent
 * the user into setup, naming the provider by the key it was saved under, or
 * on to the model step for it. Called after the save and the cache refresh, so
 * the page returned to reads the new connection.
 *
 * Either way the step is left (the model step unmounts it, the return leaves
 * the app), so a caller keeps its busy state on success and clears it only on
 * failure: the button can't send twice while the page goes.
 */
export function useProviderConnected() {
  const navigate = useNavigate();
  return useCallback(
    (state: LocationState & { provider: string }) => {
      if (!returnFromSetup(state.provider)) navigate('/setup/models', { state });
    },
    [navigate],
  );
}

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
