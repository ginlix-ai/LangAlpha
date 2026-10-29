import type { TFunction } from 'i18next';
import type { BulkAction } from '../components/BulkActionBar';
import {
  bulkSelectionKey,
  type BulkNoun,
  type BulkRunOptions,
  type BulkTarget,
} from '../components/useBulkSelection';
import type { PluginListSurface } from '../hooks/usePluginListSurface';

/**
 * Switch on, switch off and the destructive third action, over whatever rows
 * a tab has selected. Each tab names only what differs: its rows' state, what
 * holds a row down, the endpoints, and which rows the third action reaches.
 */
export interface BulkSpec<T, R = unknown> {
  noun: BulkNoun;
  key: (row: T) => string;
  name: (row: T) => string;
  /** The state the switches decide on: a row already there is in neither. */
  isOn: (row: T) => boolean;
  /** Held off by its plugin: switching it would not change what runs, so it
   *  is in neither switch and the note says why. */
  held?: (row: T) => boolean;
  /** Off in a way this surface cannot undo, which only the switch-on skips. */
  enableLocked?: (row: T) => boolean;
  setEnabled: (row: T, on: boolean) => Promise<unknown>;
  remove: {
    verb: 'remove' | 'uninstall';
    labelKey: string;
    eligible: (row: T) => boolean;
    run: (row: T) => Promise<R>;
    /** What the rows that went took with them, from what each run returned. */
    outcome?: (results: R[]) => string[];
  } & (
    | { confirmKey: string }
    /** A confirm that depends on what the rows carry. */
    | { confirm: (rows: readonly T[]) => string }
  );
  /** Why the rows the third action leaves out are left out. */
  keptKey: string;
  /** Starts the sign-in a switch-on was refused for. */
  signIn?: BulkRunOptions['signIn'];
}

export interface BulkActions {
  actions: BulkAction[];
  count: number;
  selectionKey: string;
  note: string | null;
}

export function buildBulkActions<T, R = unknown>(
  t: TFunction,
  surface: Pick<PluginListSurface, 'run'>,
  selected: readonly T[],
  spec: BulkSpec<T, R>,
): BulkActions {
  const target = <V>(row: T, run: () => Promise<V>): BulkTarget<V> => ({
    key: spec.key(row),
    name: spec.name(row),
    run,
  });
  const isHeld = (row: T) => spec.held?.(row) ?? false;
  const held = selected.filter(isHeld);
  const free = selected.filter((row) => !isHeld(row));
  const enableTargets = free
    .filter((row) => !spec.isOn(row) && !spec.enableLocked?.(row))
    .map((row) => target(row, () => spec.setEnabled(row, true)));
  const disableTargets = free
    .filter(spec.isOn)
    .map((row) => target(row, () => spec.setEnabled(row, false)));
  const removable = selected.filter(spec.remove.eligible);
  const { verb, outcome } = spec.remove;

  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableTargets.length }),
      disabled: enableTargets.length === 0,
      run: () => surface.run(enableTargets, { verb: 'enable', noun: spec.noun, signIn: spec.signIn }),
    },
    {
      id: 'disable',
      label: t('plugins.bulk.disable', { count: disableTargets.length }),
      disabled: disableTargets.length === 0,
      run: () => surface.run(disableTargets, { verb: 'disable', noun: spec.noun }),
    },
    {
      id: verb === 'uninstall' ? 'uninstall' : 'delete',
      label: t(spec.remove.labelKey, { count: removable.length }),
      destructive: true,
      disabled: removable.length === 0,
      confirmMessage:
        'confirm' in spec.remove
          ? spec.remove.confirm(removable)
          : t(spec.remove.confirmKey, { count: removable.length }),
      run: () =>
        surface.run(
          removable.map((row) => target(row, () => spec.remove.run(row))),
          { verb, noun: spec.noun, outcome },
        ),
    },
  ];

  const kept = selected.length - removable.length;
  const notes = [
    kept > 0 ? t(spec.keptKey, { count: kept }) : null,
    held.length > 0 ? t('plugins.bulk.heldByPlugin', { count: held.length }) : null,
  ].filter(Boolean);
  return {
    actions,
    count: selected.length,
    selectionKey: bulkSelectionKey(selected.map(spec.key)),
    note: notes.length > 0 ? notes.join(' ') : null,
  };
}
