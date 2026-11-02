import { useEffect, useEffectEvent, useRef, useState } from 'react';
import { useArtifactVersions, useOwnedArtifact } from '@/hooks/useArtifact';
import type { Artifact, ArtifactVersion } from '@/types/api';

/** How long the picker says an Update moved the view before it reads as the version alone. */
export const UPDATED_NOTICE_MS = 4000;

/** Which version a view shows, and the one an Update just moved it to. */
export interface VersionState {
  /** Null is the live file. */
  version: number | null;
  /** Set while the picker says "Updated to vN". */
  updatedTo: number | null;
}

export type VersionAction =
  /** The reader's own pick, which overtakes a notice. */
  | { type: 'pick'; version: number | null }
  /** The latest moved from `from` to `to`. */
  | { type: 'follow'; from: number; to: number }
  /** The notice for `seq` has been on screen for its while. */
  | { type: 'settle'; seq: number };

export const LIVE_VIEW: VersionState = { version: null, updatedTo: null };

export function versionReducer(state: VersionState, action: VersionAction): VersionState {
  switch (action.type) {
    case 'pick':
      return state.version === action.version && state.updatedTo === null
        ? state
        : { version: action.version, updatedTo: null };
    case 'follow':
      // A view on the latest moves to the new one and says so; a view on an
      // older version was opened there on purpose, and stays.
      return state.version === action.from ? { version: action.to, updatedTo: action.to } : state;
    case 'settle':
      return state.updatedTo === action.seq ? { ...state, updatedTo: null } : state;
    default:
      return action satisfies never;
  }
}

/** Where a view keeps its version: a panel tab's memory, or the page's address. */
export interface VersionStore {
  state: VersionState;
  dispatch: (action: VersionAction) => void;
}

/** One tab's file: a tab that moves on to another file opens that one live. */
export function versionKey(tabId: string, path: string): string {
  return `${tabId}\u0000${path}`;
}

/**
 * The panel's version views, one per tab and file (`versionKey`). Kept in
 * memory with the panel: a tab restored from storage opens on the live file,
 * and has nothing to tell.
 */
export function usePanelVersionStores(): (key: string) => VersionStore {
  const [states, setStates] = useState<Readonly<Record<string, VersionState>>>({});
  return (key) => ({
    state: states[key] ?? LIVE_VIEW,
    dispatch: (action) => setStates((prev) => {
      const was = prev[key] ?? LIVE_VIEW;
      const next = versionReducer(was, action);
      if (next === was) return prev;
      const rest = { ...prev };
      delete rest[key];
      return next.version === null && next.updatedTo === null ? rest : { ...rest, [key]: next };
    }),
  });
}

/** The owner's view of an HTML file's versions: which one is on screen, and the list to pick from. */
export interface ArtifactVersionView {
  /** Undefined until made or read. */
  record: Artifact | undefined;
  /** Newest first; undefined until read, and for a file never published. */
  versions: readonly ArtifactVersion[] | undefined;
  /** Null is the live file. */
  version: number | null;
  /** The published version on screen, once the list has it. */
  entry: ArtifactVersion | null;
  /** `version` names one the file does not have. */
  missing: boolean;
  /** The list could not be read. */
  failed: boolean;
  retry: () => void;
  /** The version an Update just moved the view to, while the picker says so. */
  updatedTo: number | null;
  pick: (version: number | null) => void;
}

/**
 * The version an owner's view of an HTML file shows, the file's record (made
 * if it has none) and its versions, over a store the host picks. An Update
 * moves a view on the latest version to the new one, which the picker
 * announces for `UPDATED_NOTICE_MS` from when the view has it, counted while
 * this view is up; a pick overtakes the notice.
 */
export function useArtifactVersionView(
  workspaceId: string | null,
  path: string | null,
  store: VersionStore,
  { root }: { root?: string | null } = {},
): ArtifactVersionView {
  const { record } = useOwnedArtifact(workspaceId, path, { root });
  const artifactId = record?.artifact_id ?? null;
  const latest = record?.latest_seq ?? 0;
  const versionsQuery = useArtifactVersions(artifactId, { enabled: latest > 0 });
  const { version, updatedTo } = store.state;
  const dispatch = useEffectEvent((action: VersionAction) => store.dispatch(action));

  const lastLatest = useRef<{ id: string; seq: number } | null>(null);
  useEffect(() => {
    if (!artifactId || latest < 1) return;
    const before = lastLatest.current;
    lastLatest.current = { id: artifactId, seq: latest };
    if (before?.id === artifactId && latest > before.seq) dispatch({ type: 'follow', from: before.seq, to: latest });
  }, [artifactId, latest]);

  // The address can move the version under the notice (Back on the page), and
  // the notice is only ever about the version on screen.
  const announced = updatedTo !== null && updatedTo === version ? updatedTo : null;
  useEffect(() => {
    if (announced === null) return;
    const timer = window.setTimeout(() => dispatch({ type: 'settle', seq: announced }), UPDATED_NOTICE_MS);
    return () => window.clearTimeout(timer);
  }, [announced]);

  const versions = versionsQuery.data;
  const entry = version === null ? null : (versions?.find((v) => v.seq === version) ?? null);
  // A list being read again may be about to gain the version an Update just
  // moved to, so only a settled list says it is not there.
  const settled = versionsQuery.isSuccess && !versionsQuery.isFetching;
  const missing = version !== null && !!record && !entry && (latest < 1 || settled);

  return {
    record,
    versions,
    version,
    entry,
    missing,
    failed: versionsQuery.isError,
    retry: () => void versionsQuery.refetch(),
    updatedTo: announced,
    pick: (next) => store.dispatch({ type: 'pick', version: next }),
  };
}
