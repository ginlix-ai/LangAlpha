/**
 * Deny-list arithmetic for the bulk scope actions. A user-tier row is active
 * in every workspace minus its `disabled_workspace_ids`; "only in X" and
 * "all workspaces" both reduce to flipping the per-workspace toggles whose
 * current state differs from the wanted one. Stale ids naming deleted
 * workspaces are ignored, matching the row ScopeControl's counting.
 */

export interface WorkspaceToggle {
  workspaceId: string;
  enabled: boolean;
}

/** The toggle calls that make a user-tier row active in exactly `chosen`
 * (within the live workspaces). Empty result = already in that state. */
export function onlyInPlan(
  disabledWorkspaceIds: readonly string[] | undefined,
  liveWorkspaceIds: readonly string[],
  chosen: ReadonlySet<string>,
): WorkspaceToggle[] {
  const disabled = new Set(disabledWorkspaceIds ?? []);
  const plan: WorkspaceToggle[] = [];
  for (const workspaceId of liveWorkspaceIds) {
    const wantEnabled = chosen.has(workspaceId);
    if (wantEnabled === !disabled.has(workspaceId)) continue;
    plan.push({ workspaceId, enabled: wantEnabled });
  }
  return plan;
}

/** The toggle calls that clear every live-workspace deny on a user-tier row. */
export function clearDenyPlan(
  disabledWorkspaceIds: readonly string[] | undefined,
  liveWorkspaceIds: readonly string[],
): WorkspaceToggle[] {
  return onlyInPlan(disabledWorkspaceIds, liveWorkspaceIds, new Set(liveWorkspaceIds));
}
