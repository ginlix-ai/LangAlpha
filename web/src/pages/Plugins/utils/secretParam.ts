/**
 * The Secrets section's deep links. `?tab=secrets&secret=NAME` opens the add
 * form prefilled with NAME, for a key nothing has stored yet; `&edit=NAME`
 * opens the stored key's own edit form in place, for one that is there but
 * wrong, so "Replace key" lands on the row rather than at the top of a list. A URL param for the same reason as the Add
 * intent (`addParam.ts`): the Secrets tab strips it once acted on, so a remount
 * does not reopen the form, and it works from any page, including the
 * workspace MCP tab's "Set up NAME" button.
 */

export const SECRET_PARAM = 'secret';

export function secretSetupHref(name: string): string {
  const params = new URLSearchParams({ tab: 'secrets', [SECRET_PARAM]: name });
  return `/plugins?${params.toString()}`;
}

export const EDIT_SECRET_PARAM = 'edit';

export function secretEditHref(name: string): string {
  const params = new URLSearchParams({ tab: 'secrets', [EDIT_SECRET_PARAM]: name });
  return `/plugins?${params.toString()}`;
}
