import * as z from 'zod/mini';

// Workspace/thread ids arrive from untrusted boundaries — URL params,
// location.state, sessionStorage — so validate with safeParse (never throws)
// per the boundary-validation convention. `zod/mini` keeps classic zod off the
// entry's critical path.
const uuidSchema = z.uuid();

export function isValidUuid(value: unknown): value is string {
  return uuidSchema.safeParse(value).success;
}
