import { z } from 'zod';

// Workspace/thread ids arrive from untrusted boundaries — URL params,
// location.state, sessionStorage — so validate with safeParse (never throws)
// per the boundary-validation convention.
const uuidSchema = z.string().uuid();

export function isValidUuid(value: unknown): value is string {
  return uuidSchema.safeParse(value).success;
}
