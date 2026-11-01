/** Exit codes are part of the contract: agents branch on them. */
export const Exit = {
  ok: 0,
  error: 1,
  usage: 2,
  auth: 3,
  notFound: 4,
  conflict: 5,
  paused: 6,
  failed: 7,
} as const;

export interface ErrorFacts {
  /** The HTTP status, when a server refused the call. */
  status?: number;
  detail?: unknown;
  /** A structured refusal's kind, for a caller that answers one. */
  reason?: string;
}

export class CliError extends Error {
  readonly status?: number;
  readonly detail?: unknown;
  readonly reason?: string;

  constructor(
    message: string,
    readonly code: number = Exit.error,
    facts: ErrorFacts = {},
  ) {
    super(message);
    this.status = facts.status;
    this.detail = facts.detail;
    this.reason = facts.reason;
  }
}

export function usage(message: string): CliError {
  return new CliError(message, Exit.usage);
}

export function unreachable(where: string, e: unknown): CliError {
  const cause = e instanceof Error && e.cause instanceof Error ? e.cause.message : String(e);
  return new CliError(`Cannot reach ${where} (${cause})`, Exit.error);
}

export function exitFor(status: number): number {
  if (status === 401 || status === 403) return Exit.auth;
  if (status === 404) return Exit.notFound;
  if (status === 409) return Exit.conflict;
  if (status === 400 || status === 422) return Exit.usage;
  return Exit.error;
}

/** The sentence a server error carries: FastAPI's ``detail`` as a string, or
 *  the ``message`` of a structured one, shown verbatim. */
export function detailMessage(body: unknown, fallback: string): string {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object" && "message" in detail) {
      const message = (detail as { message: unknown }).message;
      if (typeof message === "string") return message;
    }
    if (Array.isArray(detail)) {
      return detail
        .map((d) => (d && typeof d === "object" && "msg" in d ? String((d as { msg: unknown }).msg) : String(d)))
        .join("; ");
    }
  }
  return fallback;
}

/** The kind of a structured refusal: ``detail.type`` from the principal's
 *  refusals, ``detail.code`` from the routes that name theirs that way. */
export function detailReason(body: unknown): string | undefined {
  const detail = body && typeof body === "object" && "detail" in body ? (body as { detail: unknown }).detail : undefined;
  if (!detail || typeof detail !== "object") return undefined;
  const { type, code } = detail as { type?: unknown; code?: unknown };
  if (typeof type === "string") return type;
  return typeof code === "string" ? code : undefined;
}
