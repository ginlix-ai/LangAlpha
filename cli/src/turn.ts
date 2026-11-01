import type { Ctx } from "./commands/command.js";
import { CliError, Exit, usage } from "./errors.js";
import { type Api, p, type RequestOptions } from "./http.js";
import { print, printable } from "./output.js";
import { frames, type SseFrame } from "./sse.js";
import { CLI_NAME } from "./name.js";

/** What an interrupt asks about, as the server's HITL middleware puts it. */
export interface ActionRequest {
  type?: string;
  name?: string;
  args?: unknown;
  description?: string;
  question?: string;
  options?: unknown;
}

export interface Interrupt {
  interrupt_id: string;
  /** ``order_approval`` for an order awaiting the user's verdict. */
  kind?: string;
  /** An answer carries one decision per request. */
  action_requests: ActionRequest[];
}

export interface TurnResult {
  thread_id?: string;
  run_id?: string;
  /** ``disconnected``: the stream closed before the turn said how it ended,
   *  which it does on every end it reaches, so the run may still be going. */
  status: "completed" | "interrupted" | "failed" | "cancelled" | "disconnected";
  text: string;
  interrupt?: Interrupt;
  error?: string;
  /** How a failed turn is tried again: ``retry`` when the server kept the
   *  failed run to retry, ``resend`` when nothing ran and the message must be
   *  sent again. */
  recovery?: string;
}

export interface TurnRequest {
  method: "GET" | "POST";
  path: string;
  body?: unknown;
  /** The thread, when the command names one: an answer or a failure the
   *  stream ends on before naming it still says which thread it was. */
  thread?: string;
}

/** Every command that runs or watches a turn comes through here. ``stream``
 *  (the default) prints the agent's words as they come, or each frame as a
 *  JSON line with ``--json``; ``--wait`` prints only the result; ``--detach``
 *  returns once the server names the run. */
export async function turn(ctx: Ctx, req: TurnRequest): Promise<number> {
  const { args, json } = ctx;
  if (args.has("wait") && args.has("detach")) throw usage("Pass --wait or --detach, not both");
  const mode = args.has("detach") ? "detach" : args.has("wait") ? "wait" : "stream";
  const result: TurnResult = { thread_id: req.thread, status: "completed", text: "" };
  let printed = false;
  let ended = false;
  for await (const frame of events(ctx.api, req.method, req.path, { body: req.body })) {
    if (mode === "stream" && json) process.stdout.write(`${JSON.stringify({ event: frame.event, ...frame.data })}\n`);
    apply(result, frame);
    ended ||= TERMINAL.has(frame.event);
    if (mode === "stream" && !json) {
      const words = printable(answerText(frame));
      if (words) {
        process.stdout.write(words);
        printed = true;
      }
    }
    if (result.status !== "completed" || (mode === "detach" && result.thread_id && result.run_id)) break;
  }
  if (printed && !result.text.endsWith("\n")) process.stdout.write("\n");
  const named = mode === "detach" && result.thread_id && result.run_id;
  if (!ended && !named) result.status = "disconnected";

  if (mode === "detach" && result.status === "completed") {
    const { thread_id, run_id } = result;
    print(json, { thread_id, run_id, status: "detached" }, () => `Started turn ${run_id} on thread ${thread_id}.`);
    return Exit.ok;
  }
  if (mode !== "stream") print(json, result, () => result.text.trim());
  const thread = result.thread_id ?? "<thread>";
  if (result.interrupt) process.stderr.write(printable(describeInterrupt(thread, result.interrupt)));
  if (result.status === "failed") process.stderr.write(printable(`Turn failed: ${result.error}\n${recoveryHint(thread, result.recovery)}`));
  if (result.status === "cancelled") process.stderr.write("Turn cancelled.\n");
  if (result.status === "disconnected") {
    const follow = result.thread_id ? ` Follow it with: ${CLI_NAME} threads follow ${result.thread_id}` : "";
    process.stderr.write(printable(`The stream closed before the turn ended.${follow}\n`));
  }
  if (mode !== "detach" && result.thread_id && !json) process.stderr.write(printable(`thread ${result.thread_id}\n`));
  if (result.status === "interrupted") return Exit.paused;
  if (result.status === "disconnected") return Exit.error;
  return result.status === "failed" || result.status === "cancelled" ? Exit.failed : Exit.ok;
}

async function* events(api: Api, method: string, path: string, opts: RequestOptions): AsyncGenerator<SseFrame> {
  const res = await api.send(method, path, { ...opts, accept: "text/event-stream" });
  if (!res.body) throw new CliError("The server sent no stream", Exit.error);
  try {
    yield* frames(res.body);
  } finally {
    await res.body.cancel().catch(() => {});
  }
}

/** The words of the agent's reply a frame carries: its text, not its
 *  reasoning or the signals around it, which share the event name. */
export function answerText(frame: SseFrame): string {
  if (frame.event !== "message_chunk") return "";
  const { content, content_type: kind } = frame.data;
  if (typeof content !== "string") return "";
  return kind === undefined || kind === "text" ? content : "";
}

/** The frames a turn's stream ends on, one of which it always sends. */
const TERMINAL = new Set(["interrupt", "error", "retry", "run_end"]);

function apply(result: TurnResult, frame: SseFrame): void {
  const d = frame.data;
  switch (frame.event) {
    case "metadata":
    case "thread_created":
      if (typeof d.thread_id === "string") result.thread_id = d.thread_id;
      if (typeof d.run_id === "string") result.run_id = d.run_id;
      break;
    case "message_chunk":
      result.text += answerText(frame);
      break;
    case "interrupt":
      result.status = "interrupted";
      if (typeof d.thread_id === "string") result.thread_id ??= d.thread_id;
      result.interrupt = interruptOf(d);
      break;
    // A retry frame is the server giving up on this run too: it has ended
    // the run as failed, and nothing more comes on this stream.
    case "retry":
    case "error":
      result.status = "failed";
      if (typeof d.thread_id === "string") result.thread_id ??= d.thread_id;
      if (typeof d.run_id === "string") result.run_id ??= d.run_id;
      result.error = String(d.message ?? d.error ?? d.content ?? "The turn failed");
      if (typeof d.recovery === "string") result.recovery = d.recovery;
      break;
    // The run's own verdict, last on the stream: a run can end failed or
    // cancelled with no error frame before it.
    case "run_end":
      if (result.status !== "completed") break;
      if (d.outcome === "cancelled") result.status = "cancelled";
      else if (d.outcome === "error") {
        result.status = "failed";
        result.error = "The turn failed";
      }
      break;
  }
}

function recoveryHint(thread: string, recovery: string | undefined): string {
  if (recovery === "retry") return `Retry it with: ${CLI_NAME} threads retry ${thread}\n`;
  if (recovery === "resend") return "It never started; send the message again.\n";
  return "";
}

function interruptOf(d: Record<string, unknown>): Interrupt {
  return {
    interrupt_id: String(d.interrupt_id ?? ""),
    kind: typeof d.kind === "string" ? d.kind : undefined,
    action_requests: Array.isArray(d.action_requests) ? (d.action_requests as ActionRequest[]) : [],
  };
}

/** What the thread is waiting on now, read from the server: the newest page
 *  of its replay ends with the interrupts pending at its tip, and only those,
 *  since the newest turn carries no answered ones. ``wanted`` picks one of
 *  several by id; otherwise the last, as the live stream would have shown. */
export async function pendingInterrupt(api: Api, thread: string, wanted?: string): Promise<Interrupt> {
  let pending: Interrupt | undefined;
  for await (const frame of events(api, "GET", p`/api/v1/threads/${thread}/messages/replay`, { query: { limit: 1 } })) {
    if (frame.event !== "interrupt") continue;
    const interrupt = interruptOf(frame.data);
    if (!wanted || interrupt.interrupt_id === wanted) pending = interrupt;
  }
  if (!pending) {
    const what = wanted ? `an answer to ${wanted}` : "an answer";
    throw new CliError(`Thread ${thread} is not waiting for ${what}`, Exit.conflict);
  }
  return pending;
}

/** An order waits for a verdict only the app may give, since the server
 *  refuses one from any token (``order_decision_forbidden``). An answer of
 *  the usual shape would reach the order's middleware as no verdict, which
 *  it takes as a refusal: an approval would turn the order down. Each of its
 *  requests carries the order's ledger attempt, in case the kind is lost. */
export function isOrder(interrupt: Interrupt): boolean {
  return interrupt.kind === "order_approval" || interrupt.action_requests.some((r) => "attempt_id" in r);
}

/** What the agent is waiting on, and the command that answers it. */
export function describeInterrupt(thread: string, interrupt: Interrupt): string {
  const requests = interrupt.action_requests;
  if (isOrder(interrupt)) {
    const orders = requests.map((r) => `  ${String(r.description ?? r.name ?? "an order")}\n`).join("");
    return `The agent is waiting for you to approve an order:\n${orders}Approve or reject it in the LangAlpha app.\n`;
  }
  const question = requests.find((r) => r.type === "ask_user_question");
  if (question) {
    const options = Array.isArray(question.options) ? question.options.map(String) : [];
    return (
      `The agent asks: ${String(question.question ?? "")}\n` +
      (options.length ? `  options: ${options.join(" | ")}\n` : "") +
      `Answer with: ${CLI_NAME} threads approve ${thread} --message "<answer>"  (${CLI_NAME} threads reject ${thread} skips it)\n`
    );
  }
  const actions = requests
    .map((r) => String(r.description ?? r.name ?? r.type ?? ""))
    .filter(Boolean)
    .map((line) => `  ${line}\n`)
    .join("");
  return (
    `The agent is waiting for approval:\n${actions}` +
    `Answer with: ${CLI_NAME} threads approve ${thread}  (${CLI_NAME} threads reject ${thread} --message "<why>" refuses it)\n`
  );
}
