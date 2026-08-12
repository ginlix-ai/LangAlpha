// Wrapped in an IIFE so the captured host functions stay in closure scope: the
// runtime rewrites top-level `const` to `var`, which would republish them as
// global properties and undo the delete below.
(function () {
  const hostAgent = __host_agent;
  const hostPhase = __host_phase;
  const hostLog = __host_log;
  // The host bindings take raw arguments and skip the argument checks these
  // wrappers perform, so a script must not be able to reach them directly.
  delete globalThis.__host_agent;
  delete globalThis.__host_phase;
  delete globalThis.__host_log;

  // Captured before any script code runs, so reassigning globalThis.TypeError
  // cannot redirect the check below. Mutating the native constructor itself
  // still can — this guards accidental shadowing, not a hostile script.
  const NativeTypeError = TypeError;
  const NativeReferenceError = ReferenceError;

  globalThis.agent = async function agent(prompt, opts) {
    if (typeof prompt !== "string" || prompt.length === 0)
      throw new NativeTypeError("agent(prompt, opts): prompt must be a non-empty string");
    if (opts !== undefined && (typeof opts !== "object" || opts === null || Array.isArray(opts)))
      throw new NativeTypeError("agent(prompt, opts): opts must be a plain object");
    const response = await hostAgent(prompt, opts ?? {});
    if (!response.ok) {
      // A dispatch the run refused as invalid fails identically every retry —
      // that is the script's bug, thrown in the class the escalation reads.
      // Child and infrastructure failures keep the absorbable Error shape.
      if (response.usage) throw new NativeTypeError(response.error);
      throw new Error(response.error);
    }
    return response.value;
  };

  globalThis.phase = function phase(title) { hostPhase(String(title)); };
  globalThis.log = function log(message) { hostLog(String(message)); };

  // Deliberate throws — a child failure, a script's own Error — keep the
  // documented absorb-to-null contract. TypeError and ReferenceError are the
  // engine's spontaneous verdict on broken code: every retry hits them again,
  // and a null there is indistinguishable from "the child failed" (a real run
  // once reported 0/6 while all six children succeeded). Those rethrow — and
  // uncaught, end the run where the bug executed. SyntaxError and RangeError
  // stay absorbable: JSON.parse on child output throws on bad data, not a
  // broken script.
  const isScriptBug = (e) =>
    e instanceof NativeTypeError || e instanceof NativeReferenceError;

  // The reason line must never out-fail the failure it describes: a broken
  // toString() would otherwise escalate an absorbable error.
  const describe = (e) => {
    try { return String(e); }
    catch { try { return Object.prototype.toString.call(e); } catch { return "unprintable error"; } }
  };

  const absorb = (e, where) => {
    if (isScriptBug(e)) throw e;
    hostLog(`[runtime] ${where} → null: ${describe(e)}`);
    return null;
  };

  // A slot may be a thunk or an already-started promise: scripts write the
  // idiomatic `parallel([agent(...)])` as readily as the documented thunk form,
  // and by then the children are dispatched — invoking is impossible, and
  // treating the slot as an error would silently null a healthy child. Await
  // those instead. Anything else is the script's bug and says so: a slot of
  // plain values (`parallel(args.tickers)`) would otherwise resolve to the
  // tickers having dispatched nothing — an answer indistinguishable from work.
  const awaited = (value, where) => {
    const thenable =
      value !== null && typeof value === "object" && typeof value.then === "function";
    if (thenable) return value;
    throw new NativeTypeError(
      `${where}: expected a function or a promise, got ${value === null ? "null" : typeof value}`
    );
  };

  // Promise.all carries the escalation: a rethrown script bug rejects the whole
  // call on the spot, while its reactions on every element keep sibling
  // rejections handled.
  globalThis.parallel = async function parallel(thunks) {
    if (!Array.isArray(thunks)) throw new NativeTypeError("parallel(thunks): thunks must be an array");
    return Promise.all(thunks.map((t, i) =>
      Promise.resolve()
        .then(() => (typeof t === "function" ? t() : awaited(t, `parallel slot ${i}`)))
        .catch((e) => absorb(e, `parallel slot ${i}`))
    ));
  };

  globalThis.pipeline = async function pipeline(items, ...stages) {
    if (!Array.isArray(items)) throw new NativeTypeError("pipeline(items, ...stages): items must be an array");
    return Promise.all(items.map(async (item, index) => {
      let prev = item;
      for (let s = 0; s < stages.length; s++) {
        const stage = stages[s];
        try {
          prev = await (typeof stage === "function"
            ? stage(prev, item, index)
            : awaited(stage, `pipeline stage ${s}`));
        }
        catch (e) { return absorb(e, `pipeline item ${index} stage ${s}`); }
      }
      return prev;
    }));
  };

  for (const k of ["agent", "phase", "log", "parallel", "pipeline"]) {
    Object.defineProperty(globalThis, k, { writable: false, configurable: false });
  }
})();
