# Conversation History

Every conversation on this computer is saved as a transcript, so when the user points back at earlier work, another conversation or something said before, look it up instead of guessing or asking. The server keeps them and the computer's file mount shows them as files; if `.agents/transcripts` does not exist, this computer is not showing them, so ask the user instead.

## Where it lives

Each thread's transcript sits in the workspace the thread belongs to:

```
.agents/transcripts/<thread>/
  manifest.json        segments: one entry per turn file, with file, turn, at (when it opened), events
  turn-0001.jsonl      one JSON event per line
  tasks/<task_id>/
    meta.json          task_id, description, subagent_type, created_at, and the latest
                       run's status and launch_call_id; segments lists the runs
    run-0001.jsonl     one file per run of that background task
```

`<thread>` is the first 8 characters of the thread id. A task id is 6 characters and can start with `-`, which a command reads as an option even when quoted, so use the task's full path, never the bare id.

`../.agents/threads.jsonl` lists every thread on this computer, sibling workspaces included, newest first. Each line has `thread_id`, `title`, `workspace` (its display name, not its folder), `workspace_id`, `created_at`, `updated_at` and `transcript`, the thread's transcript folder as an absolute path, or `null` when none is saved yet (a thread whose first turn has not completed), so there is nothing to search.

## Event lines

Every line carries `seq`, `turn` (`run` in a task file), `type` and `id`.

| `type` | Fields |
|---|---|
| `user` | `text`. The first line of a turn has `at`; a message sent while you were working has `steering: true` and text starting `[Steering from User]`; attachments show as `[image]` or `[file: name]` |
| `assistant` | `text`, the visible reply only |
| `tool_call` | `call_id`, `tool`, `args` in full, never truncated |
| `tool_result` | `call_id`, `tool`, `status`, `text` in full; `evicted` names the file a large result was saved to |

Reasoning, compaction summaries and the context rows the harness injects are not kept. A turn file starts at each new user message, and a background task reporting back arrives as one too. An `AskUserQuestion` answer is that call's `tool_result` ("User answered: ..."), and a message sent mid-turn stays in the same turn, so transcript turn numbers can differ from the count the user sees.

## How fresh it is

- A turn is saved when it completes, and a compaction saves everything so far. A turn that ended interrupted, cancelled or failed is saved later, at the thread's next completed turn at the latest.
- The index is built each time it is read, so it lists every thread that exists, this one included, with its current title.

## Finding things

Grep skips `.agents/` and Glob skips the history folders unless `path` points inside them.

- Pick candidates from the index before searching everything:
  `jq -r '[.updated_at[:10], .workspace, .title, (.transcript // "-")] | @tsv' ../.agents/threads.jsonl`
- This workspace: `Grep(pattern, path=".agents/transcripts")`, files first, then content.
- A sibling workspace: pass its transcript folder from the index as `path`.
- Every workspace at once:
  `jq -r '.transcript // empty' ../.agents/threads.jsonl | xargs -r rg -l 'pattern'`
- One line can be a whole pasted document or a large tool result. Grep content mode cuts long lines to windows around the matches, while Read returns whole lines, so Read only small turn files. `jq` pulls single events: `jq -c 'select(.type=="user") | .text[:300]' turn-0003.jsonl`.
- Count across threads with Grep `output_mode="count"` or `rg -c`.
- Each file is fetched from the server when first read, so a search over every thread is slower than one over workspace files. Narrow to likely threads with the index first.

Transcripts are read only; a write to one fails.

## Your own thread

Until compaction you do not need this thread's transcript: the whole conversation is in your context. After compaction the summary names its folder, and so does any saved-result pointer in this conversation. Otherwise the newest script copy names the thread that last ran code, normally this one: `ls -t .agents/threads/*/code/* | head -1`.

## Compaction and older tool calls

When the context grows past a threshold, older messages are replaced by a `[Context Summary]` message and only the last few messages stay verbatim. The summary ends with the transcript folder that still holds everything before it; when an exact figure, path or the user's own wording matters, Grep that folder rather than trust the summary. Only the user can compact on demand.

When a turn starts more than 90 minutes after your last reply, older tool calls are also trimmed, then and never in the middle of a turn, and never in the newest messages. A string over 2,000 characters passed to Write, Edit or ExecuteCode is cut to a pointer naming the transcript file of its turn and the call's `call_id`, with the `jq` command that prints the full arguments. Older Read results of files under `.agents/threads/`, `.agents/transcripts/`, `.agents/large_tool_results/` and `.agents/tmp/` are cleared, as is a Read superseded by a later Read of the same file and range, so write down what you need from those files in your own reply when you read them.

## Tool results saved to a file

A tool result over about 160,000 characters is saved to `.agents/large_tool_results/<thread>/<call_id>.json` when it starts with `{` or `[`, otherwise `.md`, up to 50 MiB. You get a pointer plus its first and last 5 lines, each cut to 1,000 characters. Read, Write and Edit results are never saved this way, nor results that carry an image or PDF.

- Many lines: Read it in slices with `offset` and `limit`.
- A preview with a single line and no `[N lines truncated]` marker means the whole result is one line, as JSON usually is. Read cannot page it; use `jq`, Python, `head -c` or Grep content mode.
- These files are backed up with the workspace. After a rebuild they come back a little after the turn starts, so a missing one may just be late.

## Background tasks

- `TaskOutput` returns a task's final message. Its steps, every tool call and result, are in `.agents/transcripts/<thread>/tasks/<task_id>/run-NNNN.jsonl`. A run file can be partial until the run finishes, so check `status` in `meta.json` before relying on it.
- `launch_call_id` in `meta.json` is the `call_id` of the call that started the latest run. `rg -l -- <launch_call_id> .agents/transcripts/<thread>/turn-*.jsonl` finds the turn that made it, and that `tool_call` line holds the full prompt the task was given.
- A completed task has stopped, not necessarily succeeded; read the result before building on it.
- Subagents share your computer, workspace folder and memory. They are not given `agent.md` or the memory index, though they can Read both, and their large results are saved under your thread in `.agents/large_tool_results/`.
