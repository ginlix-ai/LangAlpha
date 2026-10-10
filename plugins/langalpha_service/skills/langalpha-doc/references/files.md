# Files and Paths

## Layout

The computer root, `..` from your workspace:

```
<workspace folder>/       one per workspace, named after it with any character a path
                          cannot hold replaced; a rename moves it between turns, never during one
.agents/threads.jsonl     every thread on this computer (see history.md)
.agents/skills/           skills shared across workspaces
.agents/tools/docs/       docs for every data server any workspace has enabled
.agents/user/             memory, memos, profile and automations shared by all workspaces
.agents/workflows/        saved workflows
```

The root also holds harness folders: `.system/`, `mcp_servers/`, and a reserved runtime folder the tools refuse to touch. Leave them alone. To find a sibling workspace, run `ls ..` in Bash or read the index for its exact folder rather than spelling it from the workspace's name.

Your workspace folder:

```
agent.md, data/, <task>/            yours
.agents/memory/                     this workspace's memory
.agents/skills/<name>               this workspace's skills, and links to shared ones
.agents/tools/                      wrappers and docs for the data servers enabled here;
                                    generated, never edit
.agents/threads/<thread>/           code/ (a copy of every ExecuteCode and Bash script)
.agents/transcripts/<thread>/       every conversation's transcript, read only
.agents/large_tool_results/<thread>/  tool results too large for your context
.agents/scratchpad/<thread>/        a thread's temporary files and checkpoint notes; deleted when the thread is archived
.file_sync_marker                   never delete: without it the workspace is restored again
```

The harness writes `.agents/threads/`, `.agents/transcripts/`, `.agents/large_tool_results/` and `.agents/tools/`. Read them; do not write them.

## How paths resolve

| Path | Read, Write, Edit, Glob, Grep | ExecuteCode, Bash |
|---|---|---|
| `data/x.csv` | your workspace | your workspace |
| `/data/x.csv` | your workspace | the real filesystem root, where it does not exist |
| `/etc/hosts` | `<workspace>/etc/hosts`, not found | the system file |
| `../<sibling>/x` | the sibling's file | the sibling's file |
| `/tmp/x` | `/tmp/x` | `/tmp/x` |

Use Bash for anything outside the workspaces and `/tmp`. Tool output prints your own files with a leading slash, a sibling's as an absolute path, and memory files as `/.agents/...`.

## Search

- **Glob**: a pattern with no `/` searches every depth, and results come newest first, at most 1000. Dependency and cache folders (`node_modules`, `.venv`, `venv`, `vendor`, `.git`, `.cache`, `__pycache__` and similar) are skipped at any depth, and so are the history folders under `.agents/` (`threads/`, `transcripts/`, `large_tool_results/`, `scratchpad/`) unless the pattern or `path` spells them out. Braces do not expand: `*.{csv,json}` matches nothing, so run one pattern per extension.
- **Grep**: skips hidden and git-ignored files and folders unless `path` points inside one. `.agents/memory/` is the exception and is searched by default. Content mode cuts a line over 500 characters to windows around its first matches.
- Data-server docs and shared skills are links, which Grep and a pattern starting `**/` do not follow. Spell the folder in the pattern, for example Glob `.agents/tools/docs/**/*.md`, or Read the file directly.

## Memory, memos, profile, workflows and automations live on the server

`.agents/memory/` (this workspace), `.agents/user/memory/` (every workspace), `.agents/user/memo/` (the user's uploaded documents, read only), `.agents/user/profile/` (`user.json`, `portfolio.json`, `watchlist.json`, `preference.json`, checked on write), `.agents/user/automations/` (one JSON file per automation, checked on write) and `.agents/workflows/` are held by the server, not on disk, so they survive every restart and rebuild. The file tools always reach them; Bash and code reach them through the computer's file mount.

- With the mount up they are ordinary files at the same paths. A save the server refuses (invalid JSON, a file changed since you read it) does not fail the command: the tool result lists it under NOT SAVED, so check there before relying on the write. In the profile and automations folders, save in place: `sed -i` and write-then-rename helpers write a temporary file first, which the profile folder refuses, as does the automations folder for a name without `.json`, like sed's. A temporary `.json` file there is saved as an automation of its own, and moving it onto the original is refused, so `rm` it.
- Without the mount, ExecuteCode and Bash refuse a call whose text names a memory, memo or automations path, even in a comment. Read the file first and put what you need into the code.
- `.agents/user/channels/` (the user's chat-app settings, present when a messaging service is connected) is reached only by the file tools: it is never on the mount, and ExecuteCode and Bash refuse a call whose text names it, mounted or not. Read its README before editing.
- From the workspace, Glob and Grep reach `.agents/memory/` by default; set `path` to `.agents/user` for the rest.
- A file holds at most 256 KB. Each part of a name uses only ASCII letters, digits and `- _ . @ + ~`, so no spaces; subfolders are fine.
- The file tools cannot delete a memory file; `rm` through the mount can. To retire one without it, remove its line from `memory.md` and overwrite the file with what is still true.
- Your context holds a copy of `memory.md` (its first 32,768 characters) taken when the thread starts. A change, by you or another thread, reaches it as a diff at the next turn. Read always returns the current file.
- With `path` set to one of these folders, `*` also matches `/`: Glob `*.md` finds every file, and `**/*.md` misses the top-level ones.
- Read a memo by its relative path, `.agents/user/memo/<file>`. Memo names are lowercase, and a PDF memo is stored as text with `--- Page N ---` markers, skipping pages that had no text.

## What the user sees

- The file panel shows your workspace folder only: not siblings, the computer root or `/tmp`. `.agents/` is behind a system-files toggle, and dependency and cache folders never appear.
- A new file shows up in the panel by the end of the turn at the latest.
- Link a file as `[name](task/file.md)` and an image as `![title](task/charts/x.png)`, relative to the workspace. Anchors work: `#L42`, `#L40-L55`, `#heading-slug`, `#page=3`. A path in backticks is not a link.
- Inside a markdown file, links resolve from the file's own folder first, but image paths always resolve from the workspace folder: in `task/report.md`, write `![](task/charts/x.png)`, not `![](charts/x.png)`.
- md, pdf, xlsx, csv, html, images and text preview in the panel; docx, pptx and parquet download only. Markdown and HTML files already offer save-as-PDF, so skip a PDF copy unless the user asks for one.
- An HTML file cannot fetch anything, not even a file beside it, so inline its data. Images, scripts and styles may be files beside it or `data:` URIs, and scripts may also load from cdnjs, jsdelivr, unpkg and esm.sh.
- Chart output from `plt.show()` is discarded. Save with `savefig` into `<task>/charts/` and link the file; give a changed chart a new filename, since images are cached.
