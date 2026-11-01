---
title: Files and what persists
description: How LangAlpha backs up workspace files after every turn, what a stop, archive or rebuild keeps, and how uploads, downloads and server-held files work.
---

Your workspace files live on a [computer](/concepts/computers/), but LangAlpha also keeps a **backup copy** of them on the server. The backup is why files survive a computer being stopped, archived, resized or rebuilt, and why you can browse them while the computer is off. Installed packages and running programs live only on the computer and do not come back after a rebuild.

## How workspace files are backed up

LangAlpha backs up a workspace folder after every completed turn, again when a computer stops (on a best-effort basis), and before it deletes or resizes a computer. Only files that changed since the last backup are sent. A file written after a turn ends waits for the next completed turn. The file browser also has a backup button, and it reports how many files were saved and which were not, with the reason.

The backup leaves out things that can be recreated or that would be huge:

- **Dependency and cache folders**, at any depth: `node_modules`, `.venv`, `venv`, `vendor`, `.next`, `.nuxt`, `.cache`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.npm`, `.local`, `.config`, `.ipython` and `__pycache__`.
- **`.git`.** A cloned repository comes back as a plain working tree, without its history. Run `git clone` again if you need the history.
- **Compiled files**: `.pyc`, `.pyo`, `.so`, `.dylib` and `.o`, plus `.DS_Store` and `Thumbs.db`.
- **Agent working data** under `.agents/`: per-thread script copies and the generated tool wrappers, which are rebuilt on their own.
- **Files that are too large.** The limit depends on how the deployment stores files.

| How the deployment stores files | Per-file limit |
|---|---|
| No object storage configured (files are kept in the database) | 100 MB |
| Object storage, with file bytes passing through the server | 256 MB |
| Object storage, with computers uploading directly | No fixed limit beyond the disk |

A file that is not saved shows a reason in the file browser: too large, path too long, unreadable, or changed while saving. Move or split a large file, keep one copy of heavy raw data outside the workspace, and rely on a `requirements.txt` or a setup script instead of an environment folder.

:::note[Hosted and self-hosted]
**Hosted:** Files are backed up to the service's storage, and the per-file limit follows how the service stores files. The file browser tells you which files were not saved.

**Self-hosted:** Without `storage.provider` (or `STORAGE_PROVIDER`) set, file bytes stay in Postgres and the limit is 100 MB per file. With an S3-compatible store, `storage.blob_transfer` chooses direct or relayed transfer, which sets the limit above.
:::

Backup can only be turned off on a computer you brought over SSH. It is always on for cloud and Docker computers, because the server relies on it to rebuild them.

## What survives each event

| Event | Workspace files | Installed packages and `/tmp` | Running programs |
|---|---|---|---|
| Stop, then start | Kept | Kept | Ended |
| Archive, then start (Daytona) | Kept | Kept | Ended |
| Rebuild | Restored from the backup | Lost, so reinstall | Ended |

Memory, profile, automations, workflows, memos and transcripts are held by the server, so every row above keeps them (see below).

A **rebuild** happens when the sandbox is gone: a size change, a Daytona sandbox deleted after 90 dormant days, a Daytona host that stayed unavailable for an hour, or a lost container. After a rebuild the workspace you open is restored before your turn starts; large tool results and scratch notes follow during the turn. Another workspace is restored when it is opened. Until a restore finishes, the file browser says some files may be missing.

On E2B, a stop pauses the sandbox, so programs other than the agent's background commands can still be there after it resumes.

An [SSH computer](/concepts/sandboxes-and-ssh/) is never rebuilt. Your files stay on your machine, and if a workspace folder goes missing you are offered a restore from the backup.

## Files while a computer is stopped

A stopped computer still serves your files. The file browser and downloads read the backup copy, read-only. Writing a file, uploading and running code need the computer running, so start it first. If a file was created after the last backup, the browser reports that it was not found in the backup and suggests starting the computer.

## Files the server holds for you

Some of your data is not stored in any one computer. The server holds it and shows it on every computer as ordinary files through a file mount, so it carries across computers and rebuilds:

| What | Where the agent sees it | Who can change it |
|---|---|---|
| Memory | `.agents/memory/` (this workspace) and `.agents/user/memory/` (all workspaces) | The agent and you |
| Profile (portfolio, watchlist, preferences) | `.agents/user/profile/` | The agent and you; the server checks each save |
| Automations | `.agents/user/automations/`, one file per automation | The agent and you; the server checks each save |
| Workflows | `.agents/workflows/` | The agent |
| Memos | `.agents/user/memo/` | You upload them; the agent can only read |
| Transcripts of past threads | `.agents/transcripts/` | Read-only |

A save that the server refuses, such as invalid JSON, does not fail the command quietly: the tool result lists it as not saved. These files are never part of the workspace backup, since the server already has the only copy. See [Memory, memos and the scratchpad](/concepts/memory-memos-scratchpad/) for what each store is for.

On an SSH computer there is no file mount. The file tools still reach these files, but shell commands and code are refused when they name one.

## Uploads and downloads

- **Upload** from the file browser into a running workspace. A file must fit the backup limit above, and the server accepts at most 256 MB per upload unless the operator turns on direct browser uploads, which raise the limit to 2 GB. When you create a workspace, files you add go up right after it is ready, 50 MB each at most.
- **Download** any file from the file browser, including while the computer is stopped. Markdown and HTML files can also be saved as PDF, and `docx`, `pptx` and `parquet` files download without a preview.
- To let someone else see a file or thread, use a share link. See [Sharing](/concepts/sharing/).

## Related

- [Computers](/concepts/computers/)
- [Workspaces](/concepts/workspaces/)
- [Memory, memos and the scratchpad](/concepts/memory-memos-scratchpad/)
- [Sharing](/concepts/sharing/)
