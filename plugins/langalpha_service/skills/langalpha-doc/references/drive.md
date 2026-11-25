# The Drive

The Drive feature is currently feature-gated. A user turns it on among the experiments in their settings.

It is on for you when your prompt has a `<drive_system>` section. Without one, or in a shared workspace, a call naming `.agents/drive` is refused as not available. Tell the user where to turn it on, and keep the file in the workspace until they do.

## What it is

`.agents/drive/personal/` holds the user's own files, kept by the server: the same tree from every workspace and every computer, through every restart and rebuild. A workspace's backup leaves it out, since the server already has it.

## Reaching it

| | Read, Write, Edit, Glob, Grep | Bash, ExecuteCode |
|---|---|---|
| A cloud computer | yes | yes, as ordinary files at the same path |
| A machine the user brought | yes | refused; use `la drive` |

## Putting a file in

- Text you are writing: Write to a path under `.agents/drive/personal/`. Folders above it are made for you.
- A file already in the workspace, of any type, where Bash reaches the Drive: `mkdir -p .agents/drive/personal/models && cp task/model.xlsx .agents/drive/personal/models/model.xlsx`; `mv` works the same way and removes the workspace copy.
- The same on any computer: `la drive put task/model.xlsx models/model.xlsx`, or a folder: `la drive put task/data datasets/q3` sends every file under it. `la drive` takes paths as they sit inside `personal/`. Add `--note "why"` to record the reason in the file's history. If `put` is interrupted, run the same command again; it continues from what the Drive already holds.

Write and Edit save at most 50 MB at a time by default; write a larger file in the workspace and copy it in. Otherwise a file may be as large as the room left in the Drive, unless the deployment sets a lower limit. A large save from Bash or code needs free disk on the computer while it is sent; when there is too little, NOT SAVED says how much more it needs.

When a save is refused because the Drive is full, tell the user: they can delete files, or clear a file's earlier versions from the Drive page. `la drive usage` shows what the Drive counts against its allowance. Only the user clears earlier versions: `la drive history clear` is refused from a computer.

## Reading

Read shows text, PDFs and images. For anything else (a spreadsheet, a parquet file), open it with code at the same path where Bash reaches the Drive. Code reads it in place, fetching only the parts it touches, so query a large dataset where it sits (DuckDB or Polars on a Parquet or CSV file) rather than copying it first. Where Bash does not reach the Drive, copy it out with `la drive get models/model.xlsx -o task/model.xlsx`; run it again to continue an interrupted download.

## Search

- Grep searches the Drive's text files only when `path` is inside it. Glob needs such a `path` or a pattern that names it: `.agents/drive/personal/**/*.md`.
- Grep may skip data files (`.csv`, `.parquet`, `.jsonl`, `.xlsx`, `.db`, archives and the like); its result names the ones it skipped. Query them with code.
- In Bash, `rg` is the fast way to search the whole Drive: it skips files that are not text, though it reads every CSV and JSON Lines file whole, so leave large datasets out with `-g '!*.{csv,tsv,jsonl,ndjson}'`. `grep -r` reads every file, which fetches each PDF and dataset from the server; use `rg`.

## Saves and versions

- Your saves to one file in one turn make one version. The user sees who made each version, with a link to this thread, and can restore an earlier one while it is kept, so save without asking first.
- By default a text file keeps every version, another file under 100 MB keeps its earlier versions 30 days and at most 10, and a larger one keeps none, so saving over it replaces it for good. `la drive history <path>` says what a file keeps. Unless the user asked to replace a large file, save the new one beside it under another name.
- Write and Edit are refused when the file changed since you read it. Read it again and make the change on what it holds now.
- A save from Bash or code reaches the server a moment after the write. One the server refuses does not fail the command: the tool result lists it under NOT SAVED, so check there before relying on the write. `NOT SAVED YET` means the computer is still sending the file, and a later result says `Saved now` or that it was not.
- `la drive history <path>` lists a file's versions; `la drive restore <path> --rev N` makes an older one current, as a new version.

## Renaming, moving, deleting

Use `mv` and `rm` where Bash reaches the Drive, else `la drive mv <from> <to>` and `la drive rm <path>`. A file keeps its history under a new name. A delete goes to the trash for up to 14 days (the oldest go sooner when the trash grows past the Drive's allowance): `la drive trash` lists it and `la drive trash restore <id>` puts an item back. The trash does not count against the Drive's allowance, so deleting is what frees room. Only the user empties the trash.

## What the user sees

The Drive page in the app's sidebar: every file, its history, and the trash. Link a Drive file as `[name](.agents/drive/personal/notes/thesis.md)`; the link opens that page at the file.
