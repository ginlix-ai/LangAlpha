# Skills

A skill is a folder holding a `SKILL.md` with YAML frontmatter (`name`, `description`) and any files it points to. The skills block in your context lists each available skill with its description; read its `SKILL.md` to use it. A skill is instructions and files only: it cannot add a tool. Ship scripts for yourself to run, and bring a real tool in as an MCP server (plugins.md).

## Where skills live

- `.agents/skills/` in your workspace holds this workspace's own skills as real folders, and links to the skills shared across workspaces: the platform's, and those the user added for their whole account, directly or through a plugin. A skill the user switched off for this workspace has no link.
- The shared skills sit once in `../.agents/skills/`, behind every workspace's links. The whole set there is uploaded again whenever any shared skill changes, so an edit to one reaches every workspace until then and is lost after. Never edit a shared skill or write into `../.agents/skills/`.
- To adapt a shared skill, copy it with the links resolved to a name no skill has yet, `cp -rL .agents/skills/<skill> .agents/skills/<new-name>`, set `name:` to the new folder name, and replace every `.agents/skills/<skill>/` path inside its files with the new name, or its instructions keep running the shared copy's scripts. A plain `cp -r` copies the link, and your edits land on the shared skill.

## Adding a skill to this workspace

1. Put the folder at `.agents/skills/<name>/` with `SKILL.md`, spelled exactly so, directly inside it. To take one from a repository, clone it into `/tmp` and copy the skill folder over with `cp -rL`, leaving out `.git`. A symbolic or hard link anywhere inside keeps the folder from being saved, and a skill folder that is itself a link is deleted when the turn completes.
2. Give it a name no other skill has. `name` must equal the folder name: lowercase letters, digits and single hyphens, not starting or ending with one, at most 64 characters. Names of platform skills and commands are taken, including `x-api`, `dashboard`, `compact`, `summarize`, `offload` and `subagent`; a folder with one of those names is never saved, and one named after a platform skill hides that skill in this workspace until you delete it.
3. `description` is required, at most 1024 characters, and decides whether a skill gets read, so say what it does and when to use it. Write `name:` and `description:` at the top level with each value on the same line as its key; quote a description containing `: `. No other `name:` or `description:` line may follow, not even an indented one, and no `#` comment may follow the name. A value on the next line or a name that differs from the folder makes the skill be skipped without a message.
4. Every saved skill answers to `/<name>`. An optional `command: <alias>`, bare with the same rules as a name, replaces that with `/<alias>`; it is read on the first save only, and dropped without a message if it is taken.
5. Stay within 64 files counting `SKILL.md`, 1 MB per file, 8 MB in total, and 64 KB for `SKILL.md`. Only `LICENSE.txt` and `__pycache__` are left out; `node_modules`, `.git` and `.DS_Store` count. An account holds at most 200 skills.

## When it is saved

- A new or changed skill is saved when a turn completes, when the computer next starts, or when the user changes this workspace's skills. A turn that fails, is cancelled or stops to wait for the user does not save it, but the folder stays for the next save. The save runs just after the turn ends, so a message sent right away can arrive before it.
- A saved skill is this workspace's skill: the user sees and manages it in the workspace's settings, and from the next turn it is listed in your context. Until then it is not listed, but you can read it yourself.
- Edits to this workspace's skills, including ones the user uploaded, are saved back the same way. When you and the user both changed one, your copy replaces theirs if it passes validation.
- Check with `jq '.skills["<name>"].sync | del(.statCache)' .agents/skills/skills-lock.json`. The latest version is saved when it has `linkedSkillId` and no `lastFailedSync`; `lastFailedSync.kind` is `validate`, `reserved` or `download` (too many files or too large), with a `reason`. A skill whose name differs from its folder, has no description, holds a link or is over an account limit leaves no failure record. Never edit that file.
- To remove a skill of this workspace, delete its folder; the saved copy goes at the next save, unless the user changed it in the meantime, in which case it comes back. A skill the user switched off for this workspace loses its folder at the next save, and recreating the folder does not switch it back on. Deleting a link to a shared skill does nothing lasting: only the user can switch a shared skill off, for this workspace or for the account.

## Making a skill available in every workspace

You cannot add a skill for the whole account. Tell the user one of these:

- **Move a workspace skill**: after the turn that saved it, Plugins page, Skills tab, the skill under "Workspace: <workspace name>", its scope control, "Move to all workspaces". Once moved, this workspace's folder stops saving and hides the shared copy; delete it so the link comes back.
- **Upload a skill**: Plugins page, Add, "Upload skill", with a `.zip` of at most 2 MB holding `SKILL.md` at its root or inside one top-level folder named like the skill. Build it in the workspace, for example `python -c "import shutil; shutil.make_archive('<task>/<name>', 'zip', '.agents/skills', '<name>')"`, and link the zip; the user downloads it from the file panel.
- **Install a plugin** that carries it (plugins.md). A repository holding only skill folders, with no plugin manifest, cannot be installed as a plugin, even with a marketplace listing; upload its skills one by one or copy them in here.
