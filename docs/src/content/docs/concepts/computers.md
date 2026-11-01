---
title: Computers
description: A computer is the machine your LangAlpha workspaces run on. How the primary computer, sizes, the shared disk, idle stop, always-on and moving workspaces work.
---

A **computer** is the machine your workspaces run on. Each workspace is a folder on a computer, and the agent runs its code, installs packages and keeps its files there. A computer can be a cloud sandbox, a Docker container on the LangAlpha server, or a Linux server or Mac that you bring yourself. Everything on one computer starts, stops and fills the same disk together.

You rarely manage one directly. Your first workspace creates your first computer, and every workspace after that lands on it unless you pick another. Open the Computers dialog when you want to see the state of a machine, change its size, stop it, or add another one.

## What a computer holds

- **Workspaces.** Each workspace is a folder named after it. All of them share the computer's disk, its installed packages and its operating system user. See [Workspaces](/concepts/workspaces/) for the folder layout.
- **Home.** The folder the [Chief of Staff](/concepts/chief-of-staff-and-analysts/) works in. It sits beside your workspace folders on your primary computer.
- **Running work.** Turns, background commands and the programs the agent starts all run on the computer their workspace belongs to.

Different providers build the machine differently. [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/) explains what each one means, and [Which computer should I use?](/compare/which-computer/) helps you choose.

## The primary computer

The primary computer is where new workspaces are created and where Home lives. Your first computer is primary. You can pick a different computer for a single new workspace, or use Make primary on a computer to change the default. Making a computer primary moves Home's files to it, which takes a moment.

Two rules follow from Home living on the primary:

- A computer you brought over SSH can never be primary. The server can rebuild a cloud computer from its backup, but it cannot rebuild your own machine if the folder is wiped or the machine is gone, and Home would go with it.
- The primary computer cannot be removed. Any other computer can be removed, and the dialog lists every workspace on it first, because removing the computer deletes those workspaces and their threads.

## Sizes

A computer's size is set when you create it and can be changed later. The dialog shows the names in the first column; the second column is what a self-hosted `agent_config.yaml` calls them.

| Size | Setting name | vCPU | Memory | Disk | Where it exists |
|---|---|---|---|---|---|
| Small | `standard` | 1 | 2 GB | 5 GB | Daytona, E2B, Docker |
| Medium | `performance` | 2 | 4 GB | 10 GB | Daytona, E2B, Docker |
| Large | `max` | 2 | 8 GB | 10 GB | Daytona, E2B, Docker |
| Extra Large | `xlarge` | 4 | 8 GB | 20 GB | E2B only |

Daytona's largest sandbox is smaller than Extra Large, so only E2B offers it. On Docker the numbers are a floor set by the operator (2 CPUs and 4 GB of memory by default), so a size can raise a container but never shrinks it, and a disk size applies only when the Docker storage driver supports per-container quotas. A computer on your own SSH machine has no size: it uses the machine as it is.

### Changing the size

Choose Change on the computer card and pick another size. LangAlpha backs up every workspace on the computer, recreates the machine at the new size, and restores the files. Installed packages, caches and running programs are not kept, so the agent reinstalls what it needs. The workspaces are unavailable for a few minutes, and you can close the dialog while it runs.

The change is refused in three cases:

- An agent is working on the computer. Wait for the running turn to finish and try again.
- A size change is already running on that computer.
- Some files could not be backed up first. Nothing changes, and the dialog names the files so you can move or delete them.

If the computer is stopped, the new size applies the next time it starts.

## One disk for every workspace

All workspaces on a computer share one disk, sized by the computer's size. The storage view shows how full it is, splits the use by workspace, and adds a line for system files, caches and shared files. It can only measure a running computer, so for a stopped one you see the last reading.

LangAlpha reacts to free space on the whole disk:

| Free space | What happens |
|---|---|
| Under 250 MB | The storage bar changes color. |
| Under 100 MB | A banner warns that the computer is running low on disk space. |
| Under 50 MB | The banner turns critical and the agent is told, so it avoids installs and downloads. Saving files and running code can start to fail in every workspace on the computer. |

The warning comes late, so check free space before you ask the agent to download large data. If one workspace fills the disk, every other workspace on that computer feels it. Delete files, or [change to a size with more disk](#changing-the-size).

## Starting and stopping

Start and Stop apply to the whole computer and therefore to every workspace on it. The dialog shows the workspace count and warns before you stop one. A stopped computer starts again when you open a workspace on it and send a message, or when you press Start.

While a computer is stopped you can still browse and download the files that were backed up. Writing, uploading and running code need it running. [Files and what persists](/concepts/files/) lists what is kept across each kind of stop.

### Idle stop

A running computer stops by itself after about 50 minutes without activity. Activity on any workspace counts, and a computer with a running turn is never stopped. The 50 minutes is the provider's 60 minute auto-stop minus a 10 minute margin, so the server stops the computer cleanly before the provider does.

### Always-on

Always-on keeps a computer from stopping when it is idle. Turn it on from the computer's controls when you want automations, wakeups or long jobs to find the machine running. It is not offered on E2B, because E2B limits how long one sandbox can run continuously. On your own SSH machine it keeps the computer's processes from being stopped on idle; the machine itself is yours.

:::note[Hosted and self-hosted]
**Hosted:** Your plan can limit how many computers you can create, which sizes and providers you can pick, and whether you can use always-on.

**Self-hosted:** There are no plan limits. The idle window follows each provider's `auto_stop_interval` in `agent_config.yaml` (3600 seconds by default). Which providers appear in the new computer dialog is set by `sandbox.provider` and `sandbox.offered_providers`.
:::

## Archive

On Daytona, a computer that has been stopped for 7 days is archived, which moves its disk to cold storage. It keeps the whole disk but takes longer to start. After 90 days dormant the sandbox is deleted, and the next start builds a new machine and restores the workspace folders from their backup. Those intervals are `auto_archive_interval` and `auto_delete_interval` in `agent_config.yaml`.

You can also press Archive on a stopped computer in its overview. E2B pauses a sandbox instead, and resuming takes about a second. Docker does not archive.

## Moving a workspace to another computer

Use Move to computer in a workspace's menu to put it on another of your computers. Its files, threads and settings go with it. Installed dependencies, caches and version control history are not copied. They stay on the old computer and are removed after 7 days, except on a machine you brought yourself, where they are set aside in a leftovers folder and kept.

A move is refused while the workspace has work running, or while a command is still running on its computer, and only one move runs at a time. You can close the dialog and let it finish, and you can cancel it until it is far along. If the old computer cannot be reached, you can restore the workspace on the new computer from its last backup instead, which leaves out anything changed since then.

## Related

- [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/)
- [Workspaces](/concepts/workspaces/)
- [Files and what persists](/concepts/files/)
- [Which computer should I use?](/compare/which-computer/)
