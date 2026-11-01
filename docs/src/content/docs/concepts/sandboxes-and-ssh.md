---
title: Sandboxes and SSH computers
description: The four kinds of LangAlpha computer (Docker, Daytona, E2B and your own SSH machine), how to add a Linux server or Mac, Remote control, and what is sent to it.
---

A **sandbox** is an isolated machine where LangAlpha's agent runs commands and code. LangAlpha has four kinds of [computer](/concepts/computers/), each backed by a different provider: **Docker**, **Daytona**, **E2B**, and **SSH**, which is a Linux server or Mac that you own. The first three are machines LangAlpha creates and can rebuild. An SSH computer is your own machine, so LangAlpha logs in to it and never replaces it.

A computer keeps the provider it was created with. To change providers, create a second computer and [move a workspace](/concepts/computers/#moving-a-workspace-to-another-computer) to it.

## The four providers

| Provider | What it is | Stop behavior | Archive |
|---|---|---|---|
| Docker | A container on the machine that runs the LangAlpha server | Stops the container; the disk stays | No |
| Daytona | A container-based cloud sandbox | Stops the sandbox; the disk stays | Yes, after 7 days stopped by default |
| E2B | A cloud microVM | Pauses the sandbox, memory and disk kept; resumes in about a second | No |
| SSH | Your own Linux server or Mac | Ends the computer's own processes; your machine keeps running | No |

**Docker** runs each computer as a container on the machine that runs the LangAlpha server, so it needs no account anywhere else. A self-hosted install picks it with `SANDBOX_PROVIDER=docker`, which the `make config` wizard sets when you choose Docker. Its size is bounded by what the host has free, and Docker sandboxes share that host's CPU, memory and disk.

**Daytona** and **E2B** are cloud providers that need an API key (`DAYTONA_API_KEY` or `E2B_API_KEY` in `.env`). Each size gets its own image, built the first time it is used, which can take a few minutes. E2B offers an Extra Large size that Daytona does not, and does not offer always-on.

**SSH** is described below. For how sizes, idle stop and archive work on each, see [Computers](/concepts/computers/).

:::note[Hosted and self-hosted]
**Hosted:** The new computer dialog shows the providers the service offers to you, and your plan can limit which ones you may pick.

**Self-hosted:** `sandbox.provider` in `agent_config.yaml` (or `SANDBOX_PROVIDER`) is the default, and `sandbox.offered_providers` adds more. SSH is offered only if you list `ssh` there.
:::

## Bringing your own machine over SSH

An SSH computer lets the agent work on a Linux server or Mac you already have, with your files, tools and network. The agent runs as the login user you choose, in a folder of its own, `~/langalpha` unless you pick another (the dialog proposes the next free name if one is taken). On its first start LangAlpha installs a Python runtime under `~/.langalpha` on the machine, a download of about a gigabyte.

:::caution
The agent can do anything the account you connect can do. Connect an account made for it rather than your own. The dialog labels connecting your own machine as a preview.
:::

### Two ways to add a machine

On a Mac, turn on Remote Login in System Settings under General, Sharing first.

1. **Run one command.** The dialog shows a `curl ... | sh` command. Run it on the machine, as the user the agent should work as. It authorizes LangAlpha's key, reports the machine's address and host keys, and LangAlpha connects at once. The command expires after 15 minutes, and running it again re-checks the machine.
2. **Enter an address.** Type `user@host` (add `:2222` for another port). The dialog shows your personal LangAlpha public key and the command that appends it to the machine's `authorized_keys`. Run that, then press Connect.

LangAlpha pins the machine's host key when you add it and refuses any later connection that presents a different one. If a machine is reinstalled, use Reconnect on its computer to pair it again.

By default LangAlpha only connects to public addresses, so a machine on a private network is refused unless the operator turns on `allow_private_addresses`. The server also limits how many SSH connection attempts one account can make per hour and how many SSH computers one account can have (20 by default).

### Machines behind a router or firewall

A machine the server cannot reach, such as a laptop at home, can connect outward instead. When LangAlpha cannot dial the machine, the command installs a small service on it that keeps a tunnel open to the **tunnel gateway**. LangAlpha logs in through that tunnel, still end to end with the machine's own SSH server, so the gateway only carries bytes it cannot read. The card shows a tunnel badge, and tunnel offline when the machine has stopped checking in.

Tunnels work only where the operator runs the gateway. A self-hoster starts it with the `ssh-gateway` Compose profile and sets `SSH_GATEWAY_ADDRESS` in `.env`.

### Remote control of your Mac

**Remote control** (Preview) is the desktop app's way to add the Mac it runs on, with no SSH server and no command to paste. The app bundles `langalpha-helper`, a small background program that holds the Mac's tunnel and answers LangAlpha's logins itself. It runs as a login item, so the agent can keep working with LangAlpha closed.

- It appears in the new computer dialog as **This Mac**, only in the Mac desktop app, and only when the server accepts SSH computers and runs the tunnel gateway. The server address must start with `https`.
- You confirm a prompt naming the server and your Mac account before anything is paired. macOS may also ask you to allow LangAlpha in Login Items.
- The agent works as your Mac account, with everything it can do, including reading credentials that command line tools keep and reaching your home or office network. Its work stays in the folder you pick, but nothing on the Mac holds it there.
- You can pause it from the LangAlpha menu or in System Settings under Login Items. Removing the computer turns it off and deletes its keys from the Mac.

### What the deployment sends to your machine

Your machine belongs to you, so a LangAlpha server sends it very little. Server settings, the keys of built-in data servers and the GitHub bot token do not reach it. The only credential it receives is that computer's own data token, which lasts an hour and ends when the computer stops.

The consequence is for tools that need a vendor key held by the server. By default, a built-in data tool that needs one (the Financial Modeling Prep tools, for example) answers that its client is unavailable on an SSH computer, while keyless tools such as Yahoo Finance prices and fundamentals work as anywhere. A built-in server that is served over HTTP with data-token authentication also works, because it needs no key on the machine. The agent still has code, files and the rest of its tools.

:::note[Hosted and self-hosted]
**Hosted:** The service decides which data tools reach an SSH computer. Check a workspace's tool list to see what is available there.

**Self-hosted:** Set `sandbox.ssh.trusted_host: true` in `agent_config.yaml` to send SSH machines the same keys any computer gets. Each machine's owner can read them, so turn it on only where every user may hold those keys, such as a server only you use.
:::

### An SSH computer is never rebuilt

LangAlpha cannot replace a machine it does not own. If the machine is unreachable, the computer waits for you. If its folder goes missing, the workspace offers to restore from its backup or keep the empty folder. Backups of workspace files are on by default and can be turned off per SSH computer, and they leave out version control and dependency folders.

Stopping an SSH computer ends only the agent's own processes, and idle stop and always-on apply as on any computer. Removing the computer takes LangAlpha's key and tunnel off the machine. Your files in the folder stay, unless you tick the option to delete LangAlpha's files too.

## Trust between workspaces

Every workspace on one computer runs as the same operating system user. Their code can read each other's files and the secrets and server connections shared on that computer. Choosing which tools a workspace has switched on shapes what the agent does, but it is not a security boundary. To keep work apart, put it on separate computers. The Chief of Staff is the exception: with more than one computer, it can read across all of yours.

## Related

- [Computers](/concepts/computers/)
- [Which computer should I use?](/compare/which-computer/)
- [Workspaces](/concepts/workspaces/)
- [Files and what persists](/concepts/files/)
