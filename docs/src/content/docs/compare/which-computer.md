---
title: Which computer should I use?
description: Compare Docker, Daytona, E2B, your own SSH machine and Mac Remote control for LangAlpha, and learn when a second computer is worth creating.
sidebar:
  order: 5
---

Use the default computer your LangAlpha gives you unless a specific need points elsewhere. On the hosted service that is a cloud computer. On a self-hosted install it is whichever provider you set up, which is Docker if you chose no cloud account. Pick your own machine over SSH when the work needs data, tools or a network that only that machine has, and use Remote control when that machine is the Mac you are sitting at.

You do not decide per workspace. A workspace lives on a [computer](/concepts/computers/), and a computer keeps its provider for life, so the choice is about where you want a group of workspaces to run. [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/) explains each option in depth.

## The options side by side

| | Docker | Daytona | E2B | SSH computer | Remote control |
|---|---|---|---|---|---|
| Where it runs | A container on the server's own machine | A cloud sandbox | A cloud microVM | Your Linux server or Mac, reached over SSH | The Mac running the desktop app |
| Stop and resume | Stops the container, disk kept | Stops the sandbox, disk kept | Pauses it, memory and disk kept, resumes in about a second | Ends the computer's processes, your machine keeps running | The same as SSH |
| Archive | No | Yes, after 7 days stopped by default | No | No | No |
| Sizes | Operator's container size, raised by a size choice | Small, Medium, Large | Small, Medium, Large, Extra Large | None, it uses the machine as it is | None |
| Always-on | Yes | Yes | No | Yes | Yes |
| Rebuilt from backup if lost | Yes | Yes | Yes | Never | Never |
| Who can offer it | The self-hosted default when no cloud account is set | Both, with an API key when self-hosted | Both, with an API key when self-hosted | Where the deployment offers SSH | Mac desktop app, where the deployment offers SSH and tunnels |

On a hosted plan, which providers, sizes and how many computers you can create can be limited. A self-hosted install has no such limits.

## Which one fits your work

**You just want it to work.** Use the default. On hosted, that is a ready cloud computer. Self-hosting without cloud accounts, Docker needs nothing but the Docker daemon, but it shares the server's CPU, memory and disk.

**You run heavy or long jobs.** Pick a larger size, or E2B's Extra Large (4 vCPU, 8 GB, 20 GB disk) for the most room. Remember that a size covers the whole computer, so all its workspaces share it. Changing size later recreates the machine and keeps only backed-up files.

**You want fast pauses and resumes.** E2B resumes in about a second and keeps memory, but it cannot be always-on because E2B limits how long one sandbox may run. If automations or wakeups need a machine that stays up, use Daytona or Docker.

**You want a machine that is yours.** Use an SSH computer for a server in your own account, data that must stay on a private network, or software installed only there. LangAlpha never rebuilds it, and its files stay on your disk when you remove the computer unless you choose to delete them. The trade is trust: the agent acts with the login user's full power, so create an account for it.

**You are on a Mac and want the agent on it.** Use Remote control in the Mac desktop app. It needs no SSH server, works when LangAlpha is closed, and also works behind a home router. It carries the same trust trade as any SSH machine, and it is in preview.

## When to make a second computer

A second computer is rarely needed for a new idea, because a new workspace is just a new folder. Make one when you need a real boundary:

- **Isolation.** All workspaces on a computer share an operating system user and credentials. Put work you want kept apart on separate computers.
- **Different resources.** One heavy modeling workspace on a Large computer, with the rest staying on Small ones, instead of enlarging every workspace together.
- **Separate disk.** One workspace that fills the disk should not stall the others.
- **Separate power state.** Starting and stopping applies to every workspace on a computer. Keep the workspaces that need always-on on one computer and let the rest stop when idle.
- **Your own machine for part of your work.** Keep the cloud computer as primary for most work and add an SSH computer or This Mac for the workspaces that need it. A computer you brought can never be primary.

You can [move a workspace](/concepts/computers/#moving-a-workspace-to-another-computer) between computers later, so the first choice is not final.

## Related

- [Computers](/concepts/computers/)
- [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/)
- [Workspaces](/concepts/workspaces/)
- [Hosted or self-hosted?](/get-started/hosted-or-self-hosted/)
