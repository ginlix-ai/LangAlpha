---
title: Sharing
description: Share a LangAlpha thread or a single file with a link anyone can open without signing in, what visitors can see, what is removed, and how to turn a link off.
---

**Sharing** gives someone a link to read your work without a LangAlpha account. LangAlpha has two kinds of share link: a **thread link**, which shows a conversation, and a **file link**, which shows one file and the files it uses. A third kind, the **app link**, points at a web app the agent is serving from your computer and always stays private to you.

Anyone who has a thread link or a shared file link can open it without signing in. Treat the link as the password: share it only with people who should see the work.

## Sharing a thread

Choose Share on a thread and turn on the public link. The link shows the conversation as a read-only replay. The visitor sees the turns as you see them, but cannot send messages or change anything.

Two options control what else a visitor reaches. Both are off by default:

| Option | What a visitor can do |
|---|---|
| Allow file browsing | Browse and read the files in the thread's workspace folder |
| Allow file download | Download raw files. Turning this on also turns on file browsing |

File browsing is not limited to files the thread touched. It covers the whole workspace folder, apart from the workspace's `agent.md` notebook, agent infrastructure folders and the server-held files in `.agents/`. Secrets you saved in the vault are masked in files that are served. If a workspace holds work you do not want to show, leave file browsing off.

### What is removed from a shared thread

The server removes private details before anything leaves it, so the removal does not depend on the web page hiding them:

- **Orders and brokerage details.** Order calls, their arguments and results, receipts, and the decisions you made on approvals are dropped or emptied. A direct tool call whose arguments might name an order is emptied unless the call is shown to be harmless.
- **Approvals and questions.** The prompts the agent put to you are not shown, and a visitor cannot answer one.
- **Source records.** The per-turn records of which data and tools the agent used, which the Sources panel shows, are not sent. Workflow run details are not available in a shared view either.
- **Errors.** The text of error events is dropped, since it can name accounts or machines.
- **Account and workspace management.** Calls the Chief of Staff made to list, create or hand work to your workspaces and threads appear only as a name and id.
- **Wakeups and background notices.** Order details in a wakeup are removed, and the text of notifications the agent wrote for itself is blank.
- **Your ids.** Your user and workspace ids are stripped at every level of the stream.

Live charts still ask the visitor to sign in before they open.

### Turning it off

Press Stop in the share dialog. The link stops working at once. There is no expiry, so a link stays on until you stop it. If you share the same thread again later, it gets the same link back, so anyone who kept the old one can open it again.

## Sharing a file

Choose Share on a file, such as an HTML report, to create a file link. The link is **private** until you turn on Anyone with the link. A file link covers the file and nothing else in the workspace, and the conversation stays private.

Before the link opens to others, LangAlpha lists the exact files it will expose: the file itself plus the files it uses.

- Pages, stylesheets and Markdown links and images are found exactly.
- A path that only appears as a string inside a script is a best guess, and the list marks it "in script".
- A page linked from your file is checked one step further. Pages linked from that page are listed but not searched, so a multi-page site does not pull in everything reachable from its menu.

You review that list, then turn sharing on. The server checks that the list is still the same when you confirm. If a file was added meanwhile, sharing is refused and you see the new list, so a share never widens without you looking at it.

Two limits apply, and sharing is refused rather than cut short: at most **200 files**, and at most **256 MB** in total. The workspace `agent.md` and hidden agent folders can never be shared.

### When the file changes

A file link is a frozen list. If the file later starts using a new image or stylesheet, the dialog shows that it uses files that are not shared, and visitors will not see them until you choose Update sharing. If the file stops using some shared files, the dialog offers to stop sharing them. Use View as visitor to check what a stranger sees.

A visitor never starts your computer. If the computer is running, they read the current files. If it is stopped, they read the last backup.

### Turning it off

Stop sharing returns the link to private, and only you can open it. The link does not expire and keeps the same address, so sharing it again later reuses the same link.

## App links

When the agent serves a web app from your computer, it gives you an app link. An app link never opens for visitors. Only you can open it, and only while you are signed in, so it shows a preview of your running app rather than sharing it. The Open links setting chooses whether app and file links in a reply open in the file panel or in your browser.

:::note[Hosted and self-hosted]
**Hosted:** Links open on the service, and anyone who has one can open it without an account.

**Self-hosted:** Links open on your own server. A visitor can open one only if they can reach your install, so a server on a private network shares nothing outside it.
:::

## Related

- [Files and what persists](/concepts/files/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Workspaces](/concepts/workspaces/)
- [Brokerages and orders](/concepts/brokerages-and-orders/)
