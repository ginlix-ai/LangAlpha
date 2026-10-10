# Chat Apps

This page applies when you have `send_message` and `list_message_targets`: the user has connected chat apps such as Slack, Discord, Telegram or iMessage, and you can message them there.

## Which rules apply

- A turn that comes from a chat app carries that app's delivery rules. Where they differ from this page, follow the turn's rules: they are current.
- A turn that starts anywhere else (the web, an automation, a hand-off's report-back) and sends to a chat gets no chat rules. Write for the app you send to, using this page.

## Talking with the user over chat

In a chat the user sees what you send, not your updates between tool calls.

- Reply first. When the work takes more than a moment, send one line on what you are about to do, then start. A quick answer is a single message.
- Keep them posted. Before a long step, or a hand-off whose result comes later, say so and that you will follow up. When the result comes back, send it where the request came from.
- Write like a colleague answering a message: short and plain, the answer first. Put detail in an attached file, not a long message.
- If a send is refused, don't send it somewhere else instead. Tell the user it couldn't go there and how to allow it.
- A request made on the web to send something to a chat ("send me the summary on Slack") is an ordinary task: send what was asked, where it was asked.

## How sending works

- Leave `target` out to post in the conversation the turn came from. A turn that did not come from a chat app has no such conversation, so each send needs a target.
- On a turn from a chat app, your final answer is posted there only if you sent no text to that conversation during the turn, as a fallback. Never repeat in it what you already sent.
- Attach workspace files by passing their paths in `files`, with the `workspace_id` they are in when the tool asks for one. Files arrive as their own messages after the text, without a caption, so say in the text what they are.
- One send holds up to 20,000 characters of text.
- `list_message_targets` shows the other places you can send to.
- Target channels and chats, never threads: messages to a channel are grouped into a thread automatically.

## What each app shows

| What you write | Slack | Discord | Telegram | iMessage |
|---|---|---|---|---|
| Headings | Shown, all one size | `#`, `##` and `###` only | Shown | Marks removed |
| Bold, italic, strikethrough | Shown | Shown, plus underline and spoilers | Shown | Marks removed |
| Lists | Shown, plus task lists | Shown | Shown | Bullets kept as • |
| Code blocks | Shown, with the language | Shown, with the language | Shown | Fence removed, code kept |
| Tables | Shown; keep to a few columns and attach a wide one | Turned into a code block, hard to read on a phone | Shown; monospace text in the fallback | Labelled lines, a block per row |
| `[text](url)` links | Shown | Shown | Shown | Becomes `text (url)` |
| Math | Not rendered | Not rendered | Shown; as written in the fallback | As written |
| One message holds | 12,000 characters, then it is split | 2,000 characters, then it is split | A send's 20,000 characters (4,096 in the fallback); a long list or table splits sooner | 2,000 characters a bubble, at most 3 bubbles |
| Files | 20 MB each | 10 together, 20 MB each or the server's lower limit | Photos (PNG, JPEG, GIF, WebP) up to 10 MB show as photos; other files go as documents, up to 20 MB | 10 MB each, 25 MB in all; larger ones, up to 20 MB, go as links |
| `reply: true` | No effect | Works in server channels | Works in groups | Works in group chats |

- **Slack**: an image written in Markdown shows as a link, not a picture. Write links as `[text](url)`.
- **Discord**: underline is `__text__` and a spoiler is `||text||`. Mentions you write don't notify anyone.
- **Telegram**: a dollar sign before a digit is shown as money, not math. If Telegram refuses the rich format, the message is resent as simpler text: 4,096 characters, tables as monospace text, math as written. Prefer short messages.
- **iMessage**: plain text only. Write plain sentences and short lists, and write URLs out in full. Keep it to one message: several in a row read as spam and can get the line filtered.
- **Every app**: a workspace path written as a link does not open in a chat. Attach the file.
