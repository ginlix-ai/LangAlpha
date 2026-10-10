# Channel Routing

Which conversation a message the user sends on a channel (Slack, Discord, Telegram, iMessage or Feishu) reaches, and why an answer can come from a different conversation than the one they expected.

## The rules every app shares

- A conversation is one thread, and it runs in one place: All workspaces, where the Chief of Staff answers from Home, or one workspace, where its Analyst answers. It keeps that place for as long as it lasts.
- A message that isn't a reply goes to the chat's current conversation, or starts a new one; each app below says which.
- A reply goes to the conversation of the message it answers, not to the chat's current one. In Slack, Discord and Feishu a reply is a message in a thread or topic; in Telegram it is a message in a topic, or, in a chat without topics, a reply to one message; in iMessage it is an inline reply.
- A message that starts with `/new` starts a new conversation and runs the rest of the message there, on every app, even where the app runs no `/new` command, as in a Slack thread. It never changes the place.
- A new conversation starts in the place the user set for that chat with `/workspace`; in a group, each person has their own. A chat without one uses the account's default, and with no default it runs in All workspaces. `.agents/user/channels/channels.json` holds this user's settings (`default`, and each app's `chats`): read it before telling the user where a chat runs now.
- Changing a chat's place doesn't move a conversation already started; each app below says when the next message starts a new one.
- On Slack, Discord, Telegram and Feishu, a message the user sends in a conversation while its answer is running joins that answer. On iMessage it waits, and runs when the answer is done.
- A result you handed to an Analyst comes back in the thread, topic or chat the request came from.
- Someone who hasn't connected their account gets a prompt to connect it, not an answer.

## Why one chat shows two conversations

One chat can show answers from more than one conversation. In a group, each person has their own. In a Telegram chat without topics, each person has one in All workspaces and one in a workspace. And a chat shows whatever you sent there from another conversation or an automation. In Slack, a Discord server and a Telegram chat with topics, those sends go in your conversation's own thread or topic there, opening one if it has none, and the user's messages there come back to you. In iMessage, a Telegram chat without topics and a Discord DM, they sit among the chat's own messages, so two answers in one chat can come from two conversations with different histories. There, the user's reply to your message comes back to you, except in a Discord DM; a message that isn't a reply goes to the chat's current conversation, and anyone else's reply goes to their own. Tell the user which message their reply answered and which conversation that belongs to.

## Slack

- In the user's one-to-one DM with the app every message is answered. In a channel or group DM, the user mentions the app to start. Inside its thread no mention is needed, and only the person who started the thread can continue it; anyone else gets a lock and no answer.
- A message at the top of a DM or channel starts a new conversation, and the answer goes in a thread under it. Reply in that thread to continue. In Slack's assistant panel each chat is one conversation; start a new chat for a new one.
- `/new <text>` sent as a message starts a new conversation with the text, inside a thread too. As a command, it posts the text in the chat and answers it in a thread under it. The `/new` command alone makes the user's next message in that chat start one.
- After `/workspace`, a thread already under way stays where it was; new messages at the top, new assistant chats and `/new` use the new place.
- There is no command to stop an answer.
- A message you send goes in your conversation's thread in that chat, or at the top, starting one. The user's replies there come back to you. `new_thread` starts another thread; the old one still reaches the same conversation.

## Discord

- In a server channel, the user mentions the app to start; the answer opens a thread from that message. Inside the thread no mention is needed, and only the person who started it can continue it; anyone else gets a lock and no answer.
- Each mention in a channel starts a new conversation in a new thread. A DM is one conversation that goes on until `/new` or `/workspace`, even re-picking the same place.
- `/new <text>` runs the text in a new conversation: from a channel, in a new thread; in a DM, or in a thread that is the user's, in place. `/new` alone makes the user's next message there start one.
- `/workspace` in a server needs the Manage Channels permission and sets the place for that person in that channel; inside a thread it sets the thread's channel. After `/workspace` in a DM or a thread, the next message there starts a new conversation.
- There is no command to stop an answer.
- A message you send to a server channel goes in your conversation's thread there, or opens one from your first message; the user's replies there come back to you. A DM has no threads: the user's next message there, even a Discord reply to yours, goes to the DM's own conversation.

## Telegram

- In a private chat every message is answered. In a group, the user mentions the bot, replies to one of your messages, or uses a command. In a topic the bot opened, every message is answered except replies to other people.
- Without topics, each person has two conversations in the chat, one in All workspaces and one in a workspace, and after 4 hours without a message the next starts a new one. With topics, each person has their own conversation in each topic, General included, with no 4-hour restart. A private chat with topics turned on works the same way.
- `/new` (or `/reset`) starts a new conversation and runs any text after it. In a chat with topics it opens a new topic, named from the text if there is any.
- `/workspace` to the place the user is already in changes nothing. Without topics, a move into a workspace starts a new conversation there, even a workspace they used before, and a move to All workspaces goes back to their All workspaces conversation if it was used in the last 4 hours. With topics, after a move each of their topic conversations starts over with their next message there.
- `/stop` (or `/cancel`) stops the user's running answer in this chat or topic, and their running answers to replies. In a private chat, the Stop button on an answer stops that answer.
- In a chat without topics, a reply to one of your messages from the last 30 days continues the conversation that posted it, if the user is the one it was answering. The answer replies to their message, and the chat's own conversation is left as it was. A reply to an older message, to a notice, or by someone else is an ordinary message carrying the quoted text. A message with files never answers a question; it runs as its own message.
- A message you send: without topics, the user's reply to it comes back to your conversation. With topics, it goes in your conversation's own topic, and the user's messages there come back to you; if the bot can't open topics in that group, it goes to the main chat and replies don't come back. `new_thread` opens a new topic. A topic you opened keeps its conversation through a move.

## iMessage

- Every message is answered. Commands work with or without the slash when the word stands alone: `new`, `workspace`, `back`, `stop`, `resume` (or `start`), `help`.
- Each person has one conversation in the chat, with no time limit.
- `/new <text>` runs the text in a new conversation. `new` alone makes the next message start one.
- `/workspace` shows a numbered list: `/workspace 2` picks the second, and a number on its own is read as a question. `back` returns to where the user was before their last switch. Any change of place, All workspaces included, makes the next message start a new conversation.
- `stop` turns off everything you'd send to that chat unasked, hand-off and automation results included, and `resume` turns it back on. Answers to the user's own messages still come. Nothing stops an answer that is running.
- An inline reply to one of your messages from the last 30 days continues the conversation that posted it, if the user is the one it was answering. The answer goes in that reply thread, and so does what follows a question it asks. When the message came from a conversation other than the chat's own, so do its hand-off results. The chat's own conversation is left as it was.
- You can message the user's one-to-one chat, or a group they've texted you in and picked for you, but not a chat where they sent `stop`. The user's inline reply comes back to your conversation; a message that isn't a reply goes to the chat's own.

## Feishu

- In a DM every message is answered. In a group, the user mentions the app, or writes in a topic where it was mentioned in the last 30 days. If the user's company connected its own Feishu app, its admin can require a mention on every group message.
- Each topic is a conversation, one per person, in a DM too. A message at the top starts a new one, and replies in its topic continue it.
- `/new` alone makes the topic it was typed in start over with the user's next message.
- After `/workspace` changes the place, each of the user's conversations in that chat starts over with their next message there.
- There is no command to stop an answer.
- You can't send to Feishu. A hand-off's result comes back as a reply in its topic.
