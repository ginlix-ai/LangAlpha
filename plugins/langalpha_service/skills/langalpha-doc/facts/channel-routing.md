# Channel Routing

Which conversation a message the user sends on a channel (Slack, Discord, Telegram, iMessage or Feishu) reaches, and why an answer can come from a different conversation than the one they expected.

## The rules every app shares

- A conversation is one thread, and it runs in one place: All workspaces, where the Chief of Staff answers from Home, or one workspace, where its Analyst answers. It keeps that place for as long as it lasts.
- A message that isn't a reply goes to the chat's current conversation, or starts a new one; each app below says which.
- A reply goes to the conversation of the message it answers, not to the chat's current one. In Slack, Discord and Feishu a reply is a message in a thread or topic; in Telegram it is a message in a topic, or, in a chat without topics, a reply to one message; in iMessage it is an inline reply.
- A message that starts with `/new` starts a new conversation and runs the rest of the message there, on every app, even where the app runs no `/new` command, as in a Slack thread. It never changes the place. In a Slack or Discord thread that isn't the user's it gets a lock, except that `/new <text>` typed in a thread nobody owns continues that thread's conversation.
- A new conversation starts in the place the user set for that chat with `/workspace`; in a group, each person has their own. A chat without one uses the account's default, and with no default it runs in All workspaces. `.agents/user/channels/channels.json` holds this user's settings (`default`, and each app's `chats`): read it before telling the user where a chat runs now.
- Changing a chat's place doesn't move a conversation already started; each app below says when the next message starts a new one.
- On Slack, Discord, Telegram and Feishu, a message the user sends in a conversation while its answer is running joins that answer. A `/new` with text, or the first message after a bare `/new` or after a `/workspace` that starts the conversation over, waits instead and runs as a new conversation when the answer ends; anything sent after it waits behind it. In a Telegram chat with topics, `/new` opens a new topic and runs there at once. On Telegram and Feishu a message with files also waits, and runs on its own. On iMessage it waits for any answer running in that chat, a group's included, then runs; if the answer it waited for was to the same person and asked them a question, it answers it.
- A result you handed to an Analyst comes back in the thread, topic or chat the request came from.
- Someone who hasn't connected their account gets a prompt to connect it, not an answer.

## Why one chat shows two conversations

One chat can show answers from more than one conversation. In a group, each person has their own. In a Telegram chat without topics, each person has one in All workspaces and one in a workspace. And a chat shows whatever you sent there from another conversation or an automation. In Slack, a Discord server, Feishu and a Telegram chat with topics, those sends go in your conversation's own thread or topic there, opening one if it has none, and the user's messages there come back to you. In iMessage, a Telegram chat without topics and a Discord DM, they sit among the chat's own messages, so two answers in one chat can come from two conversations with different histories. There, the user's reply to your message comes back to you, except in a Discord DM; a message that isn't a reply goes to the chat's current conversation, and anyone else's reply goes to their own. Tell the user which message their reply answered and which conversation that belongs to.

## Slack

- In the user's one-to-one DM with the app every message is answered. In a channel or group DM, the user mentions the app to start. Inside its thread no mention is needed, and only the person who started the thread can continue it; anyone else gets a lock and no answer.
- A message at the top of a DM or channel starts a new conversation, and the answer goes in a thread under it. Reply in that thread to continue. In Slack's assistant panel each chat is one conversation; start a new chat for a new one.
- `/new <text>` sent as a message starts a new conversation with the text, inside a thread too. As a command, it posts the text in the chat and answers it in a thread under it. `/new` alone makes the user's next message in that chat within 24 hours start one, even a reply in a thread already under way; typed alone in a thread, it starts only that thread over.
- After `/workspace`, a thread already under way stays where it was; new messages at the top, new assistant chats and `/new` use the new place.
- There is no command to stop an answer.
- A message you send goes in your conversation's thread in that chat, or at the top, starting one. The user's replies there come back to you. `new_thread` starts another thread; the old one still reaches the same conversation.

## Discord

- In a server channel, the user mentions the app to start; the answer opens a thread from that message. Inside the thread no mention is needed, and only the person who started it can continue it; anyone else gets a lock and no answer. A thread or forum post someone else started gets no answer unless the app has been added to it.
- Each mention in a channel starts a new conversation in a new thread. A DM is one conversation until `/new`, `/workspace` (even re-picking the same place) or a change to the DM in channels.json, after which the next message starts a new one if it comes within 24 hours; otherwise the old one goes on, in its old place. Changing the account default doesn't restart it.
- `/new <text>` runs the text in a new conversation: from a channel, in a new thread; in a DM, or in a thread that is the user's, in place. `/new` alone makes the user's next message there within 24 hours start one.
- `/workspace` in a server needs the server-wide Manage Channels permission and sets the place for that person in that channel; inside a thread it sets the thread's channel. After `/workspace` in a thread, the next message there within 24 hours starts a new conversation.
- There is no command to stop an answer.
- A message you send to a server channel goes in your conversation's thread there, or opens one from your first message; the user's replies there come back to you. Where the app can't start a thread, your message stays in the channel and replies to it don't come back; your next send there tries the thread again. A DM has no threads: the user's next message there, even a Discord reply to yours, goes to the DM's own conversation.

## Telegram

- In a private chat every message is answered. In a group, the user mentions the bot, replies to one of your messages, or uses a command. In a topic the bot opened, every message is answered except replies to other people.
- Without topics, each person has two conversations in the chat, one in All workspaces and one in a workspace, and each starts over at the next message once 4 hours have passed since their last message in it that wasn't a reply to one of your messages or an answer to a question. With topics, each person has their own conversation in each topic, General included, with no 4-hour restart. Where the bot runs private chats with topics, a private chat works the same way.
- `/new` (or `/reset`) starts a new conversation and runs any text after it. In a chat with topics it opens a new topic, named from the text or else "New thread"; if no topic can be opened, the current topic starts over.
- `/workspace` to the place the user is already in changes nothing. Without topics, `/workspace` into a workspace starts a new conversation there, even one they used before; a change in channels.json or the account default starts one only if the old one began in another workspace. A move to All workspaces goes back to their All workspaces conversation, which starts over only by the 4-hour rule. With topics, after a move each of their topic conversations starts over with their next message there; after an account-default change, only those that began elsewhere.
- `/stop` (or `/cancel`) stops the user's running answer in this chat or topic, and their running answers to replies. In a private chat, the Stop button on an answer stops that answer.
- In a chat without topics, a reply to one of your messages from the last 30 days continues the conversation that posted it, if that conversation is the user's. The answer replies to their message, and the chat's own conversation is left as it was. A reply to a notice that ended a turn, to a command's answer, to an older message, or by someone else is an ordinary message. Every reply carries the quoted words and any photo or file on the quoted message. A message with files, or a reply to one, never answers a question: it runs as its own message, and the question its conversation was waiting on closes.
- A message you send: without topics, the user's reply to it comes back to your conversation. With topics, it goes in your conversation's own topic, and the user's messages there come back to you; if the bot can't open topics in that group, it goes to the main chat and replies don't come back. `new_thread` opens a new topic where the chat has topics; without them it changes nothing and says so. A topic you opened keeps its conversation through a move.

## iMessage

- Every message is answered. Commands work with or without the slash when the word stands alone: `new` (or `reset`), `workspace`, `back`, `stop` (or `unsubscribe`), `start` (or `resume`), `help`.
- Each person has one conversation in the chat, with no time limit.
- `/new <text>` runs the text in a new conversation. `new` alone makes the next message within 24 hours start one.
- `/workspace` shows a numbered list, All workspaces first: `/workspace 2` picks the second entry of a list shown in the last 15 minutes (an older list is shown again), `/workspace <name>` picks by name, and a number on its own is read as a question. `back` returns to where the user was before their last switch. Any change of place, by `/workspace`, `back`, channels.json or the account default, All workspaces included, makes the next message start a new conversation; re-picking the current place changes nothing.
- `stop` turns off everything you'd send to that chat unasked, hand-off and automation results included, and `start` turns it back on. Answers to the user's own messages still come. Nothing stops an answer that is running, but a hand-off result not yet posted when `stop` arrives isn't posted.
- An inline reply to one of your messages from the last 30 days continues the conversation that posted it, if that conversation is the user's. The answer goes in that reply thread, and so does what follows a question it asks. When the message came from a conversation other than the chat's own, so do its hand-off results. The chat's own conversation is left as it was. A reply to a notice that ended a turn, to a command's answer, to an older message, or by someone else is an ordinary message. Only a reply's own words and files reach you, never the message it quotes.
- You can message the user's one-to-one chat, or a group they're still in, have texted you in and picked for you, if they also have a one-to-one chat with you. You can't message a chat where they sent `stop`, except while answering their message there. Sends from outside the conversation are limited to 20 a day across iMessage. The user's inline reply comes back to your conversation; a message that isn't a reply goes to the chat's own.

## Feishu

- In a DM every message is answered. In a group, the user mentions the app, or writes in a topic where it was mentioned in the last 30 days. On a company's own Feishu app every group message needs a mention, unless the company gave the app all group messages; even then, whoever connected it can require a mention again.
- Each topic is a conversation, one per person, in a DM too. A message at the top starts a new one, and replies in its topic continue it.
- `/new` alone makes the topic it was typed in start over with the user's next message there within 24 hours.
- After the place changes, by `/workspace`, channels.json or the account default, each of the user's conversations in that chat starts over with their next message there. A move to All workspaces does this only within 24 hours of a `/workspace` or channels.json change, and never for the account default.
- There is no command to stop an answer.
- You can message the user's DM, or a group they picked for you and talked to you in within the last 90 days; a group they haven't opens up once they mention the app there. A message you send goes in your conversation's topic in that chat, or at the top, opening one. The user's replies in that topic come back to you, in a group without a mention. `new_thread` starts another topic; the old one still reaches the same conversation. A hand-off's result comes back as a reply in its topic.
