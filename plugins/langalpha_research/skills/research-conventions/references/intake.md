# Intake

Deferred reference for `research-conventions`. Load it before asking the user anything at the start of a task, and when deciding whether to ask at all.

## Read the mandate before composing a question

The investor mandate lives in user-tier memory and follows the user across every workspace. Its slots: **mandate and vehicle**, **benchmark**, **horizon**, **risk posture and sizing norms**, **sectors and geographies in and out of scope**, and **delivery preferences**. Read them, then ask only about the forks they leave open.

Anything durable that comes back from a question goes into that memory, so the next skill inherits it instead of asking again. Capture stays explicit-only: the user volunteering a fact or asking you to remember one is the trigger. Starting a research task is not, and a missing profile never delays the first answer.

## Question limits

At most three questions before substantial work begins, and fewer is better. Each question:

- offers two or three options, the recommended one first and labelled as recommended,
- says in the option text what the choice changes about the output,
- leaves free text to the interface rather than spending an option on "something else".

`AskUserQuestion` carries one question per call. When the user skips, take the recommended option and disclose it as an assumption.

## Ask once

A preference resolved once holds for the rest of the run and for every skill downstream of it. A skill running as an input to another workflow does not prompt at all: it inherits the caller's answers and returns what it was asked for.

## Precedence over a skill's own pauses

A step inside a skill that waits for the user (an outline review, a stop after each task of a multi-task pipeline) is an intake exception that step declares. It holds when the user invoked that skill directly and nothing else. It yields to this file, and the skill continues without waiting, when the skill runs as an input to another workflow or the user asked for the whole pipeline in one request.

## Present, do not block

Our turns are not a question-and-answer loop. After intake, work in stages: present each stage as it finishes, say what you are about to build next, and carry on unless the user objects. A finished stage with its assumptions stated gives the user a better place to intervene than a question does, and it costs them nothing when the defaults were right.

## Forks worth asking about

Ask when the answer changes what gets built:

- **Workflow.** Which of two plausible deliverables the request means: a screen or a full initiation, a preview or a model update.
- **An artifact the user already has.** Extend their file, rebuild it, or produce a companion. This one bears on every workbook and deck task, and guessing wrong wastes the whole build.
- **Mode.** Which mode of a multi-mode skill: a research packet or an applied update, a new thesis or a refresh of one.
- **Scope.** The universe, the peer set, the segment boundary.
- **Horizon.** The valuation or thesis horizon, when the mandate memory does not fix it.
- **Framing.** Whether the user wants the case made, the case tested, or both.

Everything outside that list is a default you take and disclose.

## Disclose the defaults you took

Assumed defaults belong in the message that delivers the artifact rather than inside it: name the default, why it was taken, and the one-line change if it was wrong. Assumptions that bear on the numbers also stay in the artifact, labelled per `.agents/skills/research-conventions/references/evidence.md`.
