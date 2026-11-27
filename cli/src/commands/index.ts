import { auth } from "./auth.js";
import { automations } from "./automations.js";
import type { Command } from "./command.js";
import { computers } from "./computers.js";
import { drive } from "./drive.js";
import { driveMemo } from "./driveMemo.js";
import { models } from "./models.js";
import { skills } from "./skills.js";
import { threads } from "./threads.js";
import { workspaces } from "./workspaces.js";

export const COMMANDS: readonly Command[] = [...auth, ...computers, ...workspaces, ...threads, ...automations, ...drive, ...driveMemo, ...skills, ...models];
