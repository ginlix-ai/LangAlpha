/**
 * Whether a text the agent wrote mid-turn reads as an answer or as narration
 * of its work, from the text alone.
 *
 * A turn's position in the transcript says less than it seems. The agent often
 * writes its whole answer and then makes one more call (ticking its todo list,
 * asking a question, saving a note), so the answer is not its last text; and a
 * turn that stops mid-work ends on a line like "Now let me compute…". Across
 * labelled texts from real turns, form told the two apart: answers carry
 * tables, headings, length and lists, while narration is short and opens on
 * a next step or ends on a colon. The weights and thresholds were fitted on one
 * labelled sample and held on a second, so changing one changes which texts a
 * folded turn shows; it is not a tidy-up.
 */

// Line patterns pad with spaces and tabs only: `\s` crosses newlines, which
// lets each line start rescan the blank lines after it.
/** A markdown table's delimiter row: the one line every table has. */
const TABLE_RULE = /^[ \t]*\|?[ \t]*:?-{3,}:?[ \t]*(?:\|[ \t]*:?-{3,}:?[ \t]*)+\|?[ \t]*$/m;
const HEADING = /^#{1,6}[ \t]/gm;
const LIST_ITEM = /^[ \t]*(?:[-*•]|\d+[.)])[ \t]+/gm;
/** Openings of a text announcing its next step. */
const NEXT_STEP_OPENING = /^\s*(?:let me|let's|i'll|i will|i'm going to|i am going to|now\b|next\b|first\b|checking|pulling|fetching|looking|running|好的|现在|接下来|下面|我来|我先|让我|正在|开始|继续|然后)/i;

/** Lengths, CJK counted double, at which a text reads as some or a lot of writing. */
const SOME_WRITING = 600;
const MUCH_WRITING = 1200;

/** An earlier text of a folded turn stays out at this score or above. */
export const ANSWER_SCORE_SHOWN = 2;
/** The last text of a turn that ended in work folds below this score. */
export const ANSWER_SCORE_HELD = 1;

/** Whether `pattern` matches at least `enough` times, stopping once it has. */
function atLeast(pattern: RegExp, text: string, enough: number): boolean {
  pattern.lastIndex = 0;
  let n = 0;
  while (n < enough && pattern.exec(text) !== null) n++;
  return n >= enough;
}

/** The text's length with each CJK character counted twice, since one carries
 *  about what two Latin ones do. Read only when the length alone is short of
 *  the cutoff it could still reach. */
function weightedLength(text: string): number {
  if (text.length >= MUCH_WRITING || text.length * 2 < SOME_WRITING) return text.length;
  let weight = text.length;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if (code >= 0x3400 && code <= 0x9fff) weight++;
  }
  return weight;
}

/** Higher reads more like an answer. */
export function answerScore(text: string): number {
  let score = 0;
  if (TABLE_RULE.test(text)) score += 3;
  if (atLeast(HEADING, text, 2)) score += 2;
  const weight = weightedLength(text);
  if (weight >= MUCH_WRITING) score += 2;
  else if (weight >= SOME_WRITING) score += 1;
  if (atLeast(LIST_ITEM, text, 3)) score += 1;
  if (NEXT_STEP_OPENING.test(text)) score -= 2;
  // A next step named at the end is no sign either way: answers often close
  // on one ("next I'll set an alert").
  const end = lastVisibleChar(text);
  if (end === ':' || end === '：') score -= 2;
  return score;
}

function lastVisibleChar(text: string): string {
  for (let i = text.length - 1; i >= 0; i--) {
    if (text[i].trim()) return text[i];
  }
  return '';
}
