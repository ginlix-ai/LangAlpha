/**
 * Centralized file reference normalization.
 *
 * AI agents are inconsistent in how they format file references in markdown.
 * This function canonicalizes all known variants into clean relative paths
 * BEFORE markdown parsing, so downstream components only need to handle:
 *
 *   - Same-workspace relative:   results/report.md
 *   - Cross-workspace qualified: __wsref__/{uuid}/results/report.md
 *
 * Runs once per message render as a content-level pre-processing step.
 */

/**
 * Step 1: Unwrap backtick-wrapped markdown links.
 *
 * Agents sometimes wrap file links in backticks, making them render as code
 * spans instead of clickable links:
 *   `[report.md](results/report.md)` → [report.md](results/report.md)
 *   `![chart](charts/fig.png)`       → ![chart](charts/fig.png)
 *
 * Only unwraps when the ENTIRE code span is a single markdown link.
 * Does not touch multi-backtick code blocks or inline code with other content.
 */
const BACKTICK_LINK_RE = /`(!?\[[^\]]*\]\([^)]+\))`/g;

/**
 * Step 2: Strip file:///home/(workspace|daytona)/ from markdown link hrefs.
 *
 * rehype-sanitize strips non-whitelisted protocols like file://,
 * so we normalize to a relative path before parsing.
 *   [report.md](file:///home/workspace/results/report.md) → [report.md](results/report.md)
 */
const FILE_PROTO_RE = /(!?\[[^\]]*\]\()file:\/\/\/home\/(?:workspace|daytona)\//g;

/**
 * Step 3: Strip bare /home/(workspace|daytona)/ absolute paths from hrefs.
 *
 * Handles agents that use absolute sandbox paths without the file:// protocol:
 *   [report.md](/home/workspace/results/report.md) → [report.md](results/report.md)
 */
const ABS_SANDBOX_RE = /(!?\[[^\]]*\]\()\/home\/(?:workspace|daytona)\//g;

/**
 * Step 4: Clean stale prefixes inside __wsref__ paths.
 *
 * Legacy stored messages may contain artifacts from before the backend fix:
 *   __wsref__/uuid/file:///home/workspace/x → __wsref__/uuid/x
 *   __wsref__/uuid//home/workspace/x        → __wsref__/uuid/x
 */
const WSREF_INNER_RE = /(__wsref__\/[0-9a-f-]+\/)(?:file:\/\/)?\/home\/(?:workspace|daytona)\//g;

/**
 * Normalize all file references in a markdown string.
 *
 * Run this ONCE before markdown parsing. After normalization, all file hrefs
 * are either clean relative paths or __wsref__/{uuid}/relative paths.
 */
export function normalizeFileRefs(content: string): string {
  if (!content || typeof content !== 'string') return content;

  content = content.replace(BACKTICK_LINK_RE, '$1');     // step 1
  content = content.replace(FILE_PROTO_RE, '$1');         // step 2
  content = content.replace(ABS_SANDBOX_RE, '$1');        // step 3
  content = content.replace(WSREF_INNER_RE, '$1');        // step 4

  return content;
}
