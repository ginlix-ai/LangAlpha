/**
 * Load captured SSE event fixtures and prepare them for replay.
 *
 * The backend replay endpoint adds turn_index and response_id to every event.
 * This helper does the same so the frontend history processor works correctly.
 */
import { readFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturesDir = join(__dirname, '..', 'fixtures');

/**
 * Load a fixture JSON file and enrich each event with replay metadata.
 * @param {string} filename - Fixture filename (e.g., 'turn0_steer.json')
 * @param {number} turnIndex - The turn_index to inject
 * @param {string} [threadId='th-1'] - Thread ID override
 * @returns {Array<Object>} Enriched SSE events ready for configureSSE
 */
export function loadFixture(filename, turnIndex, threadId = 'th-1') {
  const raw = JSON.parse(readFileSync(join(fixturesDir, filename), 'utf8'));
  return raw.map((event) => ({
    ...event,
    data: {
      ...event.data,
      turn_index: turnIndex,
      response_id: `resp-${turnIndex}`,
      thread_id: threadId,
    },
  }));
}
