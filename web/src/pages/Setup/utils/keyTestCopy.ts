import type { useTranslation } from 'react-i18next';

type Translate = ReturnType<typeof useTranslation>['t'];

/**
 * The setup key test's verdict in the reader's language. The server words it
 * as an English sentence and sends no code, so the sentences are matched here
 * as it writes them (`setup_settings/key_test.py`), the one place that knows
 * them: one reworded there, or not met yet, shows in the server's own words.
 */
const VERDICTS: Readonly<Record<string, string>> = {
  'The key was refused': 'setup:key.testRefused',
  "The key's plan has run out or does not include this": 'setup:key.testPlan',
  'Rate limited, so the key could not be checked yet': 'setup:key.testRateLimited',
  'No answer within 10 seconds': 'setup:key.testTimeout',
  'The vendor could not be reached': 'setup:key.testUnreachable',
  'This is not a usable key': 'setup:key.testUnusable',
  'This key has no test': 'setup:key.testNone',
};

export function keyTestErrorCopy(error: string, t: Translate): string {
  if (Object.hasOwn(VERDICTS, error)) return t(VERDICTS[error]);
  const status = /status (\d+)$/.exec(error);
  return status ? t('setup:key.testStatus', { status: status[1] }) : error;
}
