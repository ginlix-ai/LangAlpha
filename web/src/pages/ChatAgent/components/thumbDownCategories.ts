// The value is what the rating stores and the team's report lists, so it stays
// English and stable; the label is the user's language.
export const ISSUE_CATEGORIES: ReadonlyArray<{ value: string; key: string }> = [
  { value: 'Incorrect data or numbers', key: 'incorrectData' },
  { value: 'Wrong ticker or company', key: 'wrongTicker' },
  { value: 'Outdated information', key: 'outdated' },
  { value: 'Flawed analysis', key: 'flawedAnalysis' },
  { value: 'Missing risk factors', key: 'missingRisks' },
  { value: 'Code execution errors', key: 'codeErrors' },
  { value: "Didn't follow instructions", key: 'ignoredInstructions' },
  { value: 'Incomplete research', key: 'incomplete' },
  { value: 'Too slow', key: 'tooSlow' },
];
