import { useState, type Dispatch, type SetStateAction } from 'react';

/**
 * A model choice that starts on `seed`, holds a pick made since, and follows
 * the seed again whenever the seed itself moves.
 *
 * The seed moving is the outside world changing its answer (the thread's saved
 * model arriving or changing, a model falling out of reach, the account
 * default changing), and that answer is newer than any pick held here. A pick
 * saved where the seed comes from moves the seed to the pick itself, so
 * following it never undoes one. Adjusted during render, so the value never
 * commits a frame behind its seed.
 */
export function useSeededModel(
  seed: string | null,
): [string | null, Dispatch<SetStateAction<string | null>>] {
  const [model, setModel] = useState<string | null>(seed);
  const [followedSeed, setFollowedSeed] = useState(seed);
  if (followedSeed !== seed) {
    setFollowedSeed(seed);
    setModel(seed);
  }
  return [model, setModel];
}
