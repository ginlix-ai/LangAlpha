/**
 * Model-metadata cache (lazy single-flight; reset on auth change). Skills
 * moved to React Query (`useSkills`) — a module-level promise cache can't
 * invalidate when the user uploads or disables a skill.
 */
import { api } from '@/api/client';
import { registerAuthReset } from '@/lib/authResets';

// --- Model Metadata (eager prefetch at import time — resolved before ChatInput mounts) ---

let _modelMetadataPromise: Promise<Record<string, unknown>> | null = null;

function fetchModelMetadata(): Promise<Record<string, unknown>> {
  const promise: Promise<Record<string, unknown>> = api.get('/api/v1/models')
    .then(({ data }) => data.model_metadata || {})
    .catch(() => {
      // Failures are not cached: clear the slot so the next call retries.
      if (_modelMetadataPromise === promise) _modelMetadataPromise = null;
      return {};
    });
  return promise;
}

export function getModelMetadata() {
  if (!_modelMetadataPromise) _modelMetadataPromise = fetchModelMetadata();
  return _modelMetadataPromise;
}

/**
 * Reset the module-level model-metadata cache. Module singletons outlive
 * React, so this runs on sign-out and account switch via the authResets
 * registry — otherwise one user's models leak into the next session on a
 * shared tab.
 */
export function resetChatApiCaches() {
  _modelMetadataPromise = null;
}

registerAuthReset(resetChatApiCaches);

// --- File Upload ---

// --- Feedback ---
