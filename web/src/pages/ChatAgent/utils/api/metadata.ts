/**
 * Skills + model-metadata caches (lazy single-flight; reset on auth change).
 */
import { api } from '@/api/client';
import { registerAuthReset } from '@/lib/authResets';

const _skillsPromises: Record<string, Promise<unknown[]>> = {};  // module-level cache keyed by mode

export async function getSkills(mode: string | null = null) {
  const key = mode || '_all';
  if (key in _skillsPromises) return _skillsPromises[key];
  _skillsPromises[key] = api.get('/api/v1/skills', { params: mode ? { mode } : {} })
    .then(({ data }) => data.skills || [])
    .catch(() => { delete _skillsPromises[key]; return []; });
  return _skillsPromises[key];
}

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
 * Reset the module-level API caches (skills, model metadata). Module
 * singletons outlive React, so this runs on sign-out and account switch via
 * the authResets registry — otherwise one user's skills/models leak into the
 * next session on a shared tab.
 */
export function resetChatApiCaches() {
  for (const key of Object.keys(_skillsPromises)) delete _skillsPromises[key];
  _modelMetadataPromise = null;
}

registerAuthReset(resetChatApiCaches);

// --- File Upload ---

// --- Feedback ---
