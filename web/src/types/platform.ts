/** Types for the platform access layer (model entitlement and provider routing). */

/** Response shape from GET /api/auth/models on the platform service. */
export interface PlatformModelsResponse {
  /** Numeric tier representing the user's model-access level. */
  model_tier: number;
  /** Provider slugs where user has their own API key configured in platform. */
  byok_providers: string[];
  /** Provider slugs where user has OAuth-connected access. */
  oauth_providers: string[];
}

/**
 * Resolved access status for a single model.
 *
 * - 'platform' — accessible via the platform subscription tier
 * - 'byok'     — accessible via a user-provided API key
 * - 'oauth'    — accessible via an OAuth-connected provider
 * - 'locked'   — not accessible under the current tier/keys
 */
export type ModelAccess = 'platform' | 'byok' | 'oauth' | 'locked';
