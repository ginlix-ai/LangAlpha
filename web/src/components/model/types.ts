/** Shared BYOK (Bring Your Own Key) and model types */

export type AccessType = 'api_key' | 'coding_plan' | 'oauth' | 'local';

export interface ByokProvider {
  provider: string;
  display_name: string;
  access_type?: AccessType;
  brand_key?: string;
  parent_provider?: string;
  has_key: boolean;
  masked_key: string | null;
  base_url: string | null;
  is_custom?: boolean;
  use_response_api?: boolean;
  /** Stored for a provider the catalog no longer has: listed only to be removed. */
  withdrawn?: boolean;
}

export type SdkType = 'openai' | 'anthropic' | 'gemini' | 'codex' | 'deepseek' | 'openrouter' | 'qwq' | 'dashscope';

export interface RegionVariant {
  provider: string;
  display_name: string;
  region: string;
  sdk?: SdkType | string;
  base_url?: string | null;
  use_response_api?: boolean;
  /** Whose manifest models a key saved here offers: the region's plan. */
  models_from?: string;
}

export interface ProviderCatalogEntry {
  provider: string;
  display_name: string;
  access_type: AccessType;
  brand_key: string;
  byok_eligible: boolean;
  region?: string;
  sdk?: SdkType | string;
  base_url?: string | null;
  use_response_api?: boolean;
  dynamic_models?: boolean;
  /** Whose manifest models a key saved here offers: the provider's own, else
   *  a coding plan's brand. */
  models_from?: string;
  region_variants?: RegionVariant[];
  /** The connection's other APIs (OpenCode Go's Chat Completions beside its
   *  Anthropic Messages): a model it serves on one is saved under that one,
   *  and stays this connection's. */
  routes?: string[];
  /** The provider this entry is a plan or region of (the providers.json group). */
  group?: string;
  group_display_name?: string;
  /** What kind of provider the group is; the key picker sorts by it. */
  category?: ProviderCategory | null;
}

export type ProviderCategory = 'closed_source' | 'open_source' | 'inference';

/** Where the server looks for a provider's stored key, from `/api/v1/models`. */
export interface KeyCandidates {
  /** Every slug the lookup visits, in its order. */
  slugs: string[];
  /** Those whose key the vendor issues as one with the provider's. */
  shared: string[];
}

/**
 * Which reasoning levels a model takes, and where the chosen one is written
 * (`reasoning_block` on the server). An empty `efforts` says it takes none.
 */
export interface ReasoningDeclaration {
  efforts: string[];
  default?: string;
  write?: string;
}

export interface CustomModelEntry {
  name: string;
  model_id: string;
  provider: string;
  parameters?: Record<string, unknown>;
  extra_body?: Record<string, unknown>;
  input_modalities?: string[];
  /** Context window in tokens, as the provider lists it; picks the compaction band. */
  context?: number;
  reasoning?: ReasoningDeclaration;
  /** The prompt scaffolding the model is tuned for, where it is known. */
  prompt_guidance?: string;
  /** The name a gateway's catalog gives the model, shown in place of `name`. */
  display_name?: string;
}

export interface CustomModelFormState {
  name: string;
  model_id: string;
  provider: string;
  parameters: string;
  extra_body: string;
  input_modalities?: string[];
  _customProvider?: boolean;
}

export interface AddProviderFormState {
  name: string;
  parent_provider: string;
  api_key?: string;
  base_url?: string;
  use_response_api?: boolean;
}

export interface ProviderModelsData {
  models?: string[];
  display_name?: string;
}
