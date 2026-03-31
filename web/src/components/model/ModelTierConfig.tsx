import { useCallback, useEffect, useMemo, useState } from "react"
import { motion, AnimatePresence } from "framer-motion"
import { ChevronRight, Lightbulb, Search, Pin } from "lucide-react"
import { cn } from "@/lib/utils"
import { ModelSelector } from "./ModelSelector"
import type { ProviderModelsData } from "./types"

export interface ModelTierConfigProps {
  /** Available models grouped by provider */
  models: Record<string, ProviderModelsData>
  /** Limit models to these providers (configured in Step 1) */
  filterProviders?: string[]
  /** Current primary model selection */
  primaryModel: string
  onPrimaryModelChange: (model: string) => void
  /** Current flash model selection */
  flashModel: string
  onFlashModelChange: (model: string) => void
  /** Whether to show the "Two ways to research" explainer */
  showExplainer?: boolean
  /** Whether to show the Advanced section (summarization, fetch, fallback) */
  showAdvanced?: boolean
  /** Advanced model selections (optional, for Settings page) */
  advancedModels?: {
    summarizationModel?: string
    fetchModel?: string
    fallbackModels?: string[]
  }
  onAdvancedModelsChange?: (models: {
    summarizationModel?: string
    fetchModel?: string
    fallbackModels?: string[]
  }) => void
  /** System defaults for fallback */
  systemDefaults?: {
    default_model?: string
    flash_model?: string
    summarization_model?: string
    fetch_model?: string
    fallback_models?: string[]
  }
}

// ---------------------------------------------------------------------------
// Fallback models picker — add/remove from accessible models
// ---------------------------------------------------------------------------

function FallbackModelsPicker({
  selected,
  onChange,
  models,
  filterProviders,
}: {
  selected: string[]
  onChange: (models: string[]) => void
  models: Record<string, ProviderModelsData>
  filterProviders?: string[]
}) {
  const [showAdd, setShowAdd] = useState(false)
  const [search, setSearch] = useState("")

  const selectedSet = useMemo(() => new Set(selected), [selected])

  const handleToggle = useCallback(
    (model: string) => {
      if (selectedSet.has(model)) {
        onChange(selected.filter((m) => m !== model))
      } else {
        onChange([...selected, model])
      }
    },
    [selected, selectedSet, onChange],
  )

  const handleRemove = useCallback(
    (model: string) => onChange(selected.filter((m) => m !== model)),
    [selected, onChange],
  )

  // Filter providers for the grouped dropdown
  const filteredGroups = useMemo(() => {
    const query = search.toLowerCase()
    const groups: { provider: string; displayName: string; models: string[] }[] = []
    for (const [provider, pd] of Object.entries(models)) {
      if (filterProviders && !filterProviders.includes(provider)) continue
      const provModels = pd.models ?? []
      const filtered = query
        ? provModels.filter((m) => m.toLowerCase().includes(query))
        : provModels
      if (filtered.length > 0) {
        groups.push({
          provider,
          displayName: pd.display_name ?? provider.charAt(0).toUpperCase() + provider.slice(1),
          models: filtered,
        })
      }
    }
    return groups
  }, [models, filterProviders, search])

  return (
    <div className="flex flex-col gap-1.5">
      <label
        className="text-sm font-medium"
        style={{ color: "var(--color-text-primary)" }}
      >
        Fallback Models
      </label>
      <p
        className="text-xs"
        style={{ color: "var(--color-text-tertiary)" }}
      >
        Used when the primary or flash model is unavailable
      </p>

      {/* Selected chips with remove */}
      <div className="flex flex-wrap gap-1.5 mt-1">
        {selected.map((m) => (
          <span
            key={m}
            className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs"
            style={{
              background: "var(--color-bg-surface)",
              border: "1px solid var(--color-border-default)",
              color: "var(--color-text-secondary)",
            }}
          >
            {m}
            <button
              type="button"
              onClick={() => handleRemove(m)}
              className="ml-0.5 hover:opacity-70"
              style={{ color: "var(--color-text-tertiary)" }}
              aria-label={`Remove ${m}`}
            >
              &times;
            </button>
          </span>
        ))}

        {/* Add button */}
        <button
          type="button"
          onClick={() => { setShowAdd((v) => !v); setSearch("") }}
          className="inline-flex items-center px-2 py-1 rounded text-xs font-medium"
          style={{
            border: "1px dashed var(--color-border-default)",
            color: "var(--color-accent-primary)",
          }}
        >
          + Add
        </button>
      </div>

      {/* Grouped searchable picker */}
      {showAdd && (
        <div
          className="rounded-lg mt-1 overflow-hidden"
          style={{
            border: "1px solid var(--color-border-muted)",
            background: "var(--color-bg-card)",
          }}
        >
          {/* Search */}
          <div className="px-3 pt-3 pb-2">
            <div className="relative">
              <Search
                className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5"
                style={{ color: "var(--color-text-tertiary)" }}
              />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search"
                className="w-full rounded-md pl-8 pr-3 py-1.5 text-xs"
                style={{
                  backgroundColor: "var(--color-bg-elevated)",
                  border: "1px solid var(--color-border-muted)",
                  color: "var(--color-text-primary)",
                }}
                autoFocus
              />
            </div>
          </div>
          {/* Provider groups */}
          <div className="px-1 pb-1 max-h-[280px] overflow-y-auto">
            {filteredGroups.map(({ provider, displayName, models: groupModels }) => (
              <div key={provider} className="mb-1">
                <div
                  className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider"
                  style={{ color: "var(--color-text-tertiary)" }}
                >
                  {displayName}
                </div>
                {groupModels.map((m) => {
                  const isSelected = selectedSet.has(m)
                  return (
                    <button
                      key={m}
                      type="button"
                      onClick={() => handleToggle(m)}
                      className="w-full flex items-center justify-between px-2 py-1.5 rounded-md text-xs transition-colors"
                      style={{
                        color: isSelected
                          ? "var(--color-accent-light)"
                          : "var(--color-text-primary)",
                        backgroundColor: isSelected
                          ? "var(--color-accent-soft)"
                          : "transparent",
                      }}
                      onMouseEnter={(e) => {
                        if (!isSelected) e.currentTarget.style.backgroundColor = "var(--color-bg-elevated)"
                      }}
                      onMouseLeave={(e) => {
                        if (!isSelected) e.currentTarget.style.backgroundColor = "transparent"
                      }}
                    >
                      <span>{m}</span>
                      {isSelected && (
                        <Pin
                          className="h-3 w-3 flex-shrink-0"
                          style={{ color: "var(--color-accent-primary)" }}
                        />
                      )}
                    </button>
                  )
                })}
              </div>
            ))}
            {filteredGroups.length === 0 && (
              <p className="px-3 py-2 text-xs" style={{ color: "var(--color-text-tertiary)" }}>
                No models found
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// ModelTierConfig
// ---------------------------------------------------------------------------

export function ModelTierConfig({
  models,
  filterProviders,
  primaryModel,
  onPrimaryModelChange,
  flashModel,
  onFlashModelChange,
  showExplainer = false,
  showAdvanced = false,
  advancedModels,
  onAdvancedModelsChange,
  systemDefaults,
}: ModelTierConfigProps) {
  const [explainerOpen, setExplainerOpen] = useState(true)
  const [advancedOpen, setAdvancedOpen] = useState(false)

  // Auto-default flash model: pick the first model from the same provider as
  // the primary model, or the first model in the list if provider doesn't match.
  const autoDefaultFlashModel = useMemo(() => {
    if (flashModel) return undefined // Already set, no auto-default needed

    // Find which provider the primary model belongs to
    let primaryProvider: string | null = null
    for (const [provider, pd] of Object.entries(models)) {
      if (filterProviders && !filterProviders.includes(provider)) continue
      if (pd.models?.includes(primaryModel)) {
        primaryProvider = provider
        break
      }
    }

    // Try same provider first
    if (primaryProvider) {
      const providerModels = models[primaryProvider]?.models ?? []
      // Pick first model that isn't the primary (or just the first one)
      const candidate =
        providerModels.find((m) => m !== primaryModel) ?? providerModels[0]
      if (candidate) return candidate
    }

    // Fallback: first model from any available provider
    const entries = Object.entries(models)
    for (const [provider, pd] of entries) {
      if (filterProviders && !filterProviders.includes(provider)) continue
      if (pd.models && pd.models.length > 0) return pd.models[0]
    }
    return undefined
  }, [flashModel, primaryModel, models, filterProviders])

  // When primary model changes, if flash is empty, suggest auto-default
  const handlePrimaryChange = useCallback(
    (model: string) => {
      onPrimaryModelChange(model)
      // If flash model is empty and we can auto-default, do so
      if (!flashModel && autoDefaultFlashModel) {
        // We'll let the parent decide via the auto-default mechanism
      }
    },
    [onPrimaryModelChange, flashModel, autoDefaultFlashModel],
  )

  // When flash model is empty but we have an auto-default, apply it
  // on first render or when conditions change
  useEffect(() => {
    if (!flashModel && autoDefaultFlashModel && primaryModel) {
      onFlashModelChange(autoDefaultFlashModel)
    }
  }, [autoDefaultFlashModel, primaryModel, flashModel, onFlashModelChange])

  const handleAdvancedChange = useCallback(
    (field: string, value: string | string[]) => {
      if (!onAdvancedModelsChange || !advancedModels) return
      onAdvancedModelsChange({
        ...advancedModels,
        [field]: value,
      })
    },
    [onAdvancedModelsChange, advancedModels],
  )

  return (
    <div className="flex flex-col" style={{ gap: "24px" }}>
      {/* "Two ways to research" explainer */}
      {showExplainer && (
        <div
          className="rounded-lg overflow-hidden"
          style={{
            background: "var(--color-bg-surface)",
            border: "1px solid var(--color-border-default)",
          }}
        >
          <button
            type="button"
            onClick={() => setExplainerOpen((v) => !v)}
            className={cn(
              "flex w-full items-center gap-2 text-left transition-colors",
              "px-4 py-3",
            )}
            style={{ color: "var(--color-text-primary)" }}
            aria-expanded={explainerOpen}
          >
            <Lightbulb
              className="h-4 w-4 shrink-0"
              style={{ color: "var(--color-accent-primary)" }}
            />
            <span className="text-sm font-medium flex-1">
              Two agent modes
            </span>
            <ChevronRight
              className={cn(
                "h-4 w-4 shrink-0 transition-transform duration-200",
                explainerOpen && "rotate-90",
              )}
              style={{ color: "var(--color-text-tertiary)" }}
            />
          </button>

          <AnimatePresence initial={false}>
            {explainerOpen && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: "auto", opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.2, ease: "easeInOut" }}
                className="overflow-hidden"
              >
                <div className="px-4 pb-4 flex flex-col gap-3">
                  <div className="flex flex-col gap-1">
                    <span
                      className="text-xs font-semibold"
                      style={{ color: "var(--color-text-primary)" }}
                    >
                      PTC Mode (Primary)
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      The full agent with a sandboxed environment for code
                      execution, charts, and data analysis. Powered by your
                      primary model.
                    </span>
                  </div>
                  <div className="flex flex-col gap-1">
                    <span
                      className="text-xs font-semibold"
                      style={{ color: "var(--color-text-primary)" }}
                    >
                      Flash Mode
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      A lightweight agent for quick answers without a sandbox.
                      Powered by your flash model for speed.
                    </span>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      )}

      {/* Primary Model selector */}
      <ModelSelector
        label="Primary Model"
        description="For deep research with code execution and data analysis"
        value={primaryModel}
        onChange={handlePrimaryChange}
        models={models}
        filterProviders={filterProviders}
        placeholder="Select primary model..."
        required
      />

      {/* Flash Model selector */}
      <ModelSelector
        label="Flash Model"
        description="For quick answers without a sandbox"
        value={flashModel}
        onChange={onFlashModelChange}
        models={models}
        filterProviders={filterProviders}
        placeholder="Select flash model..."
        required
      />

      {/* Advanced section */}
      {showAdvanced && advancedModels && onAdvancedModelsChange && (
        <div>
          <button
            type="button"
            onClick={() => setAdvancedOpen((v) => !v)}
            className="inline-flex items-center gap-1 text-xs font-medium transition-colors"
            style={{ color: "var(--color-text-tertiary)" }}
            aria-expanded={advancedOpen}
          >
            <ChevronRight
              className={cn(
                "h-3 w-3 transition-transform duration-200",
                advancedOpen && "rotate-90",
              )}
            />
            Advanced
          </button>

          <AnimatePresence initial={false}>
            {advancedOpen && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: "auto", opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.2, ease: "easeInOut" }}
                className="overflow-hidden"
              >
                <div
                  className="flex flex-col pt-4"
                  style={{ gap: "24px" }}
                >
                  <ModelSelector
                    label="Summarization Model"
                    description="Used for summarizing long content"
                    value={advancedModels.summarizationModel ?? ""}
                    onChange={(v) => handleAdvancedChange("summarizationModel", v)}
                    models={models}
                    filterProviders={filterProviders}
                    placeholder="Defaults to flash model"
                  />

                  <ModelSelector
                    label="Web Fetch Model"
                    description="Used for web page extraction"
                    value={advancedModels.fetchModel ?? ""}
                    onChange={(v) => handleAdvancedChange("fetchModel", v)}
                    models={models}
                    filterProviders={filterProviders}
                    placeholder="Defaults to flash model"
                  />

                  {/* Fallback models (editable) */}
                  <FallbackModelsPicker
                    selected={advancedModels.fallbackModels ?? []}
                    onChange={(list) => handleAdvancedChange("fallbackModels", list)}
                    models={models}
                    filterProviders={filterProviders}
                  />
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      )}
    </div>
  )
}
