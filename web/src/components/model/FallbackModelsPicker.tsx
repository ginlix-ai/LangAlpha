import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useTranslation } from "react-i18next"
import { Search, Pin } from "lucide-react"
import type { ProviderModelsData } from "./types"

// ---------------------------------------------------------------------------
// Fallback models picker — add/remove from accessible models
// ---------------------------------------------------------------------------

export function FallbackModelsPicker({
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
  const { t } = useTranslation()
  const [showAdd, setShowAdd] = useState(false)
  const [search, setSearch] = useState("")
  const containerRef = useRef<HTMLDivElement>(null)

  // Close on click outside
  useEffect(() => {
    if (!showAdd) return
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowAdd(false)
      }
    }
    document.addEventListener("mousedown", handler)
    return () => document.removeEventListener("mousedown", handler)
  }, [showAdd])

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
    <div ref={containerRef} className="flex flex-col gap-1.5">
      <label
        className="text-sm font-medium"
        style={{ color: "var(--color-text-primary)" }}
      >
        {t("settings.fallbackModels")}
      </label>
      <p
        className="text-xs leading-relaxed"
        style={{ color: "var(--color-text-tertiary)" }}
      >
        {t("settings.fallbackModelsDesc")}
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
              aria-label={t("settings.removeModel", { model: m })}
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
          + {t("settings.addModels")}
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
                placeholder={t("common.search")}
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
                  className="px-2 py-1 text-[0.625rem] font-semibold uppercase tracking-wider"
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
                {t("settings.fallbackModelsEmpty")}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

