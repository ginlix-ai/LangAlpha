import { useId, useMemo } from "react"
import { useTranslation } from "react-i18next"
import { Select } from "@/components/ui/select"
import type { ProviderModelsData } from "./types"
import type { ModelAccess } from "@/types/platform"
import type { ModelMetadataEntry } from "@/hooks/useFilteredModels"
import { modelLabel } from "@/lib/modelLabel"

const ACCESS_LABEL_KEYS: Record<ModelAccess, string> = {
  platform: "settings.modelAccess.platform",
  byok: "settings.modelAccess.byok",
  oauth: "settings.modelAccess.oauth",
  locked: "settings.modelAccess.locked",
}

export interface ModelSelectorProps {
  /** Label shown above the select */
  label: string
  /** Description shown below the label */
  description?: string
  /** Currently selected model name */
  value: string
  /** Callback when selection changes */
  onChange: (model: string) => void
  /** Available models grouped by provider */
  models: Record<string, ProviderModelsData>
  /** Optional: limit to specific providers (filter the models map) */
  filterProviders?: string[]
  /** Placeholder text when nothing selected */
  placeholder?: string
  /** An option beside the placeholder that names a choice rather than a model */
  autoOption?: { value: string; label: string }
  /** Required field indicator */
  required?: boolean
  /** Optional access map: model name → access type. When set, shows badge in option text. */
  modelAccess?: Record<string, ModelAccess>
  /** Optional model metadata: options print its display_name in place of the key. */
  metadata?: Record<string, ModelMetadataEntry>
}

export function ModelSelector({
  label,
  description,
  value,
  onChange,
  models,
  filterProviders,
  placeholder,
  autoOption,
  required = false,
  modelAccess,
  metadata,
}: ModelSelectorProps) {
  const { t } = useTranslation()
  const id = useId()

  const filteredModels = useMemo(() => {
    if (!filterProviders || filterProviders.length === 0) return models
    const result: Record<string, ProviderModelsData> = {}
    for (const provider of filterProviders) {
      if (models[provider]) {
        result[provider] = models[provider]
      }
    }
    return result
  }, [models, filterProviders])

  const hasModels = useMemo(
    () =>
      Object.values(filteredModels).some(
        (pd) => pd.models && pd.models.length > 0,
      ),
    [filteredModels],
  )

  return (
    <div className="flex flex-col gap-1.5">
      <label
        htmlFor={id}
        className="block font-medium"
        style={{
          fontSize: "0.875rem",
          fontWeight: 500,
          color: "var(--color-text-secondary)",
        }}
      >
        {label}
        {required && (
          <span
            className="ml-0.5"
            style={{ color: "var(--color-loss, #ef4444)" }}
            aria-hidden="true"
          >
            *
          </span>
        )}
      </label>

      {description && (
        <p
          className="mt-0"
          style={{
            fontSize: "0.75rem",
            fontWeight: 400,
            color: "var(--color-text-tertiary)",
            margin: 0,
          }}
        >
          {description}
        </p>
      )}

      {hasModels ? (
        <Select
          id={id}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          required={required}
        >
          <option value="">{placeholder ?? t("settings.selectModel")}</option>
          {autoOption && <option value={autoOption.value}>{autoOption.label}</option>}
          {Object.entries(filteredModels).map(([provider, providerData]) => {
            const modelList = providerData.models ?? []
            if (modelList.length === 0) return null
            const displayName =
              providerData.display_name ??
              provider.charAt(0).toUpperCase() + provider.slice(1)
            return (
              <optgroup key={provider} label={displayName}>
                {modelList.map((m) => {
                  const access = modelAccess?.[m]
                  const suffix = access ? ` (${t(ACCESS_LABEL_KEYS[access])})` : ""
                  return (
                    <option key={m} value={m}>
                      {modelLabel(m, metadata)}{suffix}
                    </option>
                  )
                })}
              </optgroup>
            )
          })}
        </Select>
      ) : (
        <p
          className="text-sm py-2"
          style={{ color: "var(--color-text-tertiary)" }}
        >
          {t("settings.noModelsAvailable")}
        </p>
      )}
    </div>
  )
}
