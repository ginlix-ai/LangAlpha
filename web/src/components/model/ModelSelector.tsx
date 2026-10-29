import { useId, useMemo } from "react"
import { useTranslation } from "react-i18next"
import { Select } from "@/components/ui/select"
import type { ProviderModelsData } from "./types"
import type { ModelMetadataEntry } from "@/hooks/useFilteredModels"
import { modelLabel } from "@/lib/modelLabel"
import type { Payer } from "@/lib/modelPayer"
import { usePayerLabel } from "@/hooks/usePayerLabel"

export interface ModelSelectorProps {
  /** Label shown above the select. Absent, `ariaLabel` names it and the
   *  surrounding block's heading is the visible label. */
  label?: string
  ariaLabel?: string
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
  /** Who pays for each model, printed after its name: an option holds text only. */
  payers?: Record<string, Payer>
  /** Optional model metadata: options print its display_name in place of the key. */
  metadata?: Record<string, ModelMetadataEntry>
  /** Models listed but not pickable (above the plan), each in its provider's group. */
  disabledModels?: Record<string, ProviderModelsData>
  /** Names a saved value no group lists, which would otherwise show as the
   *  placeholder and hide that the setting points somewhere unusable. */
  missingLabel?: (value: string) => string
  /** How an option prints its model; the manifest's name, else the key. */
  nameOf?: (model: string) => string
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
  payers,
  metadata,
  ariaLabel,
  disabledModels,
  missingLabel,
  nameOf,
}: ModelSelectorProps) {
  const { t } = useTranslation()
  const payerLabel = usePayerLabel()
  const id = useId()
  const name = nameOf ?? ((m: string) => modelLabel(m, metadata))

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

  const groups = useMemo(() => {
    const keys = [...new Set([...Object.keys(filteredModels), ...Object.keys(disabledModels ?? {})])]
    return keys.map((provider) => ({
      provider,
      display:
        filteredModels[provider]?.display_name ??
        disabledModels?.[provider]?.display_name ??
        provider.charAt(0).toUpperCase() + provider.slice(1),
      models: filteredModels[provider]?.models ?? [],
      locked: (disabledModels?.[provider]?.models ?? []).filter(
        (m) => !(filteredModels[provider]?.models ?? []).includes(m),
      ),
    }))
  }, [filteredModels, disabledModels])

  const listed =
    !value ||
    value === autoOption?.value ||
    groups.some((g) => g.models.includes(value) || g.locked.includes(value))

  return (
    <div className="flex flex-col gap-1.5">
      {label && <label
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
      </label>}

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
          aria-label={label ? undefined : ariaLabel}
        >
          <option value="">{placeholder ?? t("settings.selectModel")}</option>
          {autoOption && <option value={autoOption.value}>{autoOption.label}</option>}
          {!listed && missingLabel && <option value={value}>{missingLabel(value)}</option>}
          {groups.map(({ provider, display, models: modelList, locked }) => {
            if (modelList.length === 0 && locked.length === 0) return null
            return (
              <optgroup key={provider} label={display}>
                {modelList.map((m) => {
                  const payer = payers?.[m]
                  const suffix = payer ? ` · ${payerLabel(payer)}` : ""
                  return (
                    <option key={m} value={m}>
                      {name(m)}{suffix}
                    </option>
                  )
                })}
                {locked.map((m) => (
                  <option key={m} value={m} disabled>
                    {name(m)} · {t("payer.locked")}
                  </option>
                ))}
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
