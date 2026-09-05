import { useMemo } from "react"
import { useTranslation } from "react-i18next"
import { cn } from "@/lib/utils"
import { COMPACTION_PROFILE_ORDER } from "@/lib/modelTuning"
import type {
  CompactionProfileCatalog,
  CompactionProfileName,
} from "@/hooks/useAllModels"

/**
 * The account-wide compaction preset, as cards carrying their real numbers.
 *
 * Bundles token_threshold, keep_messages and truncate_args_trigger_messages —
 * the numbers are the only way to tell the presets apart, so they are printed
 * rather than described.
 */
export function CompactionProfilePicker({
  value,
  onChange,
  profiles,
}: {
  value: CompactionProfileName | ""
  onChange: (v: CompactionProfileName | "") => void
  profiles: CompactionProfileCatalog | null | undefined
}) {
  const { t } = useTranslation()
  // Named for the column it heads in the per-model table below it. One screen
  // must not call one setting two things.
  const heading = t("settings.modelTuning.colContext")
  const available = useMemo(
    () => COMPACTION_PROFILE_ORDER.filter((name) => profiles?.[name]),
    [profiles],
  )

  if (!profiles || available.length === 0) return null

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-col gap-1">
        <label
          className="text-sm font-medium"
          style={{ color: "var(--color-text-primary)" }}
        >
          {heading}
        </label>
        <p className="text-xs" style={{ color: "var(--color-text-tertiary)" }}>
          {t("settings.compactionProfileDesc")}
        </p>
      </div>

      <div
        role="radiogroup"
        aria-label={heading}
        className="grid gap-2"
        style={{ gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))" }}
      >
        {available.map((name) => {
          const preset = profiles[name]
          const selected = value === name
          return (
            <button
              key={name}
              type="button"
              role="radio"
              aria-checked={selected}
              onClick={() => onChange(selected ? "" : name)}
              className={cn(
                "flex flex-col items-start rounded-md p-3 text-left transition-colors",
                "hover:border-[var(--color-border-elevated)]",
              )}
              style={{
                background: selected
                  ? "var(--color-bg-tag)"
                  : "var(--color-bg-surface)",
                border: `1px solid ${selected ? "var(--color-text-primary)" : "var(--color-border-default)"}`,
                color: "var(--color-text-primary)",
              }}
            >
              <span className="text-sm font-medium">
                {t(`settings.compactionProfiles.${name}.label`)}
              </span>
              <span
                className="mt-0.5 text-[0.6875rem]"
                style={{ color: "var(--color-text-tertiary)" }}
              >
                {t(`settings.compactionProfiles.${name}.description`)}
              </span>
              <span
                className="mt-2 text-[0.6875rem] tabular-nums"
                style={{ color: "var(--color-text-secondary, var(--color-text-tertiary))" }}
              >
                {t("settings.compactionProfilePreset", {
                  tokens: Math.round(preset.token_threshold / 1000),
                  keep: preset.keep_messages,
                  trim: preset.truncate_args_trigger_messages,
                })}
              </span>
            </button>
          )
        })}
      </div>

      {value !== "" && (
        <button
          type="button"
          onClick={() => onChange("")}
          className="self-start text-xs underline-offset-2 hover:underline"
          style={{ color: "var(--color-text-tertiary)" }}
        >
          {t("settings.compactionProfileUseDefault")}
        </button>
      )}
    </div>
  )
}
