import { useState } from "react"
import { useTranslation } from "react-i18next"
import { motion, AnimatePresence } from "@/lib/framer"
import { ChevronRight, Lightbulb } from "lucide-react"
import { cn } from "@/lib/utils"
import { FLASH_AUTO } from "@/lib/modelPreferences"
import { ModelSelector } from "./ModelSelector"
import type { ProviderModelsData } from "./types"
import type { ModelAccess } from "@/types/platform"
import type { ModelMetadataEntry } from "@/hooks/useFilteredModels"

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
  /** Optional access map: model name → access type for badge display */
  modelAccess?: Record<string, ModelAccess>
  /** Optional model metadata, for display names */
  metadata?: Record<string, ModelMetadataEntry>
  /** Settings, where both slots are account defaults and may stay unset: the
   *  models Auto runs, the deployment's primary and flash, each as printed and
   *  absent when the deployment names none. An unset primary is Auto; an unset
   *  flash follows the primary once one is saved, with Auto (`FLASH_AUTO`)
   *  beside it, and is Auto before. Setup leaves this off: a first run fills
   *  both. */
  auto?: { primary?: string; flash?: string }
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
  modelAccess,
  metadata,
  auto,
}: ModelTierConfigProps) {
  const { t } = useTranslation()
  const [explainerOpen, setExplainerOpen] = useState(true)
  const autoLabel = (model: string | undefined) =>
    model ? t("settings.autoModel", { model }) : undefined
  const primaryAuto = autoLabel(auto?.primary)
  const flashAuto = autoLabel(auto?.flash)

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
              {t("setup.explainerTitle")}
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
                      {t("setup.explainerDeepTitle")}
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      {t("setup.explainerDeepBody")}
                    </span>
                  </div>
                  <div className="flex flex-col gap-1">
                    <span
                      className="text-xs font-semibold"
                      style={{ color: "var(--color-text-primary)" }}
                    >
                      {t("setup.explainerFlashTitle")}
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      {t("setup.explainerFlashBody")}
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
        label={t("setup.primaryModel")}
        description={t("setup.primaryDescription")}
        value={primaryModel}
        onChange={onPrimaryModelChange}
        models={models}
        filterProviders={filterProviders}
        placeholder={primaryAuto ?? t("setup.primaryPlaceholder")}
        required={!primaryAuto}
        modelAccess={modelAccess}
        metadata={metadata}
      />

      {/* Flash Model selector */}
      <ModelSelector
        label={t("setup.flashModel")}
        description={t("setup.flashDescription")}
        value={flashModel}
        onChange={onFlashModelChange}
        models={models}
        filterProviders={filterProviders}
        placeholder={auto && primaryModel
          ? t("setup.flashSameAsPrimary")
          : (auto && flashAuto) || t("setup.flashPlaceholder")}
        autoOption={auto && primaryModel && flashAuto
          ? { value: FLASH_AUTO, label: flashAuto }
          : undefined}
        required={!auto}
        modelAccess={modelAccess}
        metadata={metadata}
      />

    </div>
  )
}
