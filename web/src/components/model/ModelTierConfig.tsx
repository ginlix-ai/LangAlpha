import { useState } from "react"
import { useTranslation } from "react-i18next"
import { motion, AnimatePresence } from "@/lib/framer"
import { ChevronRight, Lightbulb } from "lucide-react"
import { cn } from "@/lib/utils"
import { TWEEN } from "@/lib/motion"
import { BACKGROUND_AUTO } from "@/lib/modelPreferences"
import { ModelSelector } from "./ModelSelector"
import { useModelSlotCopy } from "./useModelSlotCopy"
import type { ProviderModelsData } from "./types"
import type { Payer } from "@/lib/modelPayer"
import { isClaudeCodeModel } from "@/lib/claudeCode"
import type { ModelMetadataEntry } from "@/hooks/useFilteredModels"

export interface ModelTierConfigProps {
  /** Available models grouped by provider */
  models: Record<string, ProviderModelsData>
  /** The models the Background slot offers: a model that runs a conversation
   *  only (Claude Code) is not one. */
  backgroundModels: Record<string, ProviderModelsData>
  /** Limit models to these providers (configured in Step 1) */
  filterProviders?: string[]
  /** Current primary model selection */
  primaryModel: string
  onPrimaryModelChange: (model: string) => void
  /** Current Background model selection */
  backgroundModel: string
  onBackgroundModelChange: (model: string) => void
  /** Whether to show the "Two ways to research" explainer */
  showExplainer?: boolean
  /** Who pays for each model, shown beside its name */
  payers?: Record<string, Payer>
  /** Optional model metadata, for display names */
  metadata?: Record<string, ModelMetadataEntry>
  /** Settings, where both slots are account defaults and may stay unset: the
   *  models Auto runs, the deployment's primary and Background, each as printed and
   *  absent when the deployment names none. An unset primary is Auto; an unset
   *  Background follows the primary once one is saved, with Auto (`BACKGROUND_AUTO`)
   *  beside it, and is Auto before. Setup leaves this off: a first run fills
   *  both. */
  auto?: { primary?: string; background?: string }
}

// ---------------------------------------------------------------------------
// ModelTierConfig
// ---------------------------------------------------------------------------

export function ModelTierConfig({
  models,
  backgroundModels,
  filterProviders,
  primaryModel,
  onPrimaryModelChange,
  backgroundModel,
  onBackgroundModelChange,
  showExplainer = false,
  payers,
  metadata,
  auto,
}: ModelTierConfigProps) {
  const { t } = useTranslation()
  const [explainerOpen, setExplainerOpen] = useState(true)
  const copy = useModelSlotCopy()
  const autoLabel = (model: string | undefined) =>
    model ? t("settings.autoModel", { model }) : undefined
  const primaryAuto = autoLabel(auto?.primary)
  const backgroundAuto = autoLabel(auto?.background)
  // An unset Background follows a saved primary, except one that runs a
  // conversation only (Claude Code): then the deployment's Background model
  // runs, so the slot names it, and an Auto beside it would say the same.
  const followsPrimary = !!primaryModel && !isClaudeCodeModel(metadata?.[primaryModel])
  // A saved Auto keeps its row even when Background does not follow the
  // primary, so the slot still shows what is saved.
  const offerAuto = auto && backgroundAuto && (followsPrimary || backgroundModel === BACKGROUND_AUTO)

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
              {copy.explainerTitle}
            </span>
            <ChevronRight
              className={cn(
                "h-4 w-4 shrink-0 transition-transform duration-fold",
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
                transition={TWEEN.fold}
                className="overflow-hidden"
              >
                <div className="px-4 pb-4 flex flex-col gap-3">
                  <div className="flex flex-col gap-1">
                    <span
                      className="text-xs font-semibold"
                      style={{ color: "var(--color-text-primary)" }}
                    >
                      {copy.default.explainerTitle}
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      {copy.default.explainerBody}
                    </span>
                  </div>
                  <div className="flex flex-col gap-1">
                    <span
                      className="text-xs font-semibold"
                      style={{ color: "var(--color-text-primary)" }}
                    >
                      {copy.background.explainerTitle}
                    </span>
                    <span
                      className="text-xs leading-relaxed"
                      style={{ color: "var(--color-text-tertiary)" }}
                    >
                      {copy.background.explainerBody}
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
        label={copy.default.label}
        description={copy.default.description}
        value={primaryModel}
        onChange={onPrimaryModelChange}
        models={models}
        filterProviders={filterProviders}
        placeholder={primaryAuto ?? copy.default.placeholder}
        required={!primaryAuto}
        payers={payers}
        metadata={metadata}
      />

      {/* Background model selector */}
      <ModelSelector
        label={copy.background.label}
        description={copy.background.description}
        value={backgroundModel}
        onChange={onBackgroundModelChange}
        models={backgroundModels}
        filterProviders={filterProviders}
        placeholder={auto && followsPrimary
          ? copy.backgroundSameAsDefault
          : (auto && backgroundAuto) || copy.background.placeholder}
        autoOption={offerAuto
          ? { value: BACKGROUND_AUTO, label: backgroundAuto }
          : undefined}
        required={!auto}
        payers={payers}
        metadata={metadata}
      />

    </div>
  )
}
