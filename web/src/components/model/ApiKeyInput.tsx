import { useState, useCallback, useId } from "react"
import { Eye, EyeOff, Check, X } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Loader } from "@/components/ui/loader"
import { useTranslation } from "react-i18next"
import { useLocale } from "@/hooks/useLocale"
import { createFormatter } from "@/lib/format"
import type { TestResult } from "@/api/model"

const usd = createFormatter({ style: "currency", currency: "USD", maximumFractionDigits: 2 })

export interface ApiKeyInputProps {
  provider: string
  value: string
  onChange: (value: string) => void
  maskedKey?: string | null
  baseUrl?: string
  onBaseUrlChange?: (value: string) => void
  showBaseUrl?: boolean
  onTest?: (
    provider: string,
    apiKey: string,
  ) => Promise<TestResult>
  testDisabled?: boolean
  /** A key the endpoint may do without (a local server): blank is no error. */
  optional?: boolean
  /** For the key field, so a visible label outside can name it. */
  id?: string
}

type TestState = "idle" | "loading" | "success" | "error"

export function ApiKeyInput({
  provider,
  value,
  onChange,
  maskedKey,
  baseUrl,
  onBaseUrlChange,
  showBaseUrl = false,
  onTest,
  testDisabled = false,
  optional = false,
  id,
}: ApiKeyInputProps) {
  const { t } = useTranslation()
  const [visible, setVisible] = useState(false)
  const [touched, setTouched] = useState(false)
  // A result belongs to the key and endpoint it tested: it stays until either
  // changes, and a different pair reads as untested.
  const subject = `${value}\n${baseUrl ?? ""}`
  const [test, setTest] = useState<{ subject: string; state: TestState; result: TestResult | null } | null>(null)
  const locale = useLocale()
  const current = test && test.subject === subject ? test : null
  const testState: TestState = current?.state ?? "idle"
  const testResult = current?.result ?? null

  const errorId = useId()
  const hasError = touched && !optional && !value && !maskedKey

  const handleBlur = useCallback(() => {
    setTouched(true)
  }, [])

  const handleTest = useCallback(async () => {
    if (!onTest || testDisabled || !value) return
    const tested = subject
    setTest({ subject: tested, state: "loading", result: null })
    let result: TestResult
    try {
      result = await onTest(provider, value)
    } catch {
      result = { success: false, error: t("setup.testFailed") }
    }
    setTest((prev) => (prev && prev.subject === tested
      ? { subject: tested, state: result.success ? "success" : "error", result }
      : prev))
  }, [onTest, testDisabled, value, subject, provider, t])

  const credits = testState === "success" ? testResult?.credits : undefined

  return (
    <div className="flex flex-col gap-3 w-full">
      {/* API key row */}
      <div className="flex flex-col sm:flex-row gap-2 min-w-0">
        <div className="relative flex-1 min-w-0">
          <Input
            id={id}
            type={visible ? "text" : "password"}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onBlur={handleBlur}
            placeholder={maskedKey || t("setup.pasteKeyPlaceholder")}
            className="pr-12"
            style={
              hasError
                ? { borderColor: "var(--color-loss)" }
                : undefined
            }
            aria-invalid={hasError ? true : undefined}
            aria-describedby={hasError ? errorId : undefined}
            autoComplete="off"
            data-1p-ignore
            data-lpignore="true"
            spellCheck={false}
          />
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="absolute right-1 top-1/2 -translate-y-1/2 h-8 w-8"
            onClick={() => setVisible((v) => !v)}
            aria-label={visible ? t("setup.hideKey") : t("setup.showKey")}
            tabIndex={0}
          >
            {visible ? (
              <EyeOff className="h-4 w-4" style={{ color: "var(--color-text-secondary)" }} />
            ) : (
              <Eye className="h-4 w-4" style={{ color: "var(--color-text-secondary)" }} />
            )}
          </Button>
        </div>

        {/* Test button */}
        {onTest && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={testDisabled || testState === "loading" || !value}
            onClick={handleTest}
            title={testDisabled ? t("setup.testUnavailable") : undefined}
            className="shrink-0 self-start sm:self-auto h-10"
          >
            {testState === "loading" && (
              <>
                <span aria-hidden="true" className="mr-1.5 shrink-0">
                  <Loader size={14} className="text-current" />
                </span>
                {t("setup.testing")}
              </>
            )}
            {testState === "success" && (
              <>
                <Check
                  className="h-3.5 w-3.5 mr-1.5"
                  style={{ color: "var(--color-success)" }}
                />
                <span style={{ color: "var(--color-success)" }}>{t("setup.testSuccess")}</span>
              </>
            )}
            {(testState === "error" || testState === "idle") && t("setup.testButton")}
          </Button>
        )}
      </div>

      {/* Validation error */}
      {hasError && (
        <p
          id={errorId}
          role="alert"
          className="text-xs"
          style={{ color: "var(--color-loss)" }}
        >
          {t("setup.errorNoApiKey")}
        </p>
      )}

      {/* Test result feedback */}
      {testState === "success" && testResult && (
        <p className="text-xs flex items-center gap-1" style={{ color: "var(--color-success)" }}>
          <span>
            {testResult.models_found != null && testResult.models_found > 0
              ? t("setup.modelsFound", { count: testResult.models_found })
              : t("setup.keyAccepted")}
          </span>
          {testResult.latency_ms != null && (
            <>
              <span>&middot;</span>
              <span>{testResult.latency_ms}ms</span>
            </>
          )}
        </p>
      )}
      {credits && credits.remaining != null && (
        <p className="text-xs" style={{ color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
          {credits.limit != null
            ? t("modelAccess.apiKey.creditsLeft", { left: usd(credits.remaining, locale), total: usd(credits.limit, locale) })
            : t("modelAccess.apiKey.creditsLeftNoLimit", { left: usd(credits.remaining, locale) })}
        </p>
      )}
      {credits && credits.remaining == null && credits.usage != null && (
        <p className="text-xs" style={{ color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
          {t("modelAccess.apiKey.creditsUsed", { used: usd(credits.usage, locale) })}
        </p>
      )}
      {testState === "error" && testResult?.error && (
        <p className="text-xs flex items-center gap-1" style={{ color: "var(--color-loss)" }}>
          <X className="h-3 w-3 shrink-0" />
          {testResult.error}
        </p>
      )}

      {/* Base URL field */}
      {showBaseUrl && (
        <Input
          type="url"
          value={baseUrl ?? ""}
          onChange={(e) => onBaseUrlChange?.(e.target.value)}
          placeholder={t("setup.baseUrlPlaceholder")}
        />
      )}
    </div>
  )
}
