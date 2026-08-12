import { useState, useCallback, useRef, useId, useEffect } from "react"
import { Eye, EyeOff, Check, X } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Loader } from "@/components/ui/loader"
import { cn } from "@/lib/utils"

export interface TestResult {
  success: boolean
  model?: string
  models_found?: number
  latency_ms?: number
  error?: string
}

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
}: ApiKeyInputProps) {
  const [visible, setVisible] = useState(false)
  const [touched, setTouched] = useState(false)
  const [testState, setTestState] = useState<TestState>("idle")
  const [testResult, setTestResult] = useState<TestResult | null>(null)
  const fadeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const errorId = useId()
  const hasError = touched && !value && !maskedKey

  // Clean up fade timer on unmount
  useEffect(() => {
    return () => {
      if (fadeTimerRef.current) clearTimeout(fadeTimerRef.current)
    }
  }, [])

  const handleBlur = useCallback(() => {
    setTouched(true)
  }, [])

  const handleTest = useCallback(async () => {
    if (!onTest || testDisabled || !value) return

    // Clear any pending fade timer
    if (fadeTimerRef.current) {
      clearTimeout(fadeTimerRef.current)
      fadeTimerRef.current = null
    }

    setTestState("loading")
    setTestResult(null)

    try {
      const result = await onTest(provider, value)
      setTestResult(result)

      if (result.success) {
        setTestState("success")
        // Auto-fade success after 5s
        fadeTimerRef.current = setTimeout(() => {
          setTestState("idle")
          setTestResult(null)
        }, 5000)
      } else {
        setTestState("error")
      }
    } catch {
      setTestState("error")
      setTestResult({ success: false, error: "Test request failed" })
    }
  }, [onTest, testDisabled, value, provider])

  return (
    <div className="flex flex-col gap-3 w-full">
      {/* API key row */}
      <div className="flex flex-col sm:flex-row gap-2 min-w-0">
        <div className="relative flex-1 min-w-0">
          <Input
            type={visible ? "text" : "password"}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onBlur={handleBlur}
            placeholder={maskedKey || "Paste your API key here"}
            className={cn("pr-12 focus-visible:ring-offset-0", hasError && "focus-visible:ring-0")}
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
            aria-label={visible ? "Hide API key" : "Show API key"}
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
            title={testDisabled ? "Test not available for this provider" : undefined}
            className="shrink-0 self-start sm:self-auto h-10"
          >
            {testState === "loading" && (
              <>
                <span aria-hidden="true" className="mr-1.5 flex-shrink-0">
                  <Loader size={14} className="text-current" />
                </span>
                Testing...
              </>
            )}
            {testState === "success" && (
              <>
                <Check
                  className="h-3.5 w-3.5 mr-1.5"
                  style={{ color: "var(--color-success)" }}
                />
                <span style={{ color: "var(--color-success)" }}>Key works!</span>
              </>
            )}
            {testState === "error" && "Test"}
            {testState === "idle" && "Test"}
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
          Please enter an API key
        </p>
      )}

      {/* Test result feedback */}
      {testState === "success" && testResult && (
        <p className="text-xs flex items-center gap-1" style={{ color: "var(--color-success)" }}>
          <span>
            {testResult.models_found != null && testResult.models_found > 0
              ? `${testResult.models_found} model${testResult.models_found !== 1 ? 's' : ''} found`
              : 'Key accepted'}
          </span>
          {testResult.latency_ms != null && (
            <>
              <span>&middot;</span>
              <span>{testResult.latency_ms}ms</span>
            </>
          )}
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
          placeholder="Base URL (optional)"
        />
      )}
    </div>
  )
}
