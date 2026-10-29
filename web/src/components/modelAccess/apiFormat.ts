/** The formats a provider of the user's own can speak, and the catalog provider each is defined under. */
export const API_FORMATS = [
  { value: 'openai-responses', labelKey: 'setup.apiFormatOpenaiResponses', parent: 'openai', useResponseApi: true },
  { value: 'openai-completions', labelKey: 'setup.apiFormatOpenaiCompletions', parent: 'openai', useResponseApi: false },
  { value: 'anthropic', labelKey: 'setup.apiFormatAnthropic', parent: 'anthropic', useResponseApi: false },
  { value: 'gemini', labelKey: 'setup.apiFormatGemini', parent: 'gemini', useResponseApi: false },
] as const;

/** Translation key for a provider's API format, from its sdk + use_response_api. */
export function getApiFormatKey(sdk?: string | null, useResponseApi?: boolean): string {
  switch (sdk) {
    case 'anthropic':
      return 'setup.apiFormatAnthropic';
    case 'gemini':
      return 'setup.apiFormatGemini';
    case 'openai':
    case 'dashscope':
      return useResponseApi ? 'setup.apiFormatOpenaiResponses' : 'setup.apiFormatOpenaiCompletions';
    case 'codex':
      return 'setup.apiFormatCodex';
    case 'deepseek':
    case 'openrouter':
    case 'qwq':
      return 'setup.apiFormatCompatible';
    default:
      return sdk ? 'setup.apiFormatGeneric' : 'setup.apiFormatDefault';
  }
}
