/**
 * The flash model a first run fills in beside the primary: another model of
 * the primary's provider, else the first listed model.
 *
 * Setup only. Settings leaves an empty flash slot empty, because there it
 * already runs the primary model.
 */
export function suggestFlashModel(
  models: Record<string, { models?: string[] }>,
  primaryModel: string,
): string | undefined {
  const sameProvider = Object.values(models).find((p) => p.models?.includes(primaryModel))?.models;
  // A provider whose only model is the primary pairs it with itself.
  if (sameProvider) return sameProvider.find((m) => m !== primaryModel) ?? primaryModel;
  return Object.values(models).flatMap((p) => p.models ?? [])[0];
}
