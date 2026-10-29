import { useTranslation } from 'react-i18next';
import type { McpCheckFailure, PluginDiagnostic } from '@/pages/ChatAgent/utils/api';
import type { Refusal } from '@/pages/ChatAgent/utils/api/refusal';
import { capitalizeFirst } from '../utils/sentence';

/**
 * One finding, with the rule behind it. The backend cites the spec on the
 * diagnostics that have one, and this is the moment the citation is worth
 * anything: the user has just been refused and cannot tell our policy from
 * the specification's.
 */
export function DiagnosticLine({
  diagnostic,
  color = 'var(--color-text-tertiary)',
}: {
  diagnostic: PluginDiagnostic;
  color?: string;
}) {
  const { t } = useTranslation();
  const { target, message, spec_ref: specRef } = diagnostic;
  // Most findings already open with what they are about ("plugin.json: …"),
  // and naming it twice reads as a stutter. A line that opens on a name
  // keeps the name as written; one that opens on the finding is a sentence.
  const located = !target
    ? capitalizeFirst(message)
    : message.startsWith(target)
      ? message
      : `${target}: ${message}`;
  return (
    <p className="text-[0.6875rem] py-1" style={{ color }}>
      {located}
      {specRef && (
        <>
          {' '}
          <a
            href={specRef}
            target="_blank"
            rel="noreferrer"
            className="underline underline-offset-2"
          >
            {t('plugins.install.specRef')}
          </a>
        </>
      )}
    </p>
  );
}

/** A server whose check refused the save, and the reason it gave. */
export function FailureLine({ failure }: { failure: McpCheckFailure }) {
  const { t } = useTranslation();
  return (
    <>
      <span className="font-mono">{failure.server}</span>
      {t('mcp.check.separator')}
      {failure.reason}
    </>
  );
}

/**
 * Why a save or install did not happen, in the colour of whatever holds it (a
 * callout, a toast, the box below). A check refusal is its servers and their
 * reasons, under a lead that says what they did not pass, since the server's
 * own message only restates the first of them; any other refusal is its
 * message and the findings it carries. `limit` keeps a narrow surface to the
 * first few of each, the full list being on the step that can act on it.
 */
export function RefusalView({
  refusal,
  limit,
  lead = 'mcp.check.failedLead',
}: {
  refusal: Refusal;
  limit?: number;
  lead?: string;
}) {
  const { t } = useTranslation();
  const { message, failures, diagnostics } = refusal;
  if (failures?.length) {
    return (
      <div className="flex flex-col gap-1">
        <p>{t(lead, { count: failures.length })}</p>
        <ul className="flex flex-col gap-0.5">
          {failures.slice(0, limit).map((f) => (
            <li key={f.server}>
              <FailureLine failure={f} />
            </li>
          ))}
        </ul>
      </div>
    );
  }
  return (
    <div className="flex flex-col gap-0.5">
      <p className="wrap-break-word">{message}</p>
      {diagnostics.slice(0, limit).map((d, i) => (
        <DiagnosticLine key={`${d.code}-${i}`} diagnostic={d} color="inherit" />
      ))}
    </div>
  );
}

/**
 * A refusal standing on its own in a form, where nothing around it says a
 * save failed. Nothing was saved, so fixing the input and trying again is the
 * whole remedy, and there is no half-added server to go and clean up.
 */
export function RefusalBox({ refusal }: { refusal: Refusal }) {
  return (
    <div
      role="alert"
      className="text-xs p-2 rounded"
      style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}
      data-testid={refusal.failures?.length ? 'mcp-check-failures' : 'refusal'}
    >
      <RefusalView refusal={refusal} />
    </div>
  );
}
