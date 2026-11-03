import React, { useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useArtifactStatus } from '@/hooks/useArtifact';
import { queryKeys } from '@/lib/queryKeys';
import type { Artifact, ArtifactStatus } from '@/types/api';
import { ArtifactVersionPicker } from '../viewers/html/ArtifactVersionPicker';
import { usePageScout } from '../viewers/html/usePageScout';
import type { ArtifactVersionView } from './useArtifactVersionView';
import { usePublish } from './usePublish';

interface ArtifactCrumbsProps {
  view: ArtifactVersionView;
  /** Open the Publish dialog: an Update with files to review goes there. */
  onReview: () => void;
  /** When the panel last read the file's bytes: a new read is a new live file to compare. */
  readAt: number | null;
}

/**
 * What a published HTML file's crumb row says about it: the version visitors
 * see, with the others to open, and whether the live file has moved on since
 * ("changed since · Update"), or that visitors can't open its link at all (a
 * link shared before versions whose files were past the caps). A file never
 * published says nothing here; its Publish is in the header menu.
 */
export function ArtifactCrumbs({ view, onReview, readAt }: ArtifactCrumbsProps): React.ReactElement | null {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const { record: artifact, version } = view;
  const artifactId = artifact?.artifact_id ?? null;
  const latest = artifact?.latest_seq ?? 0;
  // Compared only where it is said: a shared file, on its live view.
  const shared = !!artifact?.shared;
  const { data: status } = useArtifactStatus(artifact, { enabled: shared && latest > 0 && version === null });
  const publish = usePublish(t('artifact.updateFailed'));
  // The file an Update is reviewing. Dropped once the crumb shows another, so
  // coming back to this one later publishes nothing on its own.
  const [reviewing, setReviewing] = useState<string | null>(null);
  if (reviewing !== null && reviewing !== artifactId) setReviewing(null);

  // The agent's edit, once the panel has read it, is a live file the status
  // has not compared yet.
  const lastRead = useRef(readAt);
  useEffect(() => {
    if (lastRead.current === readAt) return;
    lastRead.current = readAt;
    if (artifactId) void qc.invalidateQueries({ queryKey: queryKeys.artifacts.status(artifactId) });
  }, [readAt, artifactId, qc]);

  // Shared, but no version for visitors until the owner publishes again.
  const refused = shared && !!artifact?.legacy_refused;
  if (!artifact || (latest < 1 && !refused)) return null;
  const changed = shared && version === null && !!status && (status.changed === true || !!status.drift?.added.length);

  const update = () => {
    if (!status || !artifactId) return;
    // A file the page newly reads widens what visitors get: the owner reviews it first.
    if (status.drift?.added.length) {
      onReview();
      return;
    }
    setReviewing(artifactId);
  };

  const publishReviewed = async (target: Artifact, reviewed: ArtifactStatus | null) => {
    try {
      // What running the page added, a list past a cap, or one that could not
      // be read: the dialog shows it.
      if (!reviewed || reviewed.drift?.added.length || reviewed.over_cap) {
        onReview();
        return;
      }
      const result = await publish.send(target, {
        files: reviewed.files.map((f) => f.path),
        page_sha256: reviewed.page_sha256 ?? undefined,
      });
      if (result.kind === 'published') {
        // An Update that finds the files as the latest version left them cuts nothing.
        toast({ description: result.cut ? t('artifact.updatedTo', { seq: result.version.seq }) : t('artifact.unchanged') });
      } else if (result.kind !== 'failed') {
        // A list that moved, or one the server refuses, is the dialog's to show.
        onReview();
      }
    } finally {
      setReviewing(null);
    }
  };

  return (
    <>
      {reviewing === artifact.artifact_id && <UpdateReview artifact={artifact} onReviewed={publishReviewed} />}
      <ArtifactVersionPicker view={view} variant="chip" />
      {refused ? (
        // The list goes through the dialog: it may still be past the caps.
        <CrumbStatus key="refused" tone="warning" words={t('artifact.legacyRefused')} label={t('publish.legacyRefused')}>
          <button type="button" className="file-panel-artifact-update" onClick={onReview}>
            {t('artifact.update')}
          </button>
        </CrumbStatus>
      ) : changed && (
        <CrumbStatus key="changed" words={t('artifact.changedSince')} label={t('publish.changedSince', { seq: latest })}>
          <button
            type="button"
            className="file-panel-artifact-update"
            onClick={update}
            disabled={publish.pending || reviewing === artifactId}
          >
            {t('artifact.update')}
          </button>
        </CrumbStatus>
      )}
    </>
  );
}

/**
 * An Update reviews the file as the Publish dialog does before it publishes:
 * the page runs out of sight first, so a file its code as it is now asks for
 * as it loads is on the list, and the publish names that code, which the
 * server refuses once the agent has changed the page again. Answers once, with
 * the list, or null when it could not be read.
 */
function UpdateReview({ artifact, onReviewed }: {
  artifact: Artifact;
  onReviewed: (artifact: Artifact, status: ArtifactStatus | null) => void;
}): React.ReactNode {
  const scout = usePageScout(artifact.workspace_id, artifact.path);
  const { data, isFetching, isError } = useArtifactStatus(artifact, { always: true, report: true, enabled: scout.reads });
  scout.follow(data?.page_sha256);
  // A page that could not run goes to the dialog, which says so and retries.
  const reviewed = isError || scout.failed ? null : data && !isFetching && !scout.holding ? data : undefined;
  const answered = useRef(false);
  const answer = useEffectEvent(onReviewed);
  useEffect(() => {
    if (reviewed === undefined || answered.current) return;
    answered.current = true;
    answer(artifact, reviewed);
  }, [reviewed, artifact]);
  return scout.element;
}

/**
 * A state of the file and the act it calls for: "● changed since · Update".
 * The words show whole or not at all. They are the row's first give, and the
 * moment they would clip they drop out, leaving the dot, which says them in
 * its title and to a screen reader; they come back once the spacer has room
 * for all of them. Set on the element from the observer, not through state,
 * so no frame paints them cut.
 */
function CrumbStatus({ tone, words, label, children }: {
  tone?: 'warning';
  words: string;
  /** The state in full, a sentence. */
  label: string;
  children: React.ReactNode;
}): React.ReactElement {
  const ref = useRef<HTMLSpanElement>(null);
  useLayoutEffect(() => {
    const group = ref.current;
    const wordsEl = group?.querySelector<HTMLElement>('.file-panel-artifact-drift-words');
    const row = group?.closest<HTMLElement>('.file-panel-crumbs');
    const spacer = row?.querySelector<HTMLElement>('.file-panel-crumb-spacer');
    if (!group || !wordsEl || !row || !spacer || typeof ResizeObserver === 'undefined') return;
    const check = () => {
      // The whole words' width, in the row or out of it.
      const whole = wordsEl.scrollWidth;
      if (!group.hasAttribute('data-dropped')) {
        if (whole - wordsEl.clientWidth > 1) group.setAttribute('data-dropped', '');
        return;
      }
      const room = spacer.getBoundingClientRect().width - (parseFloat(getComputedStyle(spacer).minWidth) || 0);
      const gap = parseFloat(getComputedStyle(group).columnGap) || 0;
      if (room > whole + gap) group.removeAttribute('data-dropped');
    };
    check();
    const observer = new ResizeObserver(check);
    for (const el of [row, spacer, wordsEl]) observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return (
    <span ref={ref} className="file-panel-artifact-drift" title={label}>
      <span className="file-panel-artifact-dot" data-tone={tone} role="img" aria-label={label} />
      <span className="file-panel-artifact-drift-words" aria-hidden="true">{words}</span>
      {children}
    </span>
  );
}
