import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { Bot, Cog, User } from 'lucide-react';
import type { DriveActorKind, DriveModifiedBy } from '@/types/drive';
import { threadHref, whoLabel } from '../who';

const GLYPHS = { person: User, agent: Bot, system: Cog } satisfies Record<DriveActorKind, unknown>;

/**
 * A person, the agent (or a subagent) or the system, as a small glyph before
 * their name. The glyph tells them apart, so the text beside it stays in the
 * page's neutral tones.
 */
export function ActorGlyph({ kind }: { kind: DriveActorKind }) {
  const Glyph = GLYPHS[kind];
  return <Glyph className="drive-who-glyph" aria-hidden="true" />;
}

/** The thread a change was made in, by its title, or a generic label where it has none. */
function ThreadLink({ threadId, title }: { threadId: string; title: string | null }) {
  const { t } = useTranslation();
  return <Link to={threadHref(threadId)} className="drive-link-button">{title?.trim() || t('drive:who.aThread')}</Link>;
}

/**
 * A sentence that names who made a change (`<who/>`) and the thread they
 * made it in (`<thread/>`), each language in its own order. The name goes in
 * as text, never as markup: a subagent names itself.
 */
export function ActorSentence({ actor, threadId, i18nKey, values }: {
  actor: DriveModifiedBy;
  /** The thread the change was made in: the caller has checked the actor names one. */
  threadId: string;
  i18nKey: string;
  values?: Record<string, string>;
}) {
  const { t } = useTranslation();
  return (
    <Trans
      i18nKey={i18nKey}
      values={values}
      components={{ who: <>{whoLabel(t, actor)}</>, thread: <ThreadLink threadId={threadId} title={actor.thread_title} /> }}
    />
  );
}

/**
 * Who made a change: their name, after their glyph with `glyph`, and with
 * `thread` the thread they made it in ("Agent in <thread>") where one is known.
 */
export function Actor({ actor, glyph = false, thread = false }: { actor: DriveModifiedBy; glyph?: boolean; thread?: boolean }) {
  const { t } = useTranslation();
  const threadId = thread ? actor.thread_id : null;
  return (
    <>
      {glyph && <ActorGlyph kind={actor.kind} />}
      {threadId ? <ActorSentence actor={actor} threadId={threadId} i18nKey="drive:who.inThread" /> : whoLabel(t, actor)}
    </>
  );
}
