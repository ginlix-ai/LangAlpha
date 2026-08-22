import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import {
  EnabledToggle,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { useSkillContent } from '@/hooks/useSkills';
import { createDateFormatter } from '@/lib/format';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import {
  DetailField,
  DetailHeader,
  DetailOverlay,
  DetailSection,
} from './DetailOverlay';
import { PluginOriginBadge, PluginSuppressedBadge } from './PluginBadges';

/**
 * A skill's detail overlay: the SKILL.md source is the centerpiece — the
 * skill IS its instructions, so showing them beats any summary we could
 * write. Rows stay to one description line; everything else lives here.
 */

const formatDate = createDateFormatter({ dateStyle: 'medium' });

function formatSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(bytes / 1024))} KB`;
}

export function SkillDetail({
  skill,
  onClose,
  onToggle,
  toggling = false,
}: {
  skill: SkillInfo;
  onClose: () => void;
  /** Absent = the surface has no toggle for this row (render read-only). */
  onToggle?: (enabled: boolean) => void;
  toggling?: boolean;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const contentQuery = useSkillContent(skill.name, skill.workspace_id ?? null);
  const lockedByUserTier = skill.disabled_scope === 'user';

  const originLabel =
    skill.origin === 'platform'
      ? t('plugins.detail.originPlatform')
      : skill.origin === 'workspace'
        ? t('plugins.detail.originWorkspace')
        : t('plugins.detail.originUser');

  return (
    <DetailOverlay
      labelId={labelId}
      onClose={onClose}
      header={
        <DetailHeader
          name={skill.name}
          labelId={labelId}
          kind={t('plugins.detail.kindSkill')}
          meta={
            <>
              {skill.command && <TagBadge>/{skill.command}</TagBadge>}
              <span>{originLabel}</span>
              <PluginOriginBadge plugin={skill.plugin_name} variant="prose" />
              <PluginSuppressedBadge row={skill} variant="prose" />
              {lockedByUserTier && (
                <span>{t('plugins.skills.userDisabledBadge')}</span>
              )}
            </>
          }
          controls={
            onToggle && (
              // Stays live while the plugin is off, unlike the user-tier lock
              // beside it. A suppressed row still owns its own `enabled`, and
              // that flag is what decides whether it comes back when the
              // plugin does — so the switch is the one way to exclude a single
              // component before re-enabling its plugin. The badge above says
              // why the row is not delivered right now; the switch is not
              // claiming otherwise.
              <EnabledToggle
                enabled={skill.enabled}
                name={skill.name}
                disabled={toggling || lockedByUserTier}
                onToggle={() => onToggle(!skill.enabled)}
              />
            )
          }
        />
      }
    >
      {skill.description && (
        <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {skill.description}
        </p>
      )}

      <DetailSection title={t('plugins.detail.skillSource')}>
        {contentQuery.isLoading ? (
          <div className="flex items-center gap-2 py-3">
            <Loader size={14} className="text-current" />
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('common.loading')}
            </span>
          </div>
        ) : contentQuery.isError ? (
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('plugins.detail.sourceLoadFailed')}
          </p>
        ) : (
          <pre
            className="text-[0.6875rem] leading-relaxed whitespace-pre-wrap break-words rounded-md p-3.5 max-h-96 overflow-y-auto"
            style={{
              color: 'var(--color-text-secondary)',
              backgroundColor: 'var(--color-bg-card)',
              fontFamily: "'JetBrains Mono', 'Menlo', monospace",
            }}
          >
            {contentQuery.data?.content ?? ''}
          </pre>
        )}
      </DetailSection>

      {skill.tools.length > 0 && (
        <DetailSection title={t('plugins.detail.tools')} count={skill.tools.length}>
          <div className="flex items-center gap-1 flex-wrap">
            {skill.tools.map((tool) => (
              <TagBadge key={tool} soft>
                {tool}
              </TagBadge>
            ))}
          </div>
        </DetailSection>
      )}

      <DetailSection title={t('plugins.detail.info')}>
        <div className="flex flex-col gap-1.5">
          <DetailField label={t('plugins.detail.origin')}>{originLabel}</DetailField>
          {skill.size_bytes > 0 && (
            <DetailField label={t('plugins.detail.size')}>
              {formatSize(skill.size_bytes)}
            </DetailField>
          )}
          {skill.updated_at && (
            <DetailField label={t('plugins.detail.updated')}>
              {formatDate(new Date(skill.updated_at))}
            </DetailField>
          )}
        </div>
      </DetailSection>
    </DetailOverlay>
  );
}
