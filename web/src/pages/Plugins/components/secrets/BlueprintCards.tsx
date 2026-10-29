import { useTranslation } from 'react-i18next';
import { Plus } from 'lucide-react';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { UserVaultBlueprint } from '@/pages/ChatAgent/utils/api';
import { PluginOriginBadge } from '../PluginBadges';
import { CellAction, SurfaceGrid, SurfaceSection, SurfaceCell } from '../surface/Surface';

/**
 * "Recommended": the credentials enabled MCP servers and plugins declare but
 * the vault doesn't hold yet, as cells in the page's own grid. Each ends in
 * Set up rather than a state, because what it lists is an invitation to
 * create, not something that exists.
 *
 * The variable name gets a line of its own under the label, and the
 * description the line after: beside the label the two took turns being cut
 * off, and run together on one line a long name pushed the description's
 * first word past the cell's edge, cut mid-word.
 */

interface BlueprintCardsProps {
  /** Plugin-declared entries carry the declaring plugin's name. */
  blueprints: readonly UserVaultBlueprint[];
  /** At the key cap: cells stay visible, held, and the header says why. */
  atCap: boolean;
  onSelect: (blueprint: UserVaultBlueprint) => void;
}

export function BlueprintCards({ blueprints, atCap, onSelect }: BlueprintCardsProps) {
  const { t } = useTranslation();
  return (
    // The reason the cells are held is said in the header, in words, rather
    // than in a hover tooltip on each button that a touch screen never shows.
    <SurfaceSection
      title={t('vault.recommended')}
      count={blueprints.length}
      note={atCap ? t('vault.atCapSetUp') : undefined}
    >
      <SurfaceGrid>
        {blueprints.map((bp) => (
          <SurfaceCell
            key={bp.name}
            mark={<BrandMark name={bp.name} kind="secret" size="md" />}
            name={bp.label}
            meta={
              bp.plugin_name ? (
                <span className="vault-bp-origin">
                  <PluginOriginBadge plugin={bp.plugin_name} />
                </span>
              ) : undefined
            }
            line={
              <span className="vault-bp-line">
                <code className="vault-name-chip">{bp.name}</code>
                {bp.description && <span>{bp.description}</span>}
              </span>
            }
            onOpen={atCap ? undefined : () => onSelect(bp)}
            trailing={
              <CellAction
                icon={<Plus className="h-3.5 w-3.5" aria-hidden />}
                disabled={atCap}
                onClick={() => onSelect(bp)}
              >
                {t('vault.setUp')}
              </CellAction>
            }
          />
        ))}
      </SurfaceGrid>
    </SurfaceSection>
  );
}
