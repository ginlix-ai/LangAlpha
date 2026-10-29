import { useTranslation } from 'react-i18next';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import { type PlaceRow, runsInHome } from '../utils/place';

export interface AutomationPlace {
  /** Home's id, which stands for the Chief of Staff; undefined until it resolves. */
  homeId: string | undefined;
  runsInHome: (row: PlaceRow) => boolean;
  /** Where the row runs, in one phrase that names its workspace when given. */
  label: (row: PlaceRow, workspaceName?: string) => string;
}

/**
 * Where an automation runs, as the reader sees it: the Chief of Staff, where the
 * Chief of Staff runs from Home, or one workspace and its Analyst.
 */
export function useAutomationPlace(): AutomationPlace {
  const { t } = useTranslation();
  const homeId = useHomeWorkspaceId();
  const inHome = (row: PlaceRow) => runsInHome(row, homeId);
  return {
    homeId,
    runsInHome: inHome,
    label: (row, workspaceName) => {
      if (inHome(row)) return t('agents.chiefOfStaff');
      return workspaceName ? t('agents.inWorkspace', { workspace: workspaceName }) : t('automation.detailWorkspace');
    },
  };
}
