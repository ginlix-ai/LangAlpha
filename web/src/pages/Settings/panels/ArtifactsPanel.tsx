import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { usePreferences } from '@/hooks/usePreferences';
import {
  artifactAnswersPatch,
  commentDestinationPatch,
  commentEnterPatch,
  readArtifactAnswers,
  readCommentDestination,
  readCommentEnter,
} from '@/lib/artifactPreferences';
import { queryKeys } from '@/lib/queryKeys';
import { SegmentedRow, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';
import { usePreferenceWrite } from './usePreferenceWrite';

/** Artifacts: where comments on a file go, what Enter does in the comment
 *  box, and where the agent's full answer to them goes. */
export function ArtifactsPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const queryClient = useQueryClient();
  const { preferences } = usePreferences();
  const write = usePreferenceWrite();

  const destination = readCommentDestination(preferences);
  const enter = readCommentEnter(preferences);
  const answers = readArtifactAnswers(preferences);

  return (
    <SettingsGroup>
      <SegmentedRow
        {...copy(SETTINGS.commentDestination)}
        value={destination}
        onChange={(v) => {
          if (v === destination) return;
          // The server reads the setting into each file's default destination.
          void write(commentDestinationPatch(v)).then(() =>
            queryClient.invalidateQueries({ queryKey: queryKeys.artifacts.allDestinations }));
        }}
        options={[
          { value: 'beside', label: t('settings.artifacts.destinationBeside') },
          { value: 'creator', label: t('settings.artifacts.destinationCreator') },
          { value: 'new', label: t('settings.artifacts.destinationNew') },
        ]}
      />
      <SegmentedRow
        {...copy(SETTINGS.commentEnter)}
        value={enter}
        onChange={(v) => { if (v !== enter) void write(commentEnterPatch(v)); }}
        options={[
          { value: 'queue', label: t('pageViewer.comments.addToQueue') },
          { value: 'send', label: t('pageViewer.comments.sendNow') },
        ]}
      />
      <SegmentedRow
        {...copy(SETTINGS.artifactAnswers)}
        value={answers}
        onChange={(v) => { if (v !== answers) void write(artifactAnswersPatch(v)); }}
        options={[
          { value: 'auto', label: t('settings.artifacts.answersAuto') },
          { value: 'chat', label: t('settings.artifacts.answersChat') },
          { value: 'file', label: t('settings.artifacts.answersFile') },
        ]}
      />
    </SettingsGroup>
  );
}
