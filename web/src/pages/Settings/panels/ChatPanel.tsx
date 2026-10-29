import { useTranslation } from 'react-i18next';
import { usePreferences } from '@/hooks/usePreferences';
import { turnEndScrollPatch, readTurnEndScroll } from '@/lib/turnEndScroll';
import {
  readStreamingMode,
  readToolCallLayout,
  readTurnDisplay,
  streamingModePatch,
  toolCallLayoutPatch,
  turnDisplayPatch,
} from '@/lib/transcriptDisplay';
import { SegmentedRow, SettingsGroup, ToggleRow } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';
import { usePreferenceWrite } from './usePreferenceWrite';
import type { Preferences } from './types';

/** Chat: how a reply streams in and what the transcript shows while it runs,
 *  where the view lands once it ends, and the composer's voice input. */
export function ChatPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const { preferences: prefsData } = usePreferences();
  const write = usePreferenceWrite();

  const turnDisplay = readTurnDisplay(prefsData);
  const streamingMode = readStreamingMode(prefsData);
  const toolCallLayout = readToolCallLayout(prefsData);
  const turnEndScroll = readTurnEndScroll(prefsData);
  const voiceInput = (prefsData as Preferences | null)?.other_preference?.voice_input_enabled === true;

  return (
    <>
      <SettingsGroup title={t('settings.repliesGroup')}>
        <SegmentedRow
          {...copy(SETTINGS.turnDisplay)}
          value={turnDisplay}
          onChange={(v) => { if (v !== turnDisplay) void write(turnDisplayPatch(v)); }}
          options={[
            { value: 'lean', label: t('settings.turnDisplayLean') },
            { value: 'verbose', label: t('settings.turnDisplayVerbose') },
          ]}
        />
        <SegmentedRow
          {...copy(SETTINGS.streamingMode)}
          value={streamingMode}
          onChange={(v) => { if (v !== streamingMode) void write(streamingModePatch(v)); }}
          options={[
            { value: 'token', label: t('settings.streamingModeToken') },
            { value: 'paragraph', label: t('settings.streamingModeParagraph') },
          ]}
        />
        <SegmentedRow
          {...copy(SETTINGS.toolCallLayout)}
          value={toolCallLayout}
          onChange={(v) => { if (v !== toolCallLayout) void write(toolCallLayoutPatch(v)); }}
          options={[
            { value: 'rows', label: t('settings.toolCallLayoutRows') },
            { value: 'status', label: t('settings.toolCallLayoutStatus') },
          ]}
        />
        <SegmentedRow
          {...copy(SETTINGS.turnEndScroll)}
          value={turnEndScroll}
          onChange={(v) => { if (v !== turnEndScroll) void write(turnEndScrollPatch(v)); }}
          options={[
            { value: 'bottom', label: t('settings.turnEndScrollBottom') },
            { value: 'reply_start', label: t('settings.turnEndScrollReplyStart') },
          ]}
        />
      </SettingsGroup>

      <SettingsGroup title={t('settings.composerGroup')}>
        <ToggleRow
          {...copy(SETTINGS.voiceInput)}
          checked={voiceInput}
          onChange={() => { void write({ other_preference: { voice_input_enabled: !voiceInput } }); }}
        />
      </SettingsGroup>
    </>
  );
}
