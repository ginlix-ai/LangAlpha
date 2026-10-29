import { useTranslation } from 'react-i18next';

export interface ModelSlotText {
  label: string;
  description: string;
  placeholder: string;
  /** The slot's row on the setup summary. */
  summary: string;
  explainerTitle: string;
  explainerBody: string;
}

export interface ModelSlotCopy {
  /** `preferred_model`, the model conversations run on. */
  default: ModelSlotText;
  /** `preferred_background_model`, the cheaper one. */
  background: ModelSlotText;
  explainerTitle: string;
  /** Placeholder for a routing slot left blank to follow the second model. */
  defaultsToBackground: string;
  /** Placeholder for the compaction slot left blank, which follows the conversation's own model. */
  defaultsToConversation: string;
  /** Placeholder for a second model left on Auto, which follows the first. */
  backgroundSameAsDefault: string;
  fallbackDesc: string;
  chooseDesc: string;
}

/**
 * How Setup and Settings name the two model slots: Default and Background,
 * after what each one runs.
 */
export function useModelSlotCopy(): ModelSlotCopy {
  const { t } = useTranslation();
  return {
    default: {
      label: t('agents.models.defaultModel'),
      description: t('agents.models.defaultModelDesc'),
      placeholder: t('agents.models.defaultPlaceholder'),
      summary: t('agents.models.defaultModel'),
      explainerTitle: t('agents.models.defaultModel'),
      explainerBody: t('agents.models.explainerDefaultBody'),
    },
    background: {
      label: t('agents.models.backgroundModel'),
      description: t('agents.models.backgroundModelDesc'),
      placeholder: t('agents.models.backgroundPlaceholder'),
      summary: t('agents.models.backgroundModel'),
      explainerTitle: t('agents.models.backgroundModel'),
      explainerBody: t('agents.models.explainerBackgroundBody'),
    },
    explainerTitle: t('agents.models.explainerTitle'),
    defaultsToBackground: t('agents.models.defaultsToBackground'),
    defaultsToConversation: t('settings.modelTuning.defaultsToConversationModel'),
    backgroundSameAsDefault: t('agents.models.backgroundSameAsDefault'),
    fallbackDesc: t('agents.models.fallbackModelsDesc'),
    chooseDesc: t('agents.models.chooseYourModelsDesc'),
  };
}
