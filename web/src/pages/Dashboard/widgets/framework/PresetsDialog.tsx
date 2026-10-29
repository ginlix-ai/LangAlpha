import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { TermCard, TermTag } from '@/pages/ChatAgent/components/charts/TermCard';
import { TermPanelContext } from '@/pages/ChatAgent/components/charts/termContext';
import '@/pages/ChatAgent/components/charts/termCard.css';
import '@/pages/ChatAgent/components/charts/termCardStyles.css';
import '../../components/dashWindow.css';
import { PRESETS_META, type PresetId } from '../presets';
import { PresetThumbnail } from './presets/Thumbnails';

interface PresetsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onApply: (id: PresetId) => void;
}

export function PresetsDialog({ open, onOpenChange, onApply }: PresetsDialogProps) {
  const { t } = useTranslation();
  const apply = (id: PresetId) => {
    onApply(id);
    onOpenChange(false);
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="dash-window dash-window--radix max-w-[1120px] w-[96vw] max-h-[92vh] overflow-hidden flex flex-col">
        <DialogHeader className="dash-presets-head space-y-0 text-left">
          <div className="term-line dash-window-meta">
            <span className="term-t3">{t('dashboard.widgets.presetsDialog.eyebrow')}</span>
            <TermTag tone="live">{t('dashboard.widgets.presetsDialog.tailoredBadge')}</TermTag>
          </div>
          <DialogTitle className="dash-window-title">{t('dashboard.widgets.presetsDialog.title')}</DialogTitle>
          <DialogDescription className="dash-window-prose dash-window-prose--sub max-w-[64ch]">
            {t('dashboard.widgets.presetsDialog.description')}
          </DialogDescription>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <TermPanelContext value={true}>
            <div className="dash-presets-grid">
              {PRESETS_META.map((preset) => (
                // The card's title is its button: a click anywhere on the card
                // applies it, and the keyboard reaches it once.
                <TermCard
                  key={preset.id}
                  className="dash-preset"
                  title={t(preset.nameKey)}
                  sub={t(preset.tagKey)}
                  meta={preset.popular ? t('dashboard.widgets.presetsDialog.popularBadge') : undefined}
                  onClick={() => apply(preset.id)}
                >
                  <p className="dash-window-prose dash-window-prose--sub">{t(preset.descriptionKey)}</p>
                  <div className="dash-preset-thumb">
                    <PresetThumbnail id={preset.id} />
                  </div>
                  <div className="dash-window-tags">
                    {preset.pillKeys.map((key) => (
                      <TermTag key={key}>{t(key)}</TermTag>
                    ))}
                  </div>
                  <div className="term-line dash-preset-foot">
                    <span className="term-t3">
                      {t('dashboard.widgets.presetsDialog.bestWhen', { bestFor: t(preset.bestForKey) })}
                    </span>
                    <span className="term-push dash-preset-use" aria-hidden="true">
                      {t('dashboard.widgets.presetsDialog.useLayout')}
                    </span>
                  </div>
                </TermCard>
              ))}
            </div>
          </TermPanelContext>
        </div>
      </DialogContent>
    </Dialog>
  );
}
