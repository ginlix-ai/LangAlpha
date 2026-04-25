import { Sparkles, Star } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { PRESETS_META, type PresetId } from '../presets';
import { PresetThumbnail } from './presets/Thumbnails';

interface PresetsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onApply: (id: PresetId) => void;
}

export function PresetsDialog({ open, onOpenChange, onApply }: PresetsDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="max-w-[1120px] w-[96vw] max-h-[92vh] overflow-hidden flex flex-col"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-elevated)',
        }}
      >
        <DialogHeader className="flex flex-row items-start justify-between gap-4">
          <div>
            <div
              className="text-[11px] font-semibold uppercase tracking-[0.08em] mb-1.5"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              Dashboard layouts
            </div>
            <DialogTitle
              className="text-[28px] leading-tight tracking-tight"
              style={{ color: 'var(--color-text-primary)', fontWeight: 600 }}
            >
              Start with a preset
            </DialogTitle>
            <DialogDescription
              className="mt-2 text-[13px] max-w-[64ch]"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              Six curated layouts tuned to different workflows. Pick one to start, then rearrange
              and swap widgets freely. Your existing layout is saved and can be restored.
            </DialogDescription>
          </div>
          <div
            className="flex items-center gap-1.5 text-[11px] px-2 py-1 rounded-full border flex-shrink-0"
            style={{
              backgroundColor: 'var(--color-accent-soft)',
              borderColor: 'var(--color-accent-primary)',
              color: 'var(--color-accent-primary)',
            }}
          >
            <Sparkles size={10} /> tailored to your holdings
          </div>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
            {PRESETS_META.map((preset) => (
              <article
                key={preset.id}
                role="button"
                tabIndex={0}
                onClick={() => {
                  onApply(preset.id);
                  onOpenChange(false);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onApply(preset.id);
                    onOpenChange(false);
                  }
                }}
                className="preset-card group relative rounded-2xl border p-5 cursor-pointer transition"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  borderColor: 'var(--color-border-muted)',
                }}
                onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-accent-primary)')}
                onMouseLeave={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
              >
                {preset.popular && (
                  <span
                    className="absolute top-3 right-3 text-[10px] px-2 py-0.5 rounded-full border flex items-center gap-1"
                    style={{
                      backgroundColor: '#F3E6CA',
                      color: '#9A6A1E',
                      borderColor: '#E0CC9B',
                    }}
                  >
                    <Star size={9} fill="currentColor" /> most popular
                  </span>
                )}
                <div className="flex items-start justify-between mb-3 gap-3 pr-20">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                      <span
                        className="text-[22px] leading-tight"
                        style={{ color: 'var(--color-text-primary)', fontWeight: 600 }}
                      >
                        {preset.name}
                      </span>
                      <span
                        className="text-[10px] px-2 py-0.5 rounded-full border"
                        style={{
                          backgroundColor: 'var(--color-bg-subtle)',
                          borderColor: 'var(--color-border-muted)',
                          color: 'var(--color-text-secondary)',
                        }}
                      >
                        {preset.tag}
                      </span>
                    </div>
                    <p
                      className="text-[12.5px] leading-relaxed"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      {preset.description}
                    </p>
                  </div>
                </div>
                <div
                  className="rounded-xl p-3"
                  style={{
                    backgroundColor: 'var(--color-bg-page)',
                    border: '1px solid var(--color-border-muted)',
                  }}
                >
                  <PresetThumbnail id={preset.id} />
                </div>
                <div className="mt-3 flex flex-wrap gap-1.5">
                  {preset.pills.map((pill) => (
                    <span
                      key={pill}
                      className="text-[10px] px-2 py-0.5 rounded-full border"
                      style={{
                        backgroundColor: 'var(--color-bg-subtle)',
                        borderColor: 'var(--color-border-muted)',
                        color: 'var(--color-text-secondary)',
                      }}
                    >
                      {pill}
                    </span>
                  ))}
                </div>
                <div className="mt-3 flex items-center justify-between text-[11.5px]">
                  <span style={{ color: 'var(--color-text-tertiary)' }}>
                    Best when: {preset.bestFor}
                  </span>
                  <button
                    type="button"
                    className="h-8 px-3 rounded-md text-[12px] whitespace-nowrap flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
                    style={{
                      backgroundColor: 'var(--color-text-primary)',
                      color: 'var(--color-bg-card)',
                    }}
                  >
                    Use this layout →
                  </button>
                </div>
              </article>
            ))}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
