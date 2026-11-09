/**
 * The bot avatar editor: a live preview and the fields the UI recognises for
 * `bots.avatar`. An untouched value is handed back to the caller as null, so
 * an edit that never opens this section sends no `avatar` at all and preserves
 * an existing image/emoji (and any key a newer client wrote).
 */
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { BotMark } from '@/components/bots/BotMark';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { SegmentedControl } from '@/components/ui/segmented-control';
import {
  avatarKindOf,
  autoAvatar,
  BOT_COLORS,
  BOT_SHAPES,
  botColorToken,
  botGlyph,
  emojiAvatar,
  firstGrapheme,
  imageAvatar,
  isBotColor,
  isBotShape,
  isSafeImageUrl,
  lookOfBot,
  markAvatar,
  type BotAvatarKind,
  type BotColor,
  type BotShape,
} from '@/lib/botMark';

/** The catalog key for a palette entry or a shape name. */
const COLOR_KEY = (color: BotColor) => `bots.avatar.colors.${color}`;
const SHAPE_KEY = (shape: BotShape) => `bots.avatar.shapes.${shape}`;

/** The glyph a mark draws when the reader leaves the field empty. */
function glyphOrName(glyph: string, name: string): string {
  const trimmed = glyph.trim();
  return trimmed ? trimmed : botGlyph(name);
}

export interface BotAvatarEditorProps {
  /** The bot being edited; its id seeds the automatic mark. */
  botId: string;
  name: string;
  /** The stored avatar, or the draft the reader has built so far. */
  value: Record<string, unknown>;
  /** Null means "the reader has not changed the avatar"; a change is the
   *  whole value to PATCH. */
  onChange: (value: Record<string, unknown> | null) => void;
}

export function BotAvatarEditor({ botId, name, value, onChange }: BotAvatarEditorProps) {
  const { t } = useTranslation();
  const derived = lookOfBot({ bot_id: botId, name, avatar: {} });
  // The mode is the reader's pick while they type: an empty or half-typed image
  // URL must not save `kind: 'image'`, so the saved value can be automatic
  // while the form stays on the image fields.
  const [mode, setMode] = useState<BotAvatarKind>(avatarKindOf(value));
  const [imageDraft, setImageDraft] = useState(typeof value.url === 'string' ? value.url : '');
  // The fields a switch back to a kind starts from: what the avatar already
  // carries, else the id-derived mark, so the preview never sits empty.
  const shape: BotShape = isBotShape(value.shape) ? value.shape : derived.kind === 'mark' ? derived.shape : BOT_SHAPES[0];
  const color: BotColor = isBotColor(value.color) ? value.color : derived.kind === 'mark' ? derived.color : BOT_COLORS[0];
  const glyph = typeof value.glyph === 'string' ? value.glyph : '';
  const emoji = typeof value.emoji === 'string' ? value.emoji : '';
  // An unsafe image URL never previews or saves as an image.
  const previewValue = mode === 'image' && !isSafeImageUrl(imageDraft) ? autoAvatar(value) : value;

  const setKind = (next: BotAvatarKind) => {
    setMode(next);
    if (next === 'auto') return onChange(autoAvatar(value));
    if (next === 'mark') return onChange(markAvatar(shape, color, undefined, value));
    if (next === 'emoji') return onChange(emojiAvatar(emoji || '🙂', value));
    // Image: only a URL this app will load becomes the stored avatar; an empty
    // or unsafe one falls back to the automatic mark until it is fixed.
    return onChange(isSafeImageUrl(imageDraft) ? imageAvatar(imageDraft, value) : autoAvatar(value));
  };

  const changeImageUrl = (next: string) => {
    setImageDraft(next);
    onChange(isSafeImageUrl(next) ? imageAvatar(next, value) : autoAvatar(value));
  };

  return (
    <div className="flex flex-col gap-2">
      <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {t('bots.avatar.label')}
      </span>
      <div className="flex items-start gap-3">
        {/* Real-time preview of exactly what every surface will draw. */}
        <BotMark bot={{ bot_id: botId, name, avatar: previewValue }} size={36} />
        <div className="flex min-w-0 flex-1 flex-col gap-2">
          <SegmentedControl<BotAvatarKind>
            label={t('bots.avatar.label')}
            value={mode}
            onChange={setKind}
            options={[
              { value: 'auto', label: t('bots.avatar.auto') },
              { value: 'mark', label: t('bots.avatar.mark') },
              { value: 'emoji', label: t('bots.avatar.emoji') },
              { value: 'image', label: t('bots.avatar.image') },
            ]}
          />

          {mode === 'mark' && (
            <>
              <div className="flex flex-col gap-1">
                <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('bots.avatar.colorLabel')}
                </span>
                <div role="group" aria-label={t('bots.avatar.colorLabel')} className="flex flex-wrap gap-1.5">
                  {BOT_COLORS.map((entry) => {
                    const selected = entry === color;
                    return (
                      <button
                        key={entry}
                        type="button"
                        aria-label={t(COLOR_KEY(entry))}
                        aria-pressed={selected}
                        title={t(COLOR_KEY(entry))}
                        onClick={() => onChange(markAvatar(shape, entry, glyph, value))}
                        className="h-5 w-5 rounded-full"
                        style={{
                          backgroundColor: botColorToken(entry),
                          boxShadow: selected ? '0 0 0 2px var(--color-text-primary)' : undefined,
                        }}
                      />
                    );
                  })}
                </div>
              </div>
              <div className="flex flex-wrap items-end gap-2">
                <label className="flex min-w-0 flex-col gap-1">
                  <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('bots.avatar.shapeLabel')}
                  </span>
                  <Select
                    value={shape}
                    onChange={(e) => {
                      const next = e.target.value;
                      if (isBotShape(next)) onChange(markAvatar(next, color, glyph, value));
                    }}
                  >
                    {BOT_SHAPES.map((entry) => (
                      <option key={entry} value={entry}>{t(SHAPE_KEY(entry))}</option>
                    ))}
                  </Select>
                </label>
                <label className="flex min-w-0 flex-1 flex-col gap-1">
                  <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('bots.avatar.glyph')}
                  </span>
                  <Input
                    value={glyph}
                    onChange={(e) => onChange(markAvatar(shape, color, firstGrapheme(e.target.value), value))}
                    placeholder={glyphOrName('', name)}
                    aria-label={t('bots.avatar.glyph')}
                  />
                </label>
              </div>
              <p className="text-[0.6875rem] leading-snug" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('bots.avatar.glyphHint')}
              </p>
            </>
          )}

          {mode === 'emoji' && (
            <label className="flex flex-col gap-1">
              <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('bots.avatar.emoji')}
              </span>
              <Input
                value={emoji}
                maxLength={8}
                onChange={(e) => onChange(emojiAvatar(e.target.value, value))}
                placeholder={t('bots.avatar.emojiPlaceholder')}
                aria-label={t('bots.avatar.emoji')}
              />
            </label>
          )}

          {mode === 'image' && (
            <label className="flex flex-col gap-1">
              <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('bots.avatar.imageUrl')}
              </span>
              <Input
                value={imageDraft}
                onChange={(e) => changeImageUrl(e.target.value)}
                placeholder={t('bots.avatar.imagePlaceholder')}
                aria-label={t('bots.avatar.imageUrl')}
              />
            </label>
          )}
        </div>
      </div>
    </div>
  );
}
