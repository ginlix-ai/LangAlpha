/**
 * A single choice drawn as a card: an optional icon, a title and a one-line
 * description, with a filled check on the one picked.
 *
 * Native radios underneath, as `RadioChoice` does, so the group keeps the
 * browser's radio keyboard (Tab enters at the picked card, the arrow keys move
 * and pick) and each card takes its accessible name from its own text. The
 * input stays the label's direct child, which is what lets `rings-within`
 * ring the whole card when it has keyboard focus.
 *
 * A group that may be left unanswered: the picked card, picked again, puts it
 * down. The group is given `onClear` for that; one without it keeps a pick
 * once made.
 */
import { createContext, useContext, useId, type ReactNode } from 'react';
import { Check } from 'lucide-react';
import { cn } from '@/lib/utils';

interface GroupContext {
  name: string;
  value: string | null;
  onChange: (value: string) => void;
  onClear?: () => void;
}

const OptionGroupContext = createContext<GroupContext | null>(null);

/** The box a card's picture is drawn in (`media`), in pixels and in the
 *  units of the SVG it draws in. */
export const MEDIA_WIDTH = 56;
export const MEDIA_HEIGHT = 40;

interface OptionGroupProps<V extends string> {
  value: V | null;
  onChange: (value: V) => void;
  /** Lets the group go unpicked: called when the picked card is picked
   *  again. A click on the label reaches the input. Space on a radio that is
   *  already checked does nothing in the browser, so it is caught on the way
   *  down instead; the arrow keys move to another card and just pick it. */
  onClear?: () => void;
  /** id of the visible question, so the group is named by what a sighted
   *  reader sees. */
  labelledBy: string;
  /** One card per row unless this lays the grid out otherwise. */
  className?: string;
  children: ReactNode;
}

export function OptionGroup<V extends string>({
  value,
  onChange,
  onClear,
  labelledBy,
  className,
  children,
}: OptionGroupProps<V>) {
  const name = useId();
  return (
    <OptionGroupContext value={{ name, value, onChange: onChange as (value: string) => void, onClear }}>
      <div role="radiogroup" aria-labelledby={labelledBy} className={cn('grid grid-cols-1 gap-2', className)}>
        {children}
      </div>
    </OptionGroupContext>
  );
}

interface OptionCardProps<V extends string> {
  value: V;
  title: ReactNode;
  description?: ReactNode;
  /** Drawn in the text colour, so a picked card's icon darkens with it. */
  icon?: ReactNode;
  /** A picture in the icon's place, as tall as the title and description
   *  together, for what a glyph is too small to show: the SVG content of a
   *  MEDIA_WIDTH by MEDIA_HEIGHT drawing, which the card sets in a well of its
   *  own. In the text colour, as the icon is. */
  media?: ReactNode;
  /** Content under the description, inside the card (a count, a note). */
  children?: ReactNode;
  testId?: string;
}

export function OptionCard<V extends string>({
  value,
  title,
  description,
  icon,
  media,
  children,
  testId,
}: OptionCardProps<V>) {
  const group = useContext(OptionGroupContext);
  const descriptionId = useId();
  if (!group) throw new Error('OptionCard must be used within <OptionGroup>');
  const checked = group.value === value;
  return (
    <label
      data-testid={testId}
      data-state={checked ? 'checked' : 'unchecked'}
      className={cn(
        'rings-within relative flex items-start gap-3 rounded-lg border px-4 py-3.5',
        // Ink, as the app's other choice rows are: amber annotates, it does
        // not select (DESIGN.md, accent discipline).
        checked
          ? 'border-(--color-text-primary) bg-(--color-bg-tag)'
          : 'border-(--color-border-muted) bg-(--color-bg-card) hover:border-(--color-border-default)',
        // A press gives a little, so a pick is felt as the check fills.
        'cursor-pointer press',
      )}
    >
      <input
        type="radio"
        name={group.name}
        value={value}
        checked={checked}
        aria-describedby={description ? descriptionId : undefined}
        onChange={() => group.onChange(value)}
        onClick={() => {
          if (checked && group.onClear) group.onClear();
        }}
        onKeyDown={(e) => {
          if (e.key !== ' ' || !checked || !group.onClear) return;
          // Kept from becoming a click on the way up, which would land on the
          // radio just cleared and pick it straight back.
          e.preventDefault();
          group.onClear();
        }}
        className="sr-only"
      />
      {icon && (
        <span
          aria-hidden="true"
          className="mt-px flex h-5 shrink-0 items-center transition-colors"
          style={{ color: checked ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
        >
          {icon}
        </span>
      )}
      {media && (
        // A well, as a quote list sets its charts in.
        <span
          aria-hidden="true"
          className="flex shrink-0 rounded-md bg-(--color-bg-page) shadow-[inset_0_0_0_1px_var(--color-border-muted)] transition-colors"
          style={{ color: checked ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
        >
          <svg width={MEDIA_WIDTH} height={MEDIA_HEIGHT} viewBox={`0 0 ${MEDIA_WIDTH} ${MEDIA_HEIGHT}`} fill="none">
            {media}
          </svg>
        </span>
      )}
      <span className="min-w-0 flex-1">
        <span
          className="block text-sm font-medium leading-5"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title}
        </span>
        {description && (
          <span
            id={descriptionId}
            className="mt-0.5 block text-[0.8125rem] leading-snug"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {description}
          </span>
        )}
        {children}
      </span>
      <CheckMark checked={checked} />
    </label>
  );
}

/** The circle at a card's corner: an empty ring, or filled with a check that
 *  grows in as the fill does. */
function CheckMark({ checked }: { checked: boolean }) {
  return (
    <span
      aria-hidden="true"
      className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full border transition-colors"
      style={
        checked
          ? { backgroundColor: 'var(--color-btn-primary-bg)', borderColor: 'var(--color-btn-primary-bg)' }
          : { borderColor: 'var(--color-text-quaternary)' }
      }
    >
      <Check
        className={cn(
          'h-2.5 w-2.5 transition-[opacity,scale]',
          checked ? 'scale-100 opacity-100' : 'opacity-0 motion-safe:scale-50',
        )}
        strokeWidth={3.5}
        style={{ color: 'var(--color-btn-primary-text)' }}
      />
    </span>
  );
}
