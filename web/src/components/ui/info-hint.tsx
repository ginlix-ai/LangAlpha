import { useState, type ReactNode } from 'react';
import { Info } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from './tooltip';

/**
 * A small info mark beside a setting's name that holds the setting's
 * explanation, so a view of many rows does not repeat it under each. It opens
 * on hover and focus as a tooltip does, and on a tap too, since a touch screen
 * has no hover: a tooltip alone would leave the text out of reach there.
 *
 * The tooltip is in the page only while open, so the text also stays in it
 * unseen under `id`, for the control it describes to name in
 * `aria-describedby`: the mark is only how sighted readers reach it.
 */
export function InfoHint({ name, id, children, className }: {
  /** The setting it explains, which names the mark: "About {name}". */
  name: string;
  /** The unseen copy's id, for the described control's `aria-describedby`. */
  id: string;
  children: ReactNode;
  className?: string;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <>
      <span id={id} className="sr-only">{children}</span>
      <TooltipProvider delayDuration={200}>
        <Tooltip open={open} onOpenChange={setOpen}>
          <TooltipTrigger asChild>
            <button
              type="button"
              aria-label={t('common.infoAbout', { name })}
              className={cn(
                'tap-target inline-grid h-4 w-4 shrink-0 place-items-center rounded-full',
                'text-(--color-text-quaternary) transition-colors hover:text-(--color-text-primary)',
                className,
              )}
              // Radix closes a tooltip on a press of its trigger; here a press
              // is how a touch screen opens it, so a press opens it (a toggle
              // would shut what hover or focus just opened). A press elsewhere
              // closes it.
              onPointerDown={(e) => e.preventDefault()}
              onClick={(e) => {
                e.preventDefault();
                setOpen(true);
              }}
            >
              <Info aria-hidden="true" className="h-3.5 w-3.5" />
            </button>
          </TooltipTrigger>
          <TooltipContent className="max-w-72 text-pretty leading-relaxed">{children}</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </>
  );
}
