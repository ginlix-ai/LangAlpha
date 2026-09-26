import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Select,
  SelectHeader,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectSection,
  SelectTrigger,
} from '@/components/ui/aria-select';
import { Autocomplete, SearchField } from '@/components/ui/aria-search-field';
import { currentTimezoneName, isKnownTimezone } from '@/lib/deviceTimezone';
import { formatTimezoneName } from '@/lib/format';
import { formatUtcOffset } from '@/lib/timezones';
import { cn } from '@/lib/utils';
import {
  allTimezones,
  byOffset,
  COMMON_TIMEZONES,
  describeZone,
  foldZoneText,
  searchZones,
  warmZoneSearch,
  type Zone,
} from './TimezonePicker.zones';
import './TimezonePicker.css';

interface TimezonePickerProps {
  /** An IANA zone, or '' while none is set. */
  value: string;
  onChange: (tz: string) => void;
  /** The zone offered first, under its own heading: the user's own zone, or
   *  this device's where the user is choosing their own. */
  home?: { zone: string; label: string };
  placeholder?: string;
  className?: string;
  /** The trigger's own classes, for a form whose fields take another fill. */
  triggerClassName?: string;
  'aria-label'?: string;
}

const HEADING = 'pb-1 font-mono text-[11px] font-normal text-[color:var(--color-text-tertiary)]';

/**
 * A time zone, named the way people say it ("Eastern Time") with its city
 * and offset beside it. It opens on the home zone and the common ones, and
 * a search reaches every zone the browser knows.
 */
export default function TimezonePicker({
  value,
  onChange,
  home,
  placeholder,
  className,
  triggerClassName,
  'aria-label': ariaLabel,
}: TimezonePickerProps) {
  const { t, i18n } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  // Taken again on every opening, so the offsets are the ones in force that day.
  const [now, setNow] = useState(() => new Date());
  // A zone may carry a retired name (Asia/Calcutta) that the lists below hold
  // under its current one, so each is looked up by that one.
  const homeId = home && isKnownTimezone(home.zone) ? currentTimezoneName(home.zone) : undefined;
  const selected = value ? currentTimezoneName(value) : '';

  // The shortlist is always built: Select refuses to open on an empty list.
  const homeZone = useMemo(
    () => (homeId ? describeZone(homeId, now, i18n.language) : undefined),
    [homeId, now, i18n.language],
  );
  const common = useMemo(
    () =>
      [...new Set([...COMMON_TIMEZONES, selected])]
        .filter((id) => id && id !== homeId)
        .map((id) => describeZone(id, now, i18n.language))
        .sort(byOffset),
    [homeId, selected, now, i18n.language],
  );

  useEffect(() => warmZoneSearch(i18n.language), [i18n.language]);

  // Every other zone is described only once the picker opens, for search.
  const zones = useMemo(() => {
    if (!open) return [];
    const ids = allTimezones();
    return (selected && !ids.includes(selected) ? [...ids, selected] : ids).map((id) =>
      describeZone(id, now, i18n.language),
    );
  }, [open, selected, now, i18n.language]);
  const results = useMemo(
    () => searchZones(zones, query, new Set([...(homeId ? [homeId] : []), ...COMMON_TIMEZONES])),
    [zones, query, homeId],
  );

  const option = (z: Zone) => (
    <SelectItem key={z.id} id={z.id} textValue={z.name} className="gap-2 text-[0.8125rem]">
      <span className="tz-picker-name">{z.name}</span>
      {!foldZoneText(z.name).includes(foldZoneText(z.city)) && <span className="tz-picker-city">{z.city}</span>}
      <span className="tz-picker-offset">{formatUtcOffset(z.offset)}</span>
    </SelectItem>
  );

  return (
    <Select
      aria-label={ariaLabel ?? t('settings.timezone')}
      selectedKey={selected || null}
      onSelectionChange={(k) => {
        if (k != null) onChange(String(k));
      }}
      isOpen={open}
      onOpenChange={(next) => {
        if (next) setNow(new Date());
        else setQuery('');
        setOpen(next);
      }}
      className={cn('inline-flex min-w-0', className)}
    >
      <SelectTrigger className={triggerClassName}>
        <span
          className="min-w-0 flex-1 truncate data-[placeholder]:text-[color:var(--color-text-tertiary)]"
          data-placeholder={value ? undefined : ''}
        >
          {value ? formatTimezoneName(value) : placeholder}
        </span>
      </SelectTrigger>
      <SelectPopover
        placement="bottom start"
        offset={6}
        // As wide as a full-width trigger, and never narrower than a row.
        className="flex w-[max(400px,var(--trigger-width,0px))] max-w-[calc(100vw_-_32px)] flex-col overflow-hidden"
      >
        <Autocomplete inputValue={query} onInputChange={setQuery}>
          <SearchField aria-label={t('timezone.search')} placeholder={t('timezone.search')} autoFocus />
          <SelectListBox
            className="max-h-80 min-h-0"
            renderEmptyState={() => (
              <p className="px-3 py-4 text-center text-[0.8125rem] text-[color:var(--color-text-tertiary)]">
                {t('timezone.noMatch', { query: query.trim() })}
              </p>
            )}
          >
            {query.trim() ? (
              results.map(option)
            ) : (
              <>
                {homeZone && (
                  <SelectSection id="home">
                    <SelectHeader className={HEADING}>{home?.label}</SelectHeader>
                    {option(homeZone)}
                  </SelectSection>
                )}
                <SelectSection
                  id="common"
                  className={cn(homeZone && 'mt-1 border-t border-[color:var(--color-border-muted)] pt-1')}
                >
                  <SelectHeader className={HEADING}>{t('timezone.common')}</SelectHeader>
                  {common.map(option)}
                </SelectSection>
              </>
            )}
          </SelectListBox>
        </Autocomplete>
      </SelectPopover>
    </Select>
  );
}
