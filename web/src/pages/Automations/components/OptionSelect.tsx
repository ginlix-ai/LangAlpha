import { useTranslation } from 'react-i18next';
import {
  Select,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/aria-select';

export interface Option<T extends string> {
  value: T;
  labelKey: string;
}

interface OptionSelectProps<T extends string> {
  value: T;
  options: ReadonlyArray<Option<T>>;
  onChange: (value: T) => void;
  'aria-label'?: string;
  'aria-labelledby'?: string;
  className?: string;
}

/** One of a fixed set of values. The key a pick reports is looked up in the
 *  set, which is what narrows it back to the value's type. */
export default function OptionSelect<T extends string>({ value, options, onChange, className, ...aria }: OptionSelectProps<T>) {
  const { t } = useTranslation();
  return (
    <Select
      {...aria}
      selectedKey={value}
      onSelectionChange={(key) => {
        const picked = options.find((o) => o.value === key);
        if (picked) onChange(picked.value);
      }}
      className={className}
    >
      <SelectTrigger>
        <SelectValue />
      </SelectTrigger>
      <SelectPopover>
        <SelectListBox>
          {options.map((o) => (
            <SelectItem key={o.value} id={o.value}>
              {t(o.labelKey)}
            </SelectItem>
          ))}
        </SelectListBox>
      </SelectPopover>
    </Select>
  );
}
