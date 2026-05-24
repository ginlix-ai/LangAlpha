/**
 * InstantiateDialog — Generic "+ New entry" modal that renders form fields
 * based on a template's manifest.
 *
 * Reads the manifest's `fields[]` and creates the corresponding inputs.
 * Submits a TemplateInstantiateRequest via the orchestrator.
 *
 * Usage:
 *   <InstantiateDialog
 *     open={open}
 *     onOpenChange={setOpen}
 *     manifest={manifest}
 *     onCreated={(entry) => navigate(...)}
 *   />
 */
import { useEffect, useState } from 'react';
import { Loader2 } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useInstantiateEntry } from '../hooks/useTemplates';
import type {
  TemplateEntry,
  TemplateField,
  TemplateManifest,
} from '@/types/template';

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  manifest: TemplateManifest;
  onCreated?: (entry: TemplateEntry) => void;
}

const ENTRY_KEY_FIELD = 'entry_key';
const DISPLAY_NAME_FIELD = 'display_name';

export function InstantiateDialog({ open, onOpenChange, manifest, onCreated }: Props) {
  const [values, setValues] = useState<Record<string, string>>(() =>
    initialValues(manifest.fields),
  );
  const [error, setError] = useState<string | null>(null);
  const mutation = useInstantiateEntry(manifest.id);

  // Reset state when dialog opens with a fresh manifest.
  useEffect(() => {
    if (open) {
      setValues(initialValues(manifest.fields));
      setError(null);
    }
  }, [open, manifest.fields]);

  const handleChange = (name: string, value: string) => {
    setValues((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    // Validate required fields
    for (const f of manifest.fields) {
      if (f.required && !values[f.name]?.trim()) {
        setError(`请填写「${f.label}」`);
        return;
      }
    }

    const entry_key = values[ENTRY_KEY_FIELD];
    const display_name = values[DISPLAY_NAME_FIELD] || undefined;

    // Pack the remaining fields into `params`
    const params: Record<string, unknown> = {};
    for (const f of manifest.fields) {
      if (f.name === ENTRY_KEY_FIELD || f.name === DISPLAY_NAME_FIELD) continue;
      const v = values[f.name];
      if (v !== undefined && v !== '') params[f.name] = v;
    }

    try {
      const entry = await mutation.mutateAsync({ entry_key, display_name, params });
      onOpenChange(false);
      onCreated?.(entry);
    } catch (e: any) {
      const detail = e?.response?.data?.detail || e?.message || '创建失败';
      setError(typeof detail === 'string' ? detail : JSON.stringify(detail));
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>新增分析 — {manifest.name}</DialogTitle>
          {manifest.estimated_minutes ? (
            <DialogDescription>
              预计分析耗时约 {manifest.estimated_minutes} 分钟。提交后会自动创建独立工作区并启动 Agent。
            </DialogDescription>
          ) : null}
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-3">
          {manifest.fields.map((f) => (
            <FieldRow
              key={f.name}
              field={f}
              value={values[f.name] ?? ''}
              onChange={(v) => handleChange(f.name, v)}
            />
          ))}

          {error ? (
            <div className="text-sm text-red-500 px-1">{error}</div>
          ) : null}

          <DialogFooter className="pt-3">
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={mutation.isPending}
            >
              取消
            </Button>
            <Button type="submit" disabled={mutation.isPending}>
              {mutation.isPending ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  提交中…
                </>
              ) : (
                '开始分析'
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function FieldRow({
  field,
  value,
  onChange,
}: {
  field: TemplateField;
  value: string;
  onChange: (v: string) => void;
}) {
  if (field.type === 'select') {
    return (
      <div className="space-y-1">
        <label className="text-sm font-medium">
          {field.label}
          {field.required ? <span className="text-red-500 ml-1">*</span> : null}
        </label>
        <select
          className="w-full h-9 rounded-md border border-input bg-background px-3 py-1 text-sm"
          value={value}
          onChange={(e) => onChange(e.target.value)}
        >
          <option value="" disabled>
            请选择…
          </option>
          {(field.options ?? []).map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <label className="text-sm font-medium">
        {field.label}
        {field.required ? <span className="text-red-500 ml-1">*</span> : null}
      </label>
      <Input
        type={field.type === 'number' ? 'number' : 'text'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={field.placeholder ?? ''}
      />
    </div>
  );
}

function initialValues(fields: TemplateField[]): Record<string, string> {
  const init: Record<string, string> = {};
  for (const f of fields) {
    init[f.name] = '';
  }
  return init;
}
