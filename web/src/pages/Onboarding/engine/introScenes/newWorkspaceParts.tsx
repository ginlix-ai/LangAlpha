import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, FileText, Upload, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { providerLabel } from '@/pages/ChatAgent/components/providerUi';
import { during, progress, useSceneTime } from './clock';
import { Appear, TypedInPlace, pressAt } from './kit';
import { MACHINES } from './galleryParts';

/* The new-workspace modal (`CreateWorkspaceModal`), over the gallery: name,
   description, computer, files and Create, the fields filling as the
   scene's script reaches them. */

/** A field's label, as the modal sets it: the name, then a required mark or
    an optional note. */
function FieldLabel({ children, required = false }: { children: ReactNode; required?: boolean }) {
  const { t } = useTranslation();
  return (
    <span
      className="text-[8px] font-medium leading-none"
      style={{ color: 'var(--color-text-primary)' }}
    >
      {children}{' '}
      {required ? (
        <span style={{ color: 'var(--color-loss)' }}>*</span>
      ) : (
        <span className="text-[7px] font-normal" style={{ color: 'var(--color-text-secondary)' }}>
          {t('common.optional')}
        </span>
      )}
    </span>
  );
}

/** A text field in the modal: the card fill, and the accent edge while focused. */
function field(focus: boolean): CSSProperties {
  return {
    background: 'var(--color-bg-card)',
    borderColor: focus ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
  };
}

/** A file added to the new workspace: its upload bar fills from `at`, then
    the row reads as the modal lists a file, size and remove. Its space is
    held so the modal does not grow. */
function UploadRow({ name, size, at, upload }: { name: string; size: string; at: number; upload: number }) {
  const t = useSceneTime();
  const p = progress(t, at, upload);
  return (
    <Appear
      at={at}
      reserve
      className="flex items-center gap-1.5 rounded-[4px] border px-1.5 py-1"
      style={{ background: 'var(--color-bg-tag)', borderColor: 'var(--color-border-muted)' }}
    >
      <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
      <span
        className="truncate text-[7px] leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {name}
      </span>
      <span className="ml-auto flex h-2 items-center gap-1">
        {p < 1 ? (
          <span
            className="block h-0.5 w-8 overflow-hidden rounded-full"
            style={{ background: 'var(--color-border-muted)' }}
          >
            <span
              className="block h-full rounded-full"
              style={{
                width: `${p * 100}%`,
                background: 'var(--color-accent-primary)',
                // eslint-disable-next-line no-restricted-syntax -- smooths the fill between the script's ticks, not a UI timing
                transition: 'width 90ms linear',
              }}
            />
          </span>
        ) : (
          <>
            <span
              className="text-[6px] leading-none"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {size}
            </span>
            <X className="h-1.5 w-1.5" style={{ color: 'var(--color-text-secondary)' }} />
          </>
        )}
      </span>
    </Appear>
  );
}

/** The modal's width (px), centered on the page. */
export const MODAL_W = 260;

/** What the modal reads from its scene's script: the times it opens, types
    and presses at (ms), the texts typed, and how long a file takes to upload. */
interface NewWorkspaceScript {
  name: string;
  desc: string;
  modal: number;
  typeName: number;
  clickDesc: number;
  typeDesc: number;
  clickDrop: number;
  file: number;
  upload: number;
  clickCreate: number;
}

/**
 * The modal the New workspace click opens (`CreateWorkspaceModal`): name,
 * description, computer, files, Create. The name is focused when it opens,
 * then the description, until the click on the drop zone; `overDrop` has the
 * pointer on the drop zone.
 */
export function NewWorkspaceModal({
  script,
  overDrop,
}: {
  script: NewWorkspaceScript;
  overDrop: boolean;
}) {
  const { t } = useTranslation();
  const now = useSceneTime();
  const nameFocus = during(now, script.modal, script.clickDesc);
  const descFocus = during(now, script.clickDesc, script.clickDrop);
  const typed = 'text-[8px] leading-none';
  return (
    <Appear
      at={script.modal}
      pop
      className="rounded-[10px] border p-2.5"
      style={{
        background: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-muted)',
      }}
    >
      <div className="flex items-center justify-between">
        <span
          className="text-[10px] font-semibold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('workspace.createNewWorkspace')}
        </span>
        <X className="h-2.5 w-2.5" style={{ color: 'var(--color-text-primary)' }} />
      </div>
      <div className="mt-2 flex flex-col gap-1">
        {/* name, focused when the modal opens */}
        <FieldLabel required>{t('workspace.workspaceName')}</FieldLabel>
        <div className="rounded-[4px] border px-1.5 py-1" style={field(nameFocus)}>
          <span className={typed} style={{ color: 'var(--color-text-primary)' }}>
            <TypedInPlace text={script.name} at={script.typeName} focus={nameFocus} />
          </span>
        </div>
        {/* description, a three-line box */}
        <div className="mt-1">
          <FieldLabel>{t('common.description')}</FieldLabel>
        </div>
        <div className="h-7 rounded-[4px] border px-1.5 py-1" style={field(descFocus)}>
          <span className={typed} style={{ color: 'var(--color-text-primary)' }}>
            <TypedInPlace text={script.desc} at={script.typeDesc} focus={descFocus} />
          </span>
        </div>
        {/* the computer, offered once there are two: the primary, named
            with its provider, unless the user picks the other */}
        <span
          className="mt-1 text-[8px] font-medium leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('computer.computerLabel')}
        </span>
        <div
          className="flex items-center rounded-[4px] border px-1.5 py-1"
          style={field(false)}
        >
          <span className={cn(typed, 'flex-1')} style={{ color: 'var(--color-text-primary)' }}>
            {t('computer.primaryOption', {
              name: t('computer.nameWithSource', {
                name: MACHINES.primary.name,
                source: providerLabel('daytona', t),
              }),
            })}
          </span>
          <ChevronDown className="h-2 w-2" style={{ color: 'var(--color-text-secondary)' }} />
        </div>
        <span
          className="text-[6px] leading-none"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('computer.computerHint')}
        </span>
        {/* files, added after the workspace is created */}
        <div className="mt-1">
          <FieldLabel>{t('workspace.files')}</FieldLabel>
        </div>
        <span
          className="text-[6px] leading-none"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('workspace.filesUploadNote')}
        </span>
        <div
          className="mt-0.5 flex flex-col items-center gap-1 rounded-md border border-dashed py-1.5"
          style={{
            borderColor: overDrop
              ? 'var(--color-accent-primary)'
              : 'var(--color-border-muted)',
          }}
        >
          <Upload className="h-3 w-3" style={{ color: 'var(--color-text-secondary)' }} />
          <span
            className="text-[7px] leading-none"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('workspace.dragFilesHere')}
            <span className="font-medium" style={{ color: 'var(--color-accent-primary)' }}>
              {t('workspace.clickToBrowse')}
            </span>
          </span>
        </div>
        <UploadRow name="capex-model-v3.xlsx" size="412 KB" at={script.file} upload={script.upload} />
        <div className="flex items-center justify-end gap-1.5 pt-1.5">
          <span
            className="px-2 py-1 text-[8px] font-medium leading-none"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('common.cancel')}
          </span>
          <span
            className={cn(
              'rounded-[4px] px-2 py-1 text-[8px] font-medium leading-none',
              'transition-transform duration-press'
            )}
            style={{
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
              transform: pressAt(now, script.clickCreate),
            }}
          >
            {t('common.create')}
          </span>
        </div>
      </div>
    </Appear>
  );
}
