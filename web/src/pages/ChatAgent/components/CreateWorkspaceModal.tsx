import React, { useState, useRef, useCallback, useEffect, useId } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { X, Upload, FileText, CheckCircle2, Circle, AlertCircle } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { Input } from '../../../components/ui/input';
import { Select } from '@/components/ui/select';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import type { Computer, Workspace } from '@/types/api';
import { startWorkspace, uploadWorkspaceFile } from '../utils/api';
import { createWorkspaceDenialMessage, denialMessage } from '../utils/denialMessage';
import { clampWorkspaceName } from '../utils/workspaceName';
import type { NewWorkspace } from '../hooks/useCreateWorkspace';
import { useComputers } from '../hooks/useComputers';
import { useComputerSource } from '../hooks/useComputerSource';
import { isComputerPreparing, liveComputers, takesWorkspaces } from '../utils/computerStatus';
import './CreateWorkspaceModal.css';

interface CreateWorkspaceModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCreate: (data: NewWorkspace) => Promise<Workspace>;
  /** Called with the created workspace once the modal is done with it. */
  onComplete?: (workspace: Workspace) => void;
}

type Phase = 'form' | 'progress';
type CreationStep = 'uploading' | 'done' | 'error';
type FileUploadStatus = 'pending' | 'uploading' | 'done' | 'failed';

/**
 * CreateWorkspaceModal.
 *
 * Without queued files, creation opens the workspace immediately. Queued
 * uploads wait for the computer to start and the workspace folder to attach.
 * With more than one computer the user picks which one the folder goes on;
 * with one there is nothing to pick and the server uses the primary. A
 * computer still being prepared takes no workspace, so it cannot be picked,
 * and the form waits while it is the target.
 */
function CreateWorkspaceModal({ isOpen, onClose, onCreate, onComplete }: CreateWorkspaceModalProps) {
  const { t } = useTranslation();
  const locale = useLocale();

  // Lock body scroll while modal is open
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { document.body.style.overflow = prev; };
  }, [isOpen]);

  // Form state
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [queuedFiles, setQueuedFiles] = useState<File[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [pickedComputerId, setPickedComputerId] = useState<string | null>(null);
  const computerFieldId = useId();

  // The list the gallery and the computer fan-out already hold, so opening
  // the modal usually reads the cache rather than the network.
  const { data: computerData } = useComputers({ enabled: isOpen });
  const computers = liveComputers(computerData?.computers);
  const choosesComputer = computers.length > 1;
  // Each option says what the machine runs on, so a Mac and a cloud sandbox
  // are told apart here too.
  const sourceOf = useComputerSource(computers, { enabled: isOpen && choosesComputer });
  // The machine the workspace would land on, named or not. A pick that has
  // left the list (deleted elsewhere) falls back to the primary. The fan-out
  // moves its row to running when the preparation ends, which lifts the hold.
  const target = computers.find((c) => c.computer_id === pickedComputerId)
    ?? computers.find((c) => c.is_primary)
    ?? computers[0]
    ?? null;
  const preparingTarget = isComputerPreparing(target) ? target : null;

  // Progress state
  const [phase, setPhase] = useState<Phase>('form');
  const [creationStep, setCreationStep] = useState<CreationStep>('uploading');
  const [submitting, setSubmitting] = useState(false);
  const [fileStatuses, setFileStatuses] = useState<Record<string, FileUploadStatus>>({});
  const [currentUploadProgress, setCurrentUploadProgress] = useState(0);
  const [currentUploadName, setCurrentUploadName] = useState('');
  const [createdWorkspace, setCreatedWorkspace] = useState<Workspace | null>(null);
  const [progressError, setProgressError] = useState<string | null>(null);

  // Drag state
  const [isDragging, setIsDragging] = useState(false);
  const dragCounter = useRef(0);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ---- File queue helpers ----

  const addFiles = useCallback((fileList: FileList) => {
    const incoming = Array.from(fileList);
    setError(null);

    setQueuedFiles((prev) => {
      const existingNames = new Set(prev.map((f) => f.name));
      const deduped = incoming.filter((f) => !existingNames.has(f.name));
      return [...prev, ...deduped];
    });
  }, []);

  const removeFile = useCallback((fileName: string) => {
    setQueuedFiles((prev) => prev.filter((f) => f.name !== fileName));
  }, []);

  // ---- Drag-and-drop handlers ----

  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current += 1;
    if (dragCounter.current === 1) setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current -= 1;
    if (dragCounter.current === 0) setIsDragging(false);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter.current = 0;
    setIsDragging(false);
    if (e.dataTransfer.files?.length) {
      addFiles(e.dataTransfer.files);
    }
  };

  // ---- Submit ----

  const runUploads = async (workspace: Workspace) => {
    setCreationStep('uploading');
    setProgressError(null);

    const statuses: Record<string, FileUploadStatus> = {};
    queuedFiles.forEach((f) => { statuses[f.name] = 'pending'; });
    setFileStatuses({ ...statuses });
    // The blocking start also attaches a new folder on an already-running
    // computer. Retry must repeat it if the previous start failed.
    try {
      await startWorkspace(workspace.workspace_id);
    } catch (err: unknown) {
      setProgressError(denialMessage(err, t));
      setCreationStep('error');
      return;
    }
    let failed = false;

    for (const file of queuedFiles) {
      setCurrentUploadName(file.name);
      setCurrentUploadProgress(0);
      setFileStatuses((prev) => ({ ...prev, [file.name]: 'uploading' }));

      try {
        await uploadWorkspaceFile(workspace.workspace_id, file, null, (pct: number) => {
          setCurrentUploadProgress(pct);
        });
        setFileStatuses((prev) => ({ ...prev, [file.name]: 'done' }));
      } catch (err: unknown) {
        failed = true;
        setFileStatuses((prev) => ({ ...prev, [file.name]: 'failed' }));
        const e = err as { response?: { status?: number } };
        if (e?.response?.status === 413) {
          const sizeMB = (file.size / (1024 * 1024)).toFixed(1);
          const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
          setProgressError(detail || t('workspace.fileTooLarge', { name: file.name, size: sizeMB }));
        } else {
          setProgressError(denialMessage(err, t));
        }
      }
    }

    // The workspace exists either way; 'error' keeps the reason and the
    // retry on screen instead of a bare failed count.
    setCreationStep(failed ? 'error' : 'done');
  };

  const handleSubmit = async (e: React.FormEvent | Event) => {
    e.preventDefault();
    if (!name.trim() || submitting || preparingTarget) {
      if (!name.trim()) setError(t('workspace.workspaceNameRequired'));
      return;
    }

    setSubmitting(true);
    setError(null);

    let workspace: Workspace;
    try {
      workspace = await onCreate({
        name: name.trim(),
        description: description.trim(),
        ...(choosesComputer && target ? { computerId: target.computer_id } : {}),
      });
    } catch (err: unknown) {
      // A refusal keeps the user on the form with their input intact. On a 429
      // the sentence is the quota service's, relayed as it arrived.
      setSubmitting(false);
      setError(createWorkspaceDenialMessage(err, t));
      return;
    }

    setCreatedWorkspace(workspace);
    setSubmitting(false);

    // Nothing to provision and nothing to upload: the workspace exists, so
    // open it rather than showing a progress screen with nothing on it.
    if (queuedFiles.length === 0) {
      resetAndClose();
      onComplete?.(workspace);
      return;
    }

    setPhase('progress');
    await runUploads(workspace);
  };

  // ---- Retry (after an upload failure) ----

  const handleRetry = () => {
    if (createdWorkspace) void runUploads(createdWorkspace);
  };

  // ---- Reset & close ----

  const resetAndClose = () => {
    setName('');
    setDescription('');
    setQueuedFiles([]);
    setError(null);
    setPickedComputerId(null);
    setPhase('form');
    setCreationStep('uploading');
    setSubmitting(false);
    setFileStatuses({});
    setCurrentUploadProgress(0);
    setCurrentUploadName('');
    setCreatedWorkspace(null);
    setProgressError(null);
    onClose();
  };

  const handleOpenWorkspace = () => {
    const workspace = createdWorkspace;
    resetAndClose();
    if (workspace) onComplete?.(workspace);
  };

  // ---- Computed ----

  const isInProgress = phase === 'progress' && creationStep === 'uploading';
  const canClose = !isInProgress && !submitting;

  const failedCount = Object.values(fileStatuses).filter((s) => s === 'failed').length;
  const doneCount = Object.values(fileStatuses).filter((s) => s === 'done').length;

  if (!isOpen) return null;

  // =========== PROGRESS PHASE ===========
  if (phase === 'progress') {
    return (
      <div className="cwm-overlay">
        <div className="cwm-modal" onClick={(e) => e.stopPropagation()}>
          {/* Header */}
          <div className="cwm-header">
            <h2 className="cwm-title">
              {creationStep === 'done' ? t('workspace.workspaceReady') : t('workspace.uploadingFiles')}
            </h2>
            {canClose && (
              <button className="cwm-close-btn" onClick={handleOpenWorkspace}>
                <X className="h-5 w-5" />
              </button>
            )}
          </div>

          <div className="cwm-progress">
            {/* The upload operation includes preparing its destination. */}
            <div className="cwm-steps">
              <StepRow
                label={t('workspace.uploadingFiles')}
                status={
                  creationStep === 'uploading' ? 'active'
                    : creationStep === 'done' ? (failedCount > 0 ? 'error' : 'done')
                      : 'error'
                }
              />
              <StepRow
                label={t('workspace.ready')}
                status={creationStep === 'done' ? 'done' : 'pending'}
              />
            </div>

            {/* Per-file progress during uploading */}
            {creationStep === 'uploading' && (
              <div className="cwm-upload-detail">
                {queuedFiles.map((file) => {
                  const status = fileStatuses[file.name] || 'pending';
                  const isCurrentlyUploading = status === 'uploading' && currentUploadName === file.name;
                  return (
                    <div key={file.name}>
                      <div className="cwm-upload-file-row">
                        <FileText className="h-4 w-4 cwm-file-icon" />
                        <span className="cwm-upload-file-name">{file.name}</span>
                        <span className={`cwm-upload-file-status cwm-upload-file-status--${status}`}>
                          {status === 'done' ? t('common.done') : status === 'failed' ? t('common.failed') : status === 'uploading' ? `${currentUploadProgress}%` : ''}
                        </span>
                      </div>
                      {isCurrentlyUploading && (
                        <div className="cwm-progress-bar" style={{ marginTop: 4 }}>
                          <div className="cwm-progress-bar-fill" style={{ width: `${currentUploadProgress}%` }} />
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {/* Done summary */}
            {creationStep === 'done' && queuedFiles.length > 0 && (
              <div className="cwm-done-summary">
                <div className="cwm-done-subtitle">
                  {doneCount} file{doneCount !== 1 ? 's' : ''} uploaded
                  {failedCount > 0 && ` · ${failedCount} failed`}
                </div>
              </div>
            )}

            {/* Error */}
            {creationStep === 'error' && progressError && (
              <div className="cwm-error-box">{progressError}</div>
            )}

            {/* Action buttons */}
            <div className="cwm-actions">
              {creationStep === 'done' && (
                <button className="cwm-btn-create" onClick={handleOpenWorkspace}>
                  {t('workspace.openWorkspace')}
                </button>
              )}
              {creationStep === 'error' && (
                <>
                  <button className="cwm-btn-cancel" disabled={submitting} onClick={resetAndClose}>{t('common.cancel')}</button>
                  <button className="cwm-btn-create" onClick={handleRetry}>{t('common.retry')}</button>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // =========== FORM PHASE ===========
  return (
    <div className="cwm-overlay" onClick={() => { if (canClose) resetAndClose(); }}>
      <div className="cwm-modal" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="cwm-header">
          <h2 className="cwm-title">{t('workspace.createNewWorkspace')}</h2>
          <button className="cwm-close-btn" disabled={!canClose} onClick={resetAndClose}>
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit}>
          {/* Name */}
          <div className="cwm-field">
            <label className="cwm-label">
              {t('workspace.workspaceName')} <span className="cwm-label-required">*</span>
            </label>
            <Input
              type="text"
              value={name}
              onChange={(e) => setName(clampWorkspaceName(e.target.value))}
              placeholder={t('workspace.enterWorkspaceName')}
              className="w-full"
              style={{
                backgroundColor: 'var(--color-bg-card)',
                border: '1px solid var(--color-border-muted)',
                color: 'var(--color-text-primary)',
              }}
              autoFocus
            />
          </div>

          <div className="cwm-field">
            <label className="cwm-label">
              {t('common.description')} <span className="cwm-label-optional">{t('common.optional')}</span>
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder={t('workspace.enterWorkspaceDesc')}
              rows={3}
              className="cwm-textarea"
            />
          </div>

          {choosesComputer && (
            <div className="cwm-field">
              <label className="cwm-label" htmlFor={computerFieldId}>
                {t('computer.computerLabel', 'Computer')}
              </label>
              <Select
                id={computerFieldId}
                value={target?.computer_id ?? ''}
                onChange={(e) => setPickedComputerId(e.target.value)}
                disabled={submitting}
              >
                {computers.map((c) => (
                  <option key={c.computer_id} value={c.computer_id} disabled={!takesWorkspaces(c)}>
                    {computerOptionLabel(t, c, sourceOf(c))}
                  </option>
                ))}
              </Select>
              <div className="cwm-field-hint">
                {t(
                  'computer.computerHint',
                  'Workspaces on one computer share its disk and start and stop together.',
                )}
              </div>
            </div>
          )}

          {preparingTarget && (
            <div className="cwm-notice" role="status">
              <span aria-hidden="true" className="cwm-notice-glyph">
                <Loader size={12} className="text-(--color-accent-primary)" />
              </span>
              <span>
                {t(
                  'computer.prepare.blocksWorkspace',
                  "{{name}} is being prepared. You can create a workspace on it once it's ready.",
                  { name: preparingTarget.name },
                )}
              </span>
            </div>
          )}

          {/* File dropzone */}
          <div className="cwm-dropzone-wrapper">
            <div className="cwm-dropzone-label">{t('workspace.files')} <span className="cwm-label-optional">{t('common.optional')}</span></div>
            <div className="cwm-dropzone-sublabel">{t('workspace.filesUploadNote')}</div>
            <div
              className={`cwm-dropzone ${isDragging ? 'cwm-dropzone-active' : ''}`}
              onClick={() => fileInputRef.current?.click()}
              onDragEnter={handleDragEnter}
              onDragLeave={handleDragLeave}
              onDragOver={handleDragOver}
              onDrop={handleDrop}
            >
              <Upload className="h-6 w-6 cwm-dropzone-icon" />
              <div className="cwm-dropzone-text">
                {t('workspace.dragFilesHere')}<span>{t('workspace.clickToBrowse')}</span>
              </div>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                style={{ display: 'none' }}
                onChange={(e) => {
                  if (e.target.files?.length) addFiles(e.target.files);
                  e.target.value = '';
                }}
              />
            </div>

            {/* Queued files */}
            {queuedFiles.length > 0 && (
              <div className="cwm-file-list">
                {queuedFiles.map((file) => (
                  <div key={file.name} className="cwm-file-item">
                    <FileText className="h-4 w-4 cwm-file-icon" />
                    <span className="cwm-file-name">{file.name}</span>
                    <span className="cwm-file-size">{formatBytes(file.size, locale)}</span>
                    <button
                      type="button"
                      className="cwm-file-remove"
                      onClick={() => removeFile(file.name)}
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Error */}
          {error && <div className="cwm-error">{error}</div>}

          {/* Actions */}
          <div className="cwm-actions">
            <button type="button" className="cwm-btn-cancel" disabled={submitting} onClick={resetAndClose}>
              {t('common.cancel')}
            </button>
            <button
              type="submit"
              className="cwm-btn-create"
              disabled={!name.trim() || submitting || !!preparingTarget}
              aria-busy={submitting}
            >
              {submitting ? (
                <>
                  <Loader size={12} className="text-current" />
                  {t('workspace.creating', 'Creating...')}
                </>
              ) : (
                t('common.create')
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

/**
 * Step indicator row for the progress phase
 */
type StepStatus = 'done' | 'active' | 'error' | 'pending';

interface StepRowProps {
  label: string;
  status: StepStatus;
}

function StepRow({ label, status }: StepRowProps) {
  let icon: React.ReactNode;
  let iconClass = '';
  let labelClass = '';

  switch (status) {
    case 'done':
      icon = <CheckCircle2 className="h-5 w-5" />;
      iconClass = 'cwm-step-icon--done';
      break;
    case 'active':
      icon = <Loader size={20} className="text-current" />;
      iconClass = 'cwm-step-icon--active';
      break;
    case 'error':
      icon = <AlertCircle className="h-5 w-5" />;
      iconClass = 'cwm-step-icon--error';
      break;
    default:
      icon = <Circle className="h-5 w-5" />;
      iconClass = 'cwm-step-icon--pending';
      labelClass = 'cwm-step-label--pending';
  }

  return (
    <div className="cwm-step">
      <div className={`cwm-step-icon ${iconClass}`}>{icon}</div>
      <span className={`cwm-step-label ${labelClass}`}>{label}</span>
    </div>
  );
}

export default CreateWorkspaceModal;

/** A machine as the picker lists it: its name, what it runs on where that tells it apart, and whether it is the primary or still preparing. */
function computerOptionLabel(t: TFunction, computer: Computer, source: string | null): string {
  const name = source ? t('computer.nameWithSource', '{{name}} · {{source}}', { name: computer.name, source }) : computer.name;
  const preparing = isComputerPreparing(computer);
  if (computer.is_primary) {
    return preparing
      ? t('computer.primaryPreparingOption', '{{name}} (Primary, Preparing)', { name })
      : t('computer.primaryOption', '{{name}} (Primary)', { name });
  }
  return preparing ? t('computer.preparingOption', '{{name}} (Preparing)', { name }) : name;
}
