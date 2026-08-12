import React, { useState, useEffect } from 'react';
import { Edit2 } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from '@/components/ui/dialog';

interface RenameThreadModalProps {
  isOpen: boolean;
  currentTitle: string;
  onConfirm: (newTitle: string) => void;
  onCancel: () => void;
  isRenaming: boolean;
  error?: string | null;
}

/**
 * RenameThreadModal Component
 *
 * Modal for renaming a thread.
 * Bottom-sheet on mobile, centered on desktop.
 */
function RenameThreadModal({ isOpen, currentTitle, onConfirm, onCancel, isRenaming, error }: RenameThreadModalProps) {
  const [newTitle, setNewTitle] = useState('');

  // Reset form when modal opens/closes or currentTitle changes
  useEffect(() => {
    if (isOpen) {
      setNewTitle(currentTitle || '');
    } else {
      setNewTitle('');
    }
  }, [isOpen, currentTitle]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (newTitle.trim() && !isRenaming) {
      onConfirm(newTitle.trim());
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => { if (!open) onCancel(); }}>
      <DialogContent
        aria-describedby={undefined}
        style={{
          backgroundColor: 'var(--color-bg-page)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        {/* Header */}
        <div className="flex items-center gap-3">
          <div
            className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: 'var(--color-accent-soft)' }}
          >
            <Edit2 className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} />
          </div>
          <DialogTitle className="text-xl font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            Rename Thread
          </DialogTitle>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <label className="block text-sm font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
              Thread Title
            </label>
            <Input
              type="text"
              value={newTitle}
              onChange={(e) => setNewTitle(e.target.value)}
              placeholder="Enter thread title"
              maxLength={255}
              disabled={isRenaming}
              className="w-full"
              style={{
                backgroundColor: 'var(--color-border-muted)',
                border: '1px solid var(--color-border-muted)',
                color: 'var(--color-text-primary)',
              }}
              autoFocus
            />
            <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
              {newTitle.length}/255 characters
            </p>
          </div>

          {/* Error message */}
          {error && (
            <div className="mb-4 p-3 rounded-md" style={{ backgroundColor: 'rgba(255, 56, 60, 0.1)', border: '1px solid var(--color-border-loss)' }}>
              <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
                {error}
              </p>
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-3 justify-end">
            <button
              type="button"
              onClick={onCancel}
              disabled={isRenaming}
              className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ color: 'var(--color-text-primary)' }}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isRenaming || !newTitle.trim() || newTitle.trim() === currentTitle}
              className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              style={{
                backgroundColor: 'var(--color-btn-primary-bg)',
                color: 'var(--color-btn-primary-text)',
              }}
            >
              {isRenaming ? 'Renaming...' : 'Rename'}
            </button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export default RenameThreadModal;
