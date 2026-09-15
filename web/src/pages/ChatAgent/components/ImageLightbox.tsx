import React, { useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { useBackdropDismiss } from '@/hooks/useDialogA11y';

interface ImageLightboxProps {
  src: string;
  alt?: string;
  open: boolean;
  onClose: () => void;
}

function ImageLightbox({ src, alt, open, onClose }: ImageLightboxProps) {
  const { t } = useTranslation();
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    },
    [onClose]
  );

  useEffect(() => {
    if (!open) return;
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [open, handleKeyDown]);

  const backdrop = useBackdropDismiss<HTMLDivElement>(onClose);

  if (!open) return null;

  // A lightbox is a deliberately dark surface, not a dialog scrim: the close
  // button sits on white at 70% and the image is meant to be the only lit thing
  // on screen, so the ground stays black in both themes rather than following
  // the overlay token, which lightens to 45% black under the light palette.
  return createPortal(
    <div
      className="fixed inset-0 z-[1020] flex items-center justify-center bg-black/90 scrim-in"
      {...backdrop}
    >
      <button
        onClick={onClose}
        className="absolute top-4 right-4 z-10 rounded-full p-2 text-white/70 hover:text-white hover:bg-white/10 transition-colors"
        aria-label={t('common.close')}
      >
        <X className="h-6 w-6" />
      </button>
      <img
        src={src}
        alt={alt}
        className="max-w-[90vw] max-h-[90vh] object-contain rounded-lg"
        // Not for the backdrop, which checks its own target: the lightbox is a
        // portal, so a click on the image would otherwise bubble up the React
        // tree into the chat message that rendered it.
        onClick={(e) => e.stopPropagation()}
      />
    </div>,
    document.body
  );
}

export default ImageLightbox;
