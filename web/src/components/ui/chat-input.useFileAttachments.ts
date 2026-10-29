import { useCallback, useEffect, useRef, useState } from 'react';
import type { FileAttachment, ReadyAttachment } from './chat-input.types';
import { MAX_FILES, MAX_FILE_SIZE } from './chat-input.helpers';

/** Attachment handling for the chat composer: validation (count and size),
 * dataUrl reading with previews, drag & drop, and paste capture. A `disabled`
 * composer takes no files, however they come: its drag handlers refuse
 * instead of attach, and a pick or paste that still arrives is dropped. */
export function useFileAttachments({ disabled = false }: { disabled?: boolean } = {}) {
  const [attachedFiles, setAttachedFiles] = useState<FileAttachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);

  const handleFiles = useCallback((newFilesList: FileList | File[]) => {
    if (disabled) return;
    const currentCount = attachedFiles.length;
    const fileArray = Array.from(newFilesList);

    const validFiles = [];
    for (const file of fileArray) {
      if (currentCount + validFiles.length >= MAX_FILES) break;
      if (file.size > MAX_FILE_SIZE) continue;
      validFiles.push(file);
    }

    const newFiles: FileAttachment[] = validFiles.map((file) => {
      const isImage = file.type.startsWith('image/') || /\.(jpg|jpeg|png|gif|webp)$/i.test(file.name);
      return {
        id: Math.random().toString(36).substr(2, 9),
        file,
        type: file.type || (isImage ? 'image/png' : 'application/octet-stream'),
        preview: isImage ? URL.createObjectURL(file) : null,
        uploadStatus: 'pending' as const,
        dataUrl: null,
      };
    });

    if (newFiles.length === 0) return;

    setAttachedFiles((prev) => [...prev, ...newFiles]);

    newFiles.forEach((f) => {
      const reader = new FileReader();
      reader.onload = () => {
        setAttachedFiles((prev) =>
          prev.map((p) =>
            p.id === f.id ? { ...p, uploadStatus: 'complete' as const, dataUrl: reader.result as string } : p
          )
        );
      };
      reader.onerror = () => {
        setAttachedFiles((prev) => prev.filter((p) => p.id !== f.id));
      };
      reader.readAsDataURL(f.file);
    });
  }, [attachedFiles.length, disabled]);

  // Files a send took that are coming back to the composer, already read.
  // The send revoked their previews, so each gets a new one.
  const restoreFiles = useCallback((files: ReadyAttachment[]) => {
    if (files.length === 0) return;
    const restored: FileAttachment[] = files.map((f) => ({
      id: Math.random().toString(36).substr(2, 9),
      file: f.file,
      type: f.type,
      preview: f.type.startsWith('image/') ? URL.createObjectURL(f.file) : null,
      uploadStatus: 'complete' as const,
      dataUrl: f.dataUrl,
    }));
    setAttachedFiles((prev) => [...prev, ...restored].slice(0, MAX_FILES));
  }, []);

  const removeFile = useCallback((id: string) => {
    setAttachedFiles((prev) => {
      const file = prev.find((f) => f.id === id);
      if (file?.preview) URL.revokeObjectURL(file.preview);
      return prev.filter((f) => f.id !== id);
    });
  }, []);

  // Drag & Drop. A disabled composer refuses a drag over it (shown as such,
  // and kept from reaching the page, which would open the file in its place)
  // and a drop on it is not attached to a message that cannot be sent.
  //
  // The drag is over the composer while some element in it has been entered
  // and not left. Crossing from one child into another fires the child's
  // dragenter before the last one's dragleave, and both bubble here, so a
  // dragleave alone would hide the overlay at every inner edge and the next
  // dragover would show it again. An element removed under the pointer (the
  // send cell's Stop as the turn ends) is never left, so it stops counting
  // once it is out of the document.
  const dragEntered = useRef(new Set<Node>());
  const endDrag = useCallback(() => {
    dragEntered.current.clear();
    setIsDragging(false);
  }, []);
  const onDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (e.target instanceof Node) dragEntered.current.add(e.target);
    if (!disabled) setIsDragging(true);
  }, [disabled]);
  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (disabled) e.dataTransfer.dropEffect = 'none';
  }, [disabled]);
  const onDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const entered = dragEntered.current;
    if (e.target instanceof Node) entered.delete(e.target);
    for (const node of entered) if (!node.isConnected) entered.delete(node);
    if (entered.size === 0) setIsDragging(false);
  }, []);
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    endDrag();
    if (e.dataTransfer.files) handleFiles(e.dataTransfer.files);
  }, [endDrag, handleFiles]);

  // A drag that ends without leaving the composer the way it came in (dropped
  // elsewhere on the page, or cancelled) still ends here.
  useEffect(() => {
    if (!isDragging) return;
    window.addEventListener('dragend', endDrag);
    window.addEventListener('drop', endDrag);
    return () => {
      window.removeEventListener('dragend', endDrag);
      window.removeEventListener('drop', endDrag);
    };
  }, [isDragging, endDrag]);

  // Paste Handling
  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    const items = e.clipboardData.items;
    const pastedFiles = [];
    for (let i = 0; i < items.length; i++) {
      if (items[i].kind === 'file') {
        const file = items[i].getAsFile();
        if (file) pastedFiles.push(file);
      }
    }
    if (pastedFiles.length > 0) {
      e.preventDefault();
      handleFiles(pastedFiles);
    }
  }, [handleFiles]);

  return { attachedFiles, setAttachedFiles, isDragging, handleFiles, restoreFiles, removeFile, onDragEnter, onDragOver, onDragLeave, onDrop, handlePaste };
}
