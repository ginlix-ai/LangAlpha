/**
 * File upload utilities for multimodal input
 */

export const ACCEPTED_IMAGE_TYPES = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
export const ACCEPTED_PDF_TYPES = ['application/pdf'];
export const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
export const MAX_FILES = 5;

export interface Attachment {
  file: File;
  dataUrl: string | null;
  type: string;
}

export interface ImageContext {
  type: string;
  data: string;
  description: string;
}

export interface FileValidationResult {
  valid: boolean;
  error?: string;
}

/**
 * Convert a File to a base64 data URL
 */
export function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(new Error(`Failed to read file: ${file.name}`));
    reader.readAsDataURL(file);
  });
}

/**
 * Convert attachments to additional_context format with accurate type tags.
 * Images → "image", PDFs → "pdf", everything else → "file".
 */
export function attachmentsToContexts(attachments: Attachment[]): ImageContext[] {
  return attachments
    .filter((a) => a.dataUrl != null)
    .map((a) => ({
      type: a.type.startsWith('image/') ? 'image'
          : a.type === 'application/pdf' ? 'pdf'
          : 'file',
      data: a.dataUrl!,
      description: a.file.name,
    }));
}

/**
 * Validate a file for upload.
 * When flashOnly is true, only images and PDFs are accepted (Flash mode).
 * Otherwise any file type is accepted (PTC mode).
 */
export function validateFile(file: File, flashOnly = false): FileValidationResult {
  if (flashOnly) {
    const allAccepted = [...ACCEPTED_IMAGE_TYPES, ...ACCEPTED_PDF_TYPES];
    if (!allAccepted.includes(file.type)) {
      return { valid: false, error: `Unsupported file type: ${file.type || 'unknown'}` };
    }
  }
  if (file.size > MAX_FILE_SIZE) {
    return { valid: false, error: `File too large: ${file.name} (max 10MB)` };
  }
  return { valid: true };
}
