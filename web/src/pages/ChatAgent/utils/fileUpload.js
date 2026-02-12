/**
 * File upload utilities for multimodal input
 */

export const ACCEPTED_IMAGE_TYPES = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
export const ACCEPTED_PDF_TYPES = ['application/pdf'];
export const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
export const MAX_FILES = 5;

/**
 * Convert a File to a base64 data URL
 * @param {File} file
 * @returns {Promise<string>} data URL string
 */
export function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error(`Failed to read file: ${file.name}`));
    reader.readAsDataURL(file);
  });
}

/**
 * Convert attachments array to ImageContext format for additional_context
 * @param {Array<{file: File, dataUrl: string, type: string}>} attachments
 * @returns {Array<{type: string, data: string, description: string}>}
 */
export function attachmentsToImageContexts(attachments) {
  return attachments.map((a) => ({
    type: 'image',
    data: a.dataUrl,
    description: a.file.name,
  }));
}

/**
 * Validate a file for upload
 * @param {File} file
 * @returns {{ valid: boolean, error?: string }}
 */
export function validateFile(file) {
  const allAccepted = [...ACCEPTED_IMAGE_TYPES, ...ACCEPTED_PDF_TYPES];
  if (!allAccepted.includes(file.type)) {
    return { valid: false, error: `Unsupported file type: ${file.type || 'unknown'}` };
  }
  if (file.size > MAX_FILE_SIZE) {
    return { valid: false, error: `File too large: ${file.name} (max 10MB)` };
  }
  return { valid: true };
}
