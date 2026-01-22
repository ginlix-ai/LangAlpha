/**
 * MSW Configuration
 * 
 * Set VITE_USE_MSW=true in your .env file to enable Mock Service Worker
 * Set VITE_USE_MSW=false or leave it unset to use the real backend
 */

export const shouldUseMSW = () => {
  // Check environment variable, default to false (use real backend)
  return import.meta.env.VITE_USE_MSW === 'true';
};
