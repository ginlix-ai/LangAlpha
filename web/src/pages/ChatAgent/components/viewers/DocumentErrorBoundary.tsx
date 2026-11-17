import React from 'react';

interface DocumentErrorBoundaryProps {
  fallback?: React.ReactNode;
  children: React.ReactNode;
  /** Hears whether the fallback stands in for the viewer, which takes what the viewer drew with it (its comment layer). */
  onFailedChange?: (failed: boolean) => void;
}

interface DocumentErrorBoundaryState {
  hasError: boolean;
}

class DocumentErrorBoundary extends React.Component<DocumentErrorBoundaryProps, DocumentErrorBoundaryState> {
  constructor(props: DocumentErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(): DocumentErrorBoundaryState {
    return { hasError: true };
  }

  override componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('[DocumentErrorBoundary]', error, info);
    this.props.onFailedChange?.(true);
  }

  override componentWillUnmount() {
    if (this.state.hasError) this.props.onFailedChange?.(false);
  }

  override render() {
    if (this.state.hasError) {
      return this.props.fallback || null;
    }
    return this.props.children;
  }
}

export default DocumentErrorBoundary;
