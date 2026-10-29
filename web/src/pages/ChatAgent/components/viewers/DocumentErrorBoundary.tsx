import React from 'react';

interface DocumentErrorBoundaryProps {
  fallback?: React.ReactNode;
  children: React.ReactNode;
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
  }

  override render() {
    if (this.state.hasError) {
      return this.props.fallback || null;
    }
    return this.props.children;
  }
}

export default DocumentErrorBoundary;
