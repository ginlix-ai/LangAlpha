import React, { useState, useEffect, useCallback } from 'react';
import { ArrowLeft, X, FileText, FileImage, File, RefreshCw, Download } from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { listWorkspaceFiles, readWorkspaceFile, downloadWorkspaceFile, triggerFileDownload } from '../utils/api';
import ReactMarkdown from 'react-markdown';
import './FilePanel.css';

function getFileIcon(fileName) {
  const ext = fileName.split('.').pop()?.toLowerCase();
  if (['md', 'txt', 'csv', 'json', 'py', 'js', 'html'].includes(ext)) return FileText;
  if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext)) return FileImage;
  return File;
}

function getFileExtension(fileName) {
  return fileName.split('.').pop()?.toLowerCase() || '';
}

function FilePanel({ workspaceId, onClose, targetFile, onTargetFileHandled }) {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // File detail view state
  const [selectedFile, setSelectedFile] = useState(null);
  const [fileContent, setFileContent] = useState(null);
  const [fileMime, setFileMime] = useState(null);
  const [fileLoading, setFileLoading] = useState(false);

  const fetchFiles = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await listWorkspaceFiles(workspaceId, 'results');
      setFiles(data.files || []);
    } catch (err) {
      console.error('[FilePanel] Failed to list files:', err);
      setError(err?.response?.status === 503 ? 'Sandbox not available' : 'Failed to load files');
      setFiles([]);
    } finally {
      setLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchFiles();
  }, [fetchFiles]);

  // Cleanup blob URL on unmount to prevent memory leaks
  useEffect(() => {
    return () => {
      if (fileMime === 'image' && fileContent) {
        URL.revokeObjectURL(fileContent);
      }
    };
  }, [fileContent, fileMime]);

  // Auto-open file when targetFile prop changes (from chat tool call click)
  useEffect(() => {
    if (targetFile) {
      handleFileClick(targetFile);
      onTargetFileHandled?.();
    }
  }, [targetFile]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleFileClick = async (filePath) => {
    const ext = getFileExtension(filePath);

    // Binary files
    if (['pdf', 'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'xlsx', 'docx', 'zip'].includes(ext)) {
      // For images, show inline via blob URL
      if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext)) {
        // Revoke previous blob URL before loading new image
        if (fileMime === 'image' && fileContent) {
          URL.revokeObjectURL(fileContent);
        }
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('image');
        try {
          const blobUrl = await downloadWorkspaceFile(workspaceId, filePath);
          setFileContent(blobUrl);
        } catch (err) {
          console.error('[FilePanel] Failed to download image:', err);
          setFileContent(null);
          setFileMime('text/plain');
          setFileContent('Error: Failed to load image');
        } finally {
          setFileLoading(false);
        }
        return;
      }
      // For other binary files, trigger download
      try {
        await triggerFileDownload(workspaceId, filePath);
      } catch (err) {
        console.error('[FilePanel] Failed to download file:', err);
      }
      return;
    }

    // Text files - read content
    setSelectedFile(filePath);
    setFileLoading(true);
    try {
      const data = await readWorkspaceFile(workspaceId, filePath);
      setFileContent(data.content || '');
      setFileMime(data.mime || 'text/plain');
    } catch (err) {
      console.error('[FilePanel] Failed to read file:', err);
      setFileContent('Error: Failed to load file content');
      setFileMime('text/plain');
    } finally {
      setFileLoading(false);
    }
  };

  const handleBack = () => {
    if (fileMime === 'image' && fileContent) {
      URL.revokeObjectURL(fileContent);
    }
    setSelectedFile(null);
    setFileContent(null);
    setFileMime(null);
  };

  const fileName = selectedFile?.split('/').pop() || '';

  return (
    <div className="file-panel">
      {/* Header */}
      <div className="file-panel-header">
        <div className="flex items-center gap-2 min-w-0">
          {selectedFile && (
            <button onClick={handleBack} className="file-panel-icon-btn" title="Back to file list">
              <ArrowLeft className="h-4 w-4" />
            </button>
          )}
          <span className="text-sm font-semibold truncate" style={{ color: '#FFFFFF' }}>
            {selectedFile ? fileName : 'Workspace Files'}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {!selectedFile && (
            <button onClick={fetchFiles} className="file-panel-icon-btn" title="Refresh">
              <RefreshCw className={`h-3.5 w-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
          )}
          {selectedFile && (
            <button
              onClick={async () => {
                try {
                  await triggerFileDownload(workspaceId, selectedFile);
                } catch (err) {
                  console.error('[FilePanel] Download failed:', err);
                }
              }}
              className="file-panel-icon-btn"
              title="Download"
            >
              <Download className="h-3.5 w-3.5" />
            </button>
          )}
          <button onClick={onClose} className="file-panel-icon-btn" title="Close">
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Content */}
      <ScrollArea className="file-panel-content">
        {selectedFile ? (
          // File Detail View
          <div className="p-4">
            {fileLoading ? (
              <div className="flex items-center justify-center py-12">
                <RefreshCw className="h-5 w-5 animate-spin" style={{ color: 'rgba(255,255,255,0.5)' }} />
              </div>
            ) : fileMime === 'image' ? (
              <img src={fileContent} alt={fileName} className="max-w-full rounded" />
            ) : fileMime?.includes('markdown') || getFileExtension(selectedFile) === 'md' ? (
              <div className="file-panel-markdown prose prose-invert prose-sm max-w-none">
                <ReactMarkdown>{fileContent}</ReactMarkdown>
              </div>
            ) : (
              <pre className="file-panel-code">{fileContent}</pre>
            )}
          </div>
        ) : (
          // File List View
          <div className="py-1">
            {loading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="file-panel-item animate-pulse">
                  <div className="h-4 w-4 rounded" style={{ backgroundColor: 'rgba(255,255,255,0.1)' }} />
                  <div className="h-4 flex-1 rounded" style={{ backgroundColor: 'rgba(255,255,255,0.1)', width: `${50 + i * 10}%` }} />
                </div>
              ))
            ) : error ? (
              <div className="px-4 py-8 text-center">
                <p className="text-sm" style={{ color: 'rgba(255,255,255,0.5)' }}>{error}</p>
              </div>
            ) : files.length === 0 ? (
              <div className="px-4 py-8 text-center">
                <p className="text-sm" style={{ color: 'rgba(255,255,255,0.5)' }}>No files yet</p>
              </div>
            ) : (
              files.map((filePath) => {
                const name = filePath.split('/').pop();
                const Icon = getFileIcon(name);
                return (
                  <div
                    key={filePath}
                    className="file-panel-item"
                    onClick={() => handleFileClick(filePath)}
                  >
                    <Icon className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255,255,255,0.5)' }} />
                    <span className="text-sm truncate" style={{ color: '#FFFFFF' }}>{name}</span>
                  </div>
                );
              })
            )}
          </div>
        )}
      </ScrollArea>
    </div>
  );
}

export default FilePanel;