import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

/**
 * TextMessageContent Component
 * 
 * Renders text content from message_chunk events with content_type: text.
 * Supports markdown formatting including bold, italic, lists, code blocks, etc.
 * 
 * @param {Object} props
 * @param {string} props.content - The text content to display (supports markdown)
 * @param {boolean} props.isStreaming - Whether the message is currently streaming
 * @param {boolean} props.hasError - Whether the message has an error
 */
function TextMessageContent({ content, isStreaming, hasError }) {
  if (!content && !isStreaming) {
    return null;
  }

  return (
    <div className="text-sm break-words prose prose-invert prose-sm max-w-none">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Customize paragraph styling
          p: ({ node, ...props }) => (
            <p className="mb-2 last:mb-0" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize heading styling
          h1: ({ node, ...props }) => (
            <h1 className="text-lg font-bold mt-4 mb-2 first:mt-0" style={{ color: '#FFFFFF' }} {...props} />
          ),
          h2: ({ node, ...props }) => (
            <h2 className="text-base font-bold mt-3 mb-2 first:mt-0" style={{ color: '#FFFFFF' }} {...props} />
          ),
          h3: ({ node, ...props }) => (
            <h3 className="text-sm font-bold mt-2 mb-1 first:mt-0" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize list styling
          ul: ({ node, ...props }) => (
            <ul className="list-disc list-inside mb-2 ml-4 space-y-1" style={{ color: '#FFFFFF' }} {...props} />
          ),
          ol: ({ node, ...props }) => (
            <ol className="list-decimal list-inside mb-2 ml-4 space-y-1" style={{ color: '#FFFFFF' }} {...props} />
          ),
          li: ({ node, ...props }) => (
            <li className="text-sm" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize strong (bold) styling
          strong: ({ node, ...props }) => (
            <strong className="font-semibold" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize emphasis (italic) styling
          em: ({ node, ...props }) => (
            <em className="italic" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize code styling
          // In react-markdown v9, the inline prop was removed
          // We detect inline code by checking if className contains 'language-' (block code has language prefix)
          code: ({ node, className, children, ...props }) => {
            // In v9, inline code is NOT inside a <pre> element, so it won't have language- prefix
            const isBlock = /language-/.test(className || '');

            if (!isBlock) {
              // Inline code styling
              return (
                <code
                  className="px-1.5 py-0.5 rounded text-xs font-mono"
                  style={{
                    backgroundColor: 'rgba(97, 85, 245, 0.2)',
                    color: '#6155F5',
                    border: '1px solid rgba(97, 85, 245, 0.3)',
                  }}
                  {...props}
                >
                  {children}
                </code>
              );
            }

            // Block code styling
            return (
              <code
                className="block p-3 rounded text-xs font-mono overflow-x-auto"
                style={{
                  backgroundColor: 'rgba(0, 0, 0, 0.3)',
                  color: '#FFFFFF',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                }}
                {...props}
              >
                {children}
              </code>
            );
          },
          // Customize pre (code block) styling
          pre: ({ node, ...props }) => (
            <pre className="mb-2 overflow-x-auto" {...props} />
          ),
          // Customize blockquote styling
          blockquote: ({ node, ...props }) => (
            <blockquote
              className="border-l-4 pl-4 my-2 italic"
              style={{
                borderColor: '#6155F5',
                color: '#FFFFFF',
                opacity: 0.8,
              }}
              {...props}
            />
          ),
          // Customize link styling
          a: ({ node, ...props }) => (
            <a
              className="underline hover:opacity-80 transition-opacity"
              style={{ color: '#6155F5' }}
              target="_blank"
              rel="noopener noreferrer"
              {...props}
            />
          ),
          // Customize horizontal rule
          hr: ({ node, ...props }) => (
            <hr className="my-3 border-0" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }} {...props} />
          ),
          // Customize table styling
          table: ({ node, ...props }) => (
            <div className="overflow-x-auto my-3">
              <table
                className="border-collapse w-full text-sm"
                style={{
                  border: '1px solid rgba(255, 255, 255, 0.2)',
                }}
                {...props}
              />
            </div>
          ),
          thead: ({ node, ...props }) => (
            <thead
              style={{
                backgroundColor: 'rgba(97, 85, 245, 0.2)',
              }}
              {...props}
            />
          ),
          tbody: ({ node, ...props }) => (
            <tbody {...props} />
          ),
          tr: ({ node, ...props }) => (
            <tr
              className="border-b"
              style={{
                borderColor: 'rgba(255, 255, 255, 0.1)',
              }}
              {...props}
            />
          ),
          th: ({ node, ...props }) => (
            <th
              className="px-4 py-2 text-left font-semibold"
              style={{
                color: '#FFFFFF',
                borderRight: '1px solid rgba(255, 255, 255, 0.1)',
              }}
              {...props}
            />
          ),
          td: ({ node, ...props }) => (
            <td
              className="px-4 py-2"
              style={{
                color: '#FFFFFF',
                opacity: 0.9,
                borderRight: '1px solid rgba(255, 255, 255, 0.1)',
              }}
              {...props}
            />
          ),
        }}
      >
        {content || (isStreaming ? '...' : '')}
      </ReactMarkdown>
    </div>
  );
}

export default TextMessageContent;
