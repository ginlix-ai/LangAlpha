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
    <div className="text-base leading-[1.5] break-words max-w-none overflow-hidden" style={{ color: '#FFFFFF' }}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Customize paragraph styling
          p: ({ node, ...props }) => (
            <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize heading styling
          h1: ({ node, ...props }) => (
            <h1 className="py-[3px] font-semibold first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.5em', lineHeight: '1.3', marginTop: '1.4em', marginBottom: '1px' }} {...props} />
          ),
          h2: ({ node, ...props }) => (
            <h2 className="py-[3px] font-semibold first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.5em', lineHeight: '1.3', marginTop: '1.4em', marginBottom: '1px' }} {...props} />
          ),
          h3: ({ node, ...props }) => (
            <h3 className="py-[3px] font-semibold first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.25em', lineHeight: '1.3', marginTop: '1em', marginBottom: '1px' }} {...props} />
          ),
          // Customize list styling
          ul: ({ node, ...props }) => (
            <ul className="list-disc ml-6 my-2" style={{ color: '#FFFFFF' }} {...props} />
          ),
          ol: ({ node, ...props }) => (
            <ol className="list-decimal ml-6 my-2" style={{ color: '#FFFFFF' }} {...props} />
          ),
          li: ({ node, ...props }) => (
            <li className="ps-[2px] break-words" style={{ color: '#FFFFFF' }} {...props} />
          ),
          // Customize strong (bold) styling
          strong: ({ node, ...props }) => (
            <strong className="font-[600]" style={{ color: '#FFFFFF' }} {...props} />
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
              // Inline code styling - 无背景无边框
              return (
                <code
                  className="font-mono"
                  style={{
                    color: '#abb2bf',
                    fontSize: '0.875rem',
                    lineHeight: '1.25rem',
                  }}
                  {...props}
                >
                  {children}
                </code>
              );
            }

            // Block code - 不加额外样式，由 pre 容器控制
            return (
              <code
                className="font-mono"
                style={{
                  color: '#abb2bf',
                  fontSize: '0.875rem',
                  lineHeight: '1.25rem',
                }}
                {...props}
              >
                {children}
              </code>
            );
          },
          // Customize pre (code block) styling - 圆角卡片容器，自适应内容宽度
          pre: ({ node, ...props }) => (
            <div className="py-[4px]">
              <pre
                className="rounded-lg overflow-x-auto inline-block"
                style={{
                  backgroundColor: '#282c34',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  padding: '1rem',
                  maxWidth: '100%',
                  minWidth: 0,
                  margin: 0,
                }}
                {...props}
              />
            </div>
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
          // Customize table styling - 自适应内容宽度
          table: ({ node, ...props }) => (
            <div className="pt-[8px] pb-[18px]">
              <div className="overflow-x-auto inline-block border rounded-lg" style={{ borderColor: 'rgba(255, 255, 255, 0.1)', maxWidth: '100%' }}>
                <table
                  className="m-0 table-auto border-collapse"
                  {...props}
                />
              </div>
            </div>
          ),
          thead: ({ node, ...props }) => (
            <thead {...props} />
          ),
          tbody: ({ node, ...props }) => (
            <tbody {...props} />
          ),
          tr: ({ node, ...props }) => (
            <tr {...props} />
          ),
          th: ({ node, ...props }) => (
            <th
              className="text-left align-top first:border-s-0 last:border-e-0"
              style={{
                backgroundColor: 'rgba(255, 255, 255, 0.05)',
                borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
                borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
                color: '#FFFFFF',
                fontSize: '0.875rem',
                fontWeight: 500,
                padding: '7px 9px',
              }}
              {...props}
            />
          ),
          td: ({ node, ...props }) => (
            <td
              className="text-left first:border-s-0 last:border-e-0"
              style={{
                borderTop: '1px solid rgba(255, 255, 255, 0.1)',
                borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
                color: '#FFFFFF',
                fontSize: '0.875rem',
                padding: '8px 14px',
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
