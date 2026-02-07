import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import { getInfoFlowDetail } from '../Dashboard/utils/api';
import './DetailPage.css';

function DetailPage() {
  const { indexNumber } = useParams();
  const navigate = useNavigate();
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchDetail() {
      setLoading(true);
      setError(null);
      const data = await getInfoFlowDetail(indexNumber);
      if (data) {
        setDetail(data);
      } else {
        setError('Failed to load content.');
      }
      setLoading(false);
    }
    if (indexNumber) fetchDetail();
  }, [indexNumber]);

  const handleBack = () => navigate('/dashboard');
  const handlePrev = () => {
    if (detail?.previousResult?.indexNumber) {
      navigate(`/detail/${detail.previousResult.indexNumber}`);
    }
  };
  const handleNext = () => {
    if (detail?.nextResult?.indexNumber) {
      navigate(`/detail/${detail.nextResult.indexNumber}`);
    }
  };

  if (loading) {
    return (
      <div className="flex flex-col gap-4 p-8 max-w-4xl mx-auto animate-pulse">
        <div className="h-8 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '30%' }} />
        <div className="h-6 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '70%' }} />
        <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '100%', marginTop: '16px' }} />
        <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '90%' }} />
        <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '95%' }} />
        <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '80%' }} />
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div className="flex flex-col items-center justify-center gap-4 p-8" style={{ color: 'var(--color-text-secondary)' }}>
        <p>{error || 'Content not found.'}</p>
        <button
          onClick={handleBack}
          className="flex items-center gap-2 px-4 py-2 rounded-md cursor-pointer transition-colors"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)', border: '1px solid var(--color-border-default)' }}
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Dashboard
        </button>
      </div>
    );
  }

  return (
    <div className="detail-page flex flex-col gap-6 p-6 max-w-4xl mx-auto" style={{ color: 'var(--color-text-primary)' }}>
      {/* Back button */}
      <button
        onClick={handleBack}
        className="flex items-center gap-2 self-start px-3 py-1.5 rounded-md cursor-pointer transition-colors"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)', color: 'var(--color-text-primary)' }}
      >
        <ArrowLeft className="w-4 h-4" />
        <span className="text-sm">Back</span>
      </button>

      {/* Title & metadata */}
      <div className="flex flex-col gap-3">
        <h1 className="text-2xl font-bold" style={{ lineHeight: '1.3' }}>
          {detail.title}
        </h1>
        <div className="flex items-center gap-3 flex-wrap">
          {detail.event_timestamp && (
            <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {new Date(detail.event_timestamp).toLocaleString()}
            </span>
          )}
          {detail.market_type && (
            <span className="text-xs px-2 py-0.5 rounded-md font-medium" style={{ backgroundColor: 'var(--color-bg-tag)', color: 'var(--color-text-primary)' }}>
              {detail.market_type}
            </span>
          )}
          {detail.event_type && (
            <span className="text-xs px-2 py-0.5 rounded-md" style={{ backgroundColor: 'var(--color-bg-tag)', color: 'var(--color-text-primary)', opacity: 0.8 }}>
              {detail.event_type.replace(/_/g, ' ')}
            </span>
          )}
        </div>
        {detail.tags && detail.tags.length > 0 && (
          <div className="flex items-center gap-1.5 flex-wrap">
            {detail.tags.map((tag, i) => (
              <span
                key={i}
                className="text-xs px-2 py-0.5 rounded-full"
                style={{ backgroundColor: 'var(--color-bg-tag)', color: 'var(--color-text-primary)', opacity: 0.7 }}
              >
                #{tag}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Summary */}
      {detail.summary && (
        <div className="rounded-md p-4" style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)' }}>
          <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)', lineHeight: '1.7' }}>
            {detail.summary}
          </p>
        </div>
      )}

      {/* Images */}
      {detail.images && detail.images.length > 0 && (
        <div className="flex gap-3 overflow-x-auto pb-2">
          {detail.images.map((img, i) => (
            <img
              key={i}
              src={typeof img === 'string' ? img : img.url}
              alt={typeof img === 'string' ? `Image ${i + 1}` : (img.description || `Image ${i + 1}`)}
              className="rounded-md flex-shrink-0"
              style={{ maxHeight: '280px', objectFit: 'cover', border: '1px solid var(--color-border-default)' }}
              onError={(e) => { e.target.style.display = 'none'; }}
            />
          ))}
        </div>
      )}

      {/* Markdown Content */}
      <article className="detail-content">
        <ReactMarkdown>{detail.content}</ReactMarkdown>
      </article>

      {/* Citations / References */}
      {detail.citations && detail.citations.length > 0 && (
        <div className="flex flex-col gap-3" style={{ borderTop: '1px solid var(--color-border-default)', paddingTop: '20px' }}>
          <h3 className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>References</h3>
          <div className="flex flex-col gap-2">
            {detail.citations.map((c, i) => (
              <div key={i} className="flex items-start gap-3 text-xs rounded-md p-2.5" style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
                <span className="flex-shrink-0 font-mono font-medium" style={{ color: 'var(--color-text-secondary)', minWidth: '20px' }}>
                  [{c.index ?? i}]
                </span>
                <div className="flex flex-col gap-0.5 min-w-0">
                  {c.url && c.url !== 'internal' ? (
                    <a
                      href={c.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 hover:underline"
                      style={{ color: 'var(--color-accent-primary)' }}
                    >
                      <span className="truncate">{c.title || c.url}</span>
                      <ExternalLink className="w-3 h-3 flex-shrink-0" />
                    </a>
                  ) : (
                    <span style={{ color: 'var(--color-text-primary)' }}>{c.title || `Source ${i + 1}`}</span>
                  )}
                  <div className="flex items-center gap-2" style={{ color: 'var(--color-text-secondary)' }}>
                    {c.source && <span>{c.source}</span>}
                    {c.date && <span>{c.date}</span>}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Prev / Next navigation */}
      <div
        className="flex items-center justify-between"
        style={{ borderTop: '1px solid var(--color-border-default)', paddingTop: '16px', marginTop: '8px' }}
      >
        {detail.previousResult ? (
          <button
            onClick={handlePrev}
            className="flex items-center gap-2 px-3 py-2 rounded-md cursor-pointer transition-colors"
            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)', color: 'var(--color-text-primary)', maxWidth: '45%' }}
          >
            <ChevronLeft className="w-4 h-4 flex-shrink-0" />
            <span className="text-sm truncate">{detail.previousResult.title || 'Previous'}</span>
          </button>
        ) : <div />}
        {detail.nextResult ? (
          <button
            onClick={handleNext}
            className="flex items-center gap-2 px-3 py-2 rounded-md cursor-pointer transition-colors"
            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)', color: 'var(--color-text-primary)', maxWidth: '45%' }}
          >
            <span className="text-sm truncate">{detail.nextResult.title || 'Next'}</span>
            <ChevronRight className="w-4 h-4 flex-shrink-0" />
          </button>
        ) : <div />}
      </div>
    </div>
  );
}

export default DetailPage;
