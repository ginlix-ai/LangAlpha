import React from 'react';

/**
 * LogoLoading Component
 *
 * A custom animated logo loading spinner with draw → hold → reverse erase animation.
 * Based on the LangAlpha logo design.
 *
 * @param {number} size - The size of the loading animation in pixels (default: 60)
 * @param {string} color - The stroke color of the logo (default: 'white')
 * @param {string} className - Additional CSS classes to apply
 * @param {object} style - Additional inline styles
 */
function LogoLoading({ size = 60, color = 'white', className = '', style = {} }) {
  return (
    <div
      className={`logo-loading-wrap ${className}`}
      style={{
        width: size,
        height: size,
        display: 'inline-block',
        ...style
      }}
    >
      <svg
        width={size}
        height={size}
        viewBox="0 0 60 60"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        style={{
          width: '100%',
          height: '100%',
          filter: 'drop-shadow(0 0 6px rgba(255,255,255,0.2))',
        }}
      >
        <path
          className="logo-loading-path1"
          d="M40.0312 29.6023L49.9852 25.4051C50.5292 25.1758 50.7571 24.5277 50.4765 24.0084L45.6363 15.0496C45.3489 14.5178 44.6591 14.3605 44.1696 14.7153L34.6523 21.6136M40.0312 29.6023L33.933 32.1736C31.7869 33.0785 31.4456 35.9773 33.3229 37.3559L44.168 45.3202C44.6573 45.6795 45.3512 45.5235 45.6397 44.9895L50.5087 35.9776C50.7774 35.4803 50.5808 34.8593 50.0749 34.6072L40.0312 29.6023ZM34.6523 21.6136L30.5854 24.5614C28.7503 25.8916 26.1597 24.7846 25.8525 22.5391L24.1554 10.1356C24.0732 9.53499 24.54 9 25.1461 9H34.7163C35.3048 9 35.766 9.50561 35.7121 10.0916L34.6523 21.6136Z"
          stroke={color}
          strokeWidth="3"
          style={{
            strokeDasharray: 260,
            strokeDashoffset: 260,
            animation: 'logo-loading-draw-reverse 4s ease-in-out infinite',
          }}
        />
        <path
          className="logo-loading-path2"
          d="M35.282 47L35.6587 50.0175C35.7338 50.6188 35.2611 51.1482 34.6551 51.1413L25.1712 51.034C24.5829 51.0273 24.1274 50.5167 24.1878 49.9315L25.1428 40.668C25.2309 39.8127 24.2701 39.2523 23.5691 39.7501L15.853 45.2293C15.3591 45.58 14.6693 45.4146 14.3882 44.8781L9.68991 35.911C9.41644 35.389 9.65127 34.745 10.1965 34.5215L18.1128 31.2775C18.9026 30.9539 18.9499 29.8532 18.1909 29.4629L17.5 29.1076L10.2106 25.3592C9.70888 25.1012 9.51977 24.4795 9.79284 23.9858L14.7222 15.0745C15.0166 14.5423 15.714 14.3939 16.1995 14.7603L18.5 16.4959"
          stroke={color}
          strokeWidth="3"
          strokeLinecap="round"
          style={{
            strokeDasharray: 260,
            strokeDashoffset: 260,
            animation: 'logo-loading-draw-reverse 4s ease-in-out 0.2s infinite',
          }}
        />
      </svg>

      {/* Inline styles for animation */}
      <style>{`
        @keyframes logo-loading-draw-reverse {
          0%   { stroke-dashoffset: 260; }
          30%  { stroke-dashoffset: 0; }
          55%  { stroke-dashoffset: 0; }
          85%  { stroke-dashoffset: 260; }
          100% { stroke-dashoffset: 260; }
        }
      `}</style>
    </div>
  );
}

export default LogoLoading;
