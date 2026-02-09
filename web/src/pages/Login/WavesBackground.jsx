import React, { useRef, useEffect } from 'react';
import './WavesBackground.css';

/**
 * WavesBackground - Wavy lines grid driven by mouse/touch only (no noise).
 * Lines bend toward the cursor; grid resizes with the window.
 * Colors: set --login-waves-bg and --login-waves-stroke in LoginPage.css (see comments there).
 */
function WavesBackground() {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const rafRef = useRef(null);
  const stateRef = useRef({
    bounding: null,
    mouse: { x: 0, y: 0, lx: 0, ly: 0, sx: 0, sy: 0, v: 0, vs: 0, a: 0 },
    lines: [],
    paths: [],
  });

  useEffect(() => {
    const container = containerRef.current;
    const svg = svgRef.current;
    if (!container || !svg) return;

    const state = stateRef.current;

    function setSize() {
      const rect = container.getBoundingClientRect();
      state.bounding = { width: rect.width, height: rect.height, left: rect.left, top: rect.top };
      svg.setAttribute('width', state.bounding.width);
      svg.setAttribute('height', state.bounding.height);
      svg.setAttribute('viewBox', `0 0 ${state.bounding.width} ${state.bounding.height}`);
    }

    function setLines() {
      const { width, height } = state.bounding;
      if (!width || !height) return;

      state.lines = [];
      state.paths.forEach((path) => path.remove());
      state.paths = [];

      const xGap = 10;
      const yGap = 32;
      const oWidth = width + 200;
      const oHeight = height + 30;
      const totalLines = Math.ceil(oWidth / xGap);
      const totalPoints = Math.ceil(oHeight / yGap);
      const xStart = (width - xGap * totalLines) / 2;
      const yStart = (height - yGap * totalPoints) / 2;

      for (let i = 0; i <= totalLines; i++) {
        const points = [];
        for (let j = 0; j <= totalPoints; j++) {
          points.push({
            x: xStart + xGap * i,
            y: yStart + yGap * j,
            cursor: { x: 0, y: 0, vx: 0, vy: 0 },
          });
        }
        state.lines.push(points);
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('class', 'waves-bg__line');
        svg.appendChild(path);
        state.paths.push(path);
      }

      // Clamp mouse to new bounds after resize
      const { mouse } = state;
      if (state.bounding.width && state.bounding.height) {
        mouse.sx = Math.max(0, Math.min(width, mouse.sx));
        mouse.sy = Math.max(0, Math.min(height, mouse.sy));
        mouse.x = Math.max(0, Math.min(width, mouse.x));
        mouse.y = Math.max(0, Math.min(height, mouse.y));
        mouse.lx = mouse.x;
        mouse.ly = mouse.y;
      }
    }

    function updateMousePosition(x, y) {
      const { mouse } = state;
      const rect = container.getBoundingClientRect();
      mouse.x = x - rect.left;
      mouse.y = y - rect.top;
    }

    function movePoints() {
      const { lines, mouse } = state;
      lines.forEach((points) => {
        points.forEach((p) => {
          const dx = p.x - mouse.sx;
          const dy = p.y - mouse.sy;
          const d = Math.hypot(dx, dy);
          const l = Math.max(175, mouse.vs);

          if (d < l) {
            const f = 1 - d / l;
            p.cursor.vx += Math.cos(mouse.a) * f * mouse.vs * 0.08;
            p.cursor.vy += Math.sin(mouse.a) * f * mouse.vs * 0.08;
          }

          p.cursor.vx += (0 - p.cursor.x) * 0.005;
          p.cursor.vy += (0 - p.cursor.y) * 0.005;
          p.cursor.vx *= 0.925;
          p.cursor.vy *= 0.925;
          p.cursor.x += p.cursor.vx * 2;
          p.cursor.y += p.cursor.vy * 2;
          p.cursor.x = Math.min(100, Math.max(-100, p.cursor.x));
          p.cursor.y = Math.min(100, Math.max(-100, p.cursor.y));
        });
      });
    }

    function moved(point, withCursorForce = true) {
      const x = point.x + (withCursorForce ? point.cursor.x : 0);
      const y = point.y + (withCursorForce ? point.cursor.y : 0);
      return { x: Math.round(x * 10) / 10, y: Math.round(y * 10) / 10 };
    }

    function drawLines() {
      const { lines, paths } = state;
      lines.forEach((points, lIndex) => {
        let p1 = moved(points[0], false);
        let d = `M ${p1.x} ${p1.y}`;
        points.forEach((pt, pIndex) => {
          const isLast = pIndex === points.length - 1;
          const p = moved(pt, !isLast);
          d += `L ${p.x} ${p.y}`;
        });
        paths[lIndex].setAttribute('d', d);
      });
    }

    function tick() {
      const { mouse } = state;
      mouse.sx += (mouse.x - mouse.sx) * 0.1;
      mouse.sy += (mouse.y - mouse.sy) * 0.1;
      const dx = mouse.x - mouse.lx;
      const dy = mouse.y - mouse.ly;
      const d = Math.hypot(dx, dy);
      mouse.v = d;
      mouse.vs += (d - mouse.vs) * 0.1;
      mouse.vs = Math.min(100, mouse.vs);
      mouse.lx = mouse.x;
      mouse.ly = mouse.y;
      mouse.a = Math.atan2(dy, dx);

      movePoints();
      drawLines();
      rafRef.current = requestAnimationFrame(tick);
    }

    function syncSize() {
      setSize();
      setLines();
    }

    const onMouseMove = (e) => updateMousePosition(e.clientX, e.clientY);
    const onTouchMove = (e) => {
      e.preventDefault();
      const t = e.touches[0];
      if (t) updateMousePosition(t.clientX, t.clientY);
    };

    setSize();
    setLines();
    const resizeObserver = new ResizeObserver(syncSize);
    resizeObserver.observe(container);
    window.addEventListener('resize', syncSize);
    window.addEventListener('mousemove', onMouseMove);
    container.addEventListener('touchmove', onTouchMove, { passive: false });

    rafRef.current = requestAnimationFrame(tick);

    return () => {
      resizeObserver.disconnect();
      window.removeEventListener('resize', syncSize);
      window.removeEventListener('mousemove', onMouseMove);
      container.removeEventListener('touchmove', onTouchMove);
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, []);

  return (
    <div ref={containerRef} className="waves-bg" aria-hidden="true">
      <svg ref={svgRef} className="waves-bg__svg" />
    </div>
  );
}

export default WavesBackground;
