import { useEffect, useRef } from "react";
import { cn } from "@/lib/utils";

const SVG_NS = "http://www.w3.org/2000/svg";

const PARTICLE_COUNT = 68;
const TRAIL_SPAN = 0.34;
const DURATION_MS = 6000;
const PULSE_DURATION_MS = 5400;
const AMP = 24;
const AMP_BOOST = 6;
const AX = 3;
const BY = 4;
const PHASE = 1.57;
const Y_SCALE = 0.92;

function normalizeProgress(p: number) {
  return ((p % 1) + 1) % 1;
}

function getDetailScale(time: number) {
  const pulseProgress = (time % PULSE_DURATION_MS) / PULSE_DURATION_MS;
  return 0.52 + ((Math.sin(pulseProgress * Math.PI * 2 + 0.55) + 1) / 2) * 0.48;
}

function point(progress: number, detailScale: number) {
  const t = progress * Math.PI * 2;
  const amp = AMP + detailScale * AMP_BOOST;
  return {
    x: 50 + Math.sin(AX * t + PHASE) * amp,
    y: 50 + Math.sin(BY * t) * amp * Y_SCALE,
  };
}

interface LissajousLoadingProps {
  className?: string;
}

export default function LissajousLoading({
  className,
}: LissajousLoadingProps) {
  const groupRef = useRef<SVGGElement>(null);
  const rafRef = useRef<number>(0);
  const startRef = useRef<number>(0);

  useEffect(() => {
    const group = groupRef.current;
    if (!group) return;

    const particles: SVGCircleElement[] = [];
    for (let i = 0; i < PARTICLE_COUNT; i++) {
      const circle = document.createElementNS(SVG_NS, "circle");
      circle.setAttribute("fill", "currentColor");
      group.appendChild(circle);
      particles.push(circle);
    }

    startRef.current = performance.now();

    function render(now: number) {
      const time = now - startRef.current;
      const progress = (time % DURATION_MS) / DURATION_MS;
      const detailScale = getDetailScale(time);

      for (let i = 0; i < PARTICLE_COUNT; i++) {
        const tailOffset = i / (PARTICLE_COUNT - 1);
        const p = point(
          normalizeProgress(progress - tailOffset * TRAIL_SPAN),
          detailScale,
        );
        const fade = Math.pow(1 - tailOffset, 0.56);

        particles[i].setAttribute("cx", p.x.toFixed(2));
        particles[i].setAttribute("cy", p.y.toFixed(2));
        particles[i].setAttribute("r", (0.9 + fade * 2.7).toFixed(2));
        particles[i].setAttribute("opacity", (0.04 + fade * 0.96).toFixed(3));
      }

      rafRef.current = requestAnimationFrame(render);
    }

    rafRef.current = requestAnimationFrame(render);

    return () => {
      cancelAnimationFrame(rafRef.current);
      particles.forEach((c) => c.remove());
    };
  }, []);

  return (
    <div className={cn("relative", className)}>
      <svg
        viewBox="0 0 100 100"
        fill="none"
        className="w-full h-full overflow-visible"
        aria-hidden="true"
      >
        <g ref={groupRef} />
      </svg>
    </div>
  );
}
