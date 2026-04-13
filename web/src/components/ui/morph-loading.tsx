// Currently unused — replaced by LissajousLoading in the chat streaming indicator.
// Kept as an alternative loading animation. Uses CSS keyframes (morph-0 to morph-3)
// defined in index.css.
import { cn } from "@/lib/utils";

const containerSizes: Record<string, string> = {
  sm: "w-4 h-4",
  md: "w-6 h-6",
  lg: "w-8 h-8",
};

interface MorphLoadingProps {
  size?: "sm" | "md" | "lg";
  className?: string;
}

export default function MorphLoading({ size = "md", className }: MorphLoadingProps) {
  return (
    <div className={cn("relative", containerSizes[size], className)}>
      <div className="absolute inset-0 flex items-center justify-center">
        {[0, 1, 2, 3].map((i) => (
          <div
            key={i}
            className="absolute rounded-none bg-current"
            style={{
              width: '15%',
              height: '15%',
              animation: `morph-${i} 2s infinite ease-in-out`,
              animationDelay: `${i * 0.2}s`,
            }}
          />
        ))}
      </div>
    </div>
  );
}
