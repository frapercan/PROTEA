export function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded bg-gray-200 ${className}`} />;
}

export function SkeletonTableRow({ cols = 4 }: { cols?: number }) {
  const widths = ["w-20", "w-32", "flex-1", "w-28", "w-16", "w-24"];
  return (
    <div className="flex items-center gap-4 border-b px-4 py-3 last:border-0">
      {Array.from({ length: cols }).map((_, i) => (
        <Skeleton key={i} className={`h-4 ${widths[i] ?? "w-20"}`} />
      ))}
    </div>
  );
}
