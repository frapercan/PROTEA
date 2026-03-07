type Status = "queued" | "running" | "succeeded" | "failed" | "cancelled" | string;

const STYLES: Record<string, string> = {
  queued: "bg-yellow-100 text-yellow-800 border-yellow-200",
  running: "bg-blue-100 text-blue-800 border-blue-200",
  succeeded: "bg-green-100 text-green-800 border-green-200",
  failed: "bg-red-100 text-red-800 border-red-200",
  cancelled: "bg-gray-100 text-gray-600 border-gray-200",
};

export function StatusBadge({ status }: { status: Status }) {
  const key = status.toLowerCase();
  const cls = STYLES[key] ?? "bg-gray-100 text-gray-600 border-gray-200";
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium ${cls}`}>
      {key === "running" && (
        <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500" />
      )}
      {status.toUpperCase()}
    </span>
  );
}
