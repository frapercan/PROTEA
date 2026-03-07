import { Fragment } from "react";
import { JobEvent } from "@/lib/api";

const LEVEL_STYLES: Record<string, string> = {
  info: "bg-blue-50 text-blue-700 border-blue-200",
  warning: "bg-yellow-50 text-yellow-700 border-yellow-200",
  error: "bg-red-50 text-red-700 border-red-200",
};

function formatTs(ts: string) {
  try {
    return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return ts;
  }
}

function Fields({ fields }: { fields: Record<string, any> }) {
  const entries = Object.entries(fields ?? {});
  if (entries.length === 0) return null;
  return (
    <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs text-gray-500">
      {entries.map(([k, v]) => (
        <Fragment key={k}>
          <dt className="font-medium text-gray-400">{k}</dt>
          <dd className="truncate">{typeof v === "object" ? JSON.stringify(v) : String(v)}</dd>
        </Fragment>
      ))}
    </dl>
  );
}

export function EventTimeline({ events }: { events: JobEvent[] }) {
  if (events.length === 0) {
    return <p className="text-sm text-gray-400">No events yet.</p>;
  }

  return (
    <ol className="space-y-2">
      {events.map((e) => {
        const levelCls = LEVEL_STYLES[e.level] ?? LEVEL_STYLES.info;
        return (
          <li key={e.id} className="flex gap-3">
            <div className="flex flex-col items-center">
              <span className={`mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border text-[9px] font-bold uppercase ${levelCls}`}>
                {e.level[0]}
              </span>
              <span className="mt-1 w-px grow bg-gray-100" />
            </div>
            <div className="pb-3 min-w-0 flex-1">
              <div className="flex items-baseline gap-2">
                <span className="font-mono text-xs font-semibold text-gray-800">{e.event}</span>
                <span className="text-xs text-gray-400">{formatTs(e.ts)}</span>
              </div>
              {e.message && <p className="mt-0.5 text-sm text-gray-600">{e.message}</p>}
              <Fields fields={e.fields} />
            </div>
          </li>
        );
      })}
    </ol>
  );
}
