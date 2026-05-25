"use client";

import { useMemo, useState } from "react";
import type { AnnotationSet } from "@/lib/api";

/**
 * Horizontal temporal axis of GOA / QuickGO releases.
 *
 * Each `AnnotationSet` becomes a dot positioned at its
 * `source_published_at` along an axis that spans the earliest to the
 * latest known release date (with a small margin). Rows are grouped
 * per `source` family so multi-source benches read at a glance.
 *
 * Releases without a `source_published_at` are stacked at the right
 * edge of the axis with a greyed-out fill and an explanatory tooltip
 * pointing the operator at the `refresh_goa_release_dates` job that
 * populates that column.
 *
 * Hand-rolled SVG by design: a one-row strip of dots over a labeled
 * axis does not justify a chart library dependency.
 */

type GoaReleaseTimelineProps = {
  sets: AnnotationSet[];
  onSelect?: (set: AnnotationSet) => void;
  refreshing?: boolean;
  onRefresh?: () => void;
  selectedId?: string;
  /**
   * Heading level for the component title. Pick the level that keeps
   * the surrounding page's heading order monotonic (no skipped levels).
   * Defaults to `h3` for backward compatibility.
   */
  headingLevel?: "h2" | "h3";
};

type Positioned = {
  set: AnnotationSet;
  /** 0..1 on the inner axis; undefined when `source_published_at` is missing. */
  x?: number;
  date?: Date;
};

const SOURCE_LABEL: Record<string, string> = {
  goa: "GOA",
  quickgo: "QuickGO",
};

function parseDate(s?: string | null): Date | undefined {
  if (!s) return undefined;
  const d = new Date(s);
  return Number.isFinite(d.getTime()) ? d : undefined;
}

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function yearTicks(min: Date, max: Date): Date[] {
  const startYear = min.getUTCFullYear();
  const endYear = max.getUTCFullYear();
  const out: Date[] = [];
  for (let y = startYear; y <= endYear; y++) {
    out.push(new Date(Date.UTC(y, 0, 1)));
  }
  return out;
}

function monthTicks(min: Date, max: Date): Date[] {
  const start = new Date(Date.UTC(min.getUTCFullYear(), min.getUTCMonth(), 1));
  const out: Date[] = [];
  const cursor = new Date(start);
  while (cursor <= max) {
    out.push(new Date(cursor));
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }
  return out;
}

function tooltipText(p: Positioned): string {
  const family = SOURCE_LABEL[p.set.source] ?? p.set.source.toUpperCase();
  const v = p.set.source_version ?? "?";
  const count = p.set.annotation_count?.toLocaleString();
  const countPart = count ? ` . ${count} ann.` : "";
  if (!p.date) {
    return (
      `[${family}] v${v} . date unknown` +
      countPart +
      " . run refresh_goa_release_dates"
    );
  }
  return `[${family}] v${v} . ${formatDate(p.date)}${countPart}`;
}

export function GoaReleaseTimeline({
  sets,
  onSelect,
  refreshing = false,
  onRefresh,
  selectedId,
  headingLevel = "h3",
}: GoaReleaseTimelineProps) {
  const Heading = headingLevel;
  const [hoverId, setHoverId] = useState<string | null>(null);

  const grouped = useMemo(() => {
    const bySource = new Map<string, AnnotationSet[]>();
    for (const s of sets) {
      if (!bySource.has(s.source)) bySource.set(s.source, []);
      bySource.get(s.source)!.push(s);
    }
    return Array.from(bySource.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [sets]);

  const datedSets = useMemo(
    () =>
      sets
        .map((s) => parseDate(s.source_published_at))
        .filter((d): d is Date => !!d),
    [sets],
  );

  if (sets.length === 0) {
    return null;
  }

  // Axis range: pad 1 month each side so dots are not clipped.  Fall
  // back to a sensible default span when no release carries a date.
  const minDate = datedSets.length
    ? new Date(Math.min(...datedSets.map((d) => d.getTime())))
    : new Date(Date.UTC(2020, 0, 1));
  const maxDate = datedSets.length
    ? new Date(Math.max(...datedSets.map((d) => d.getTime())))
    : new Date();
  const padded = new Date(minDate);
  padded.setUTCMonth(padded.getUTCMonth() - 1);
  const paddedMax = new Date(maxDate);
  paddedMax.setUTCMonth(paddedMax.getUTCMonth() + 1);

  const minMs = padded.getTime();
  const maxMs = paddedMax.getTime();
  const span = Math.max(1, maxMs - minMs);

  const W = 720;
  const H = 56 + grouped.length * 38;
  const padLeft = 60;
  const padRight = 24;
  const axisW = W - padLeft - padRight;
  const axisY = H - 22;

  function xOf(d: Date): number {
    return padLeft + ((d.getTime() - minMs) / span) * axisW;
  }

  const undatedX = padLeft + axisW + 12;

  const positioned: Positioned[] = sets.map((s) => {
    const d = parseDate(s.source_published_at);
    return { set: s, date: d, x: d ? xOf(d) : undefined };
  });

  const months = monthTicks(padded, paddedMax);
  const years = yearTicks(padded, paddedMax);

  return (
    <div className="rounded-lg border border-stone-200 bg-white p-4">
      <div className="flex items-center justify-between mb-2">
        <Heading className="text-sm font-medium text-stone-800">
          Release timeline
        </Heading>
        {onRefresh && (
          <button
            type="button"
            onClick={onRefresh}
            disabled={refreshing}
            className="rounded-md border border-stone-300 bg-white px-3 py-1 text-xs font-medium text-stone-700 hover:bg-stone-50 disabled:opacity-50"
            title="Fetch the EBI FTP listing and backfill publication dates"
          >
            {refreshing ? "Refreshing." : "Refresh release dates"}
          </button>
        )}
      </div>

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full font-[Inter] text-[11px] text-stone-600"
        role="img"
        aria-label="GOA release timeline"
      >
        {/* Month ticks (light) */}
        {months.map((m, i) => (
          <line
            key={`m-${i}`}
            x1={xOf(m)}
            x2={xOf(m)}
            y1={axisY - 3}
            y2={axisY + 3}
            stroke="#e7e5e4"
            strokeWidth={1}
          />
        ))}
        {/* Axis */}
        <line
          x1={padLeft}
          x2={padLeft + axisW}
          y1={axisY}
          y2={axisY}
          stroke="#a8a29e"
          strokeWidth={1}
        />
        {/* Year ticks + labels */}
        {years.map((y, i) => (
          <g key={`y-${i}`}>
            <line
              x1={xOf(y)}
              x2={xOf(y)}
              y1={axisY - 6}
              y2={axisY + 6}
              stroke="#78716c"
              strokeWidth={1}
            />
            <text
              x={xOf(y)}
              y={axisY + 16}
              textAnchor="middle"
              fill="#57534e"
            >
              {y.getUTCFullYear()}
            </text>
          </g>
        ))}

        {/* Per-source rows */}
        {grouped.map(([source, list], rowIdx) => {
          const rowY = 28 + rowIdx * 38;
          return (
            <g key={source}>
              <text
                x={padLeft - 8}
                y={rowY + 4}
                textAnchor="end"
                fill="#44403c"
                className="font-medium"
              >
                {SOURCE_LABEL[source] ?? source.toUpperCase()}
              </text>
              <line
                x1={padLeft}
                x2={padLeft + axisW}
                y1={rowY}
                y2={rowY}
                stroke="#f5f5f4"
                strokeWidth={1}
              />
              {positioned
                .filter((p) => p.set.source === source)
                .map((p) => {
                  const cx = p.x ?? undatedX;
                  const hover = hoverId === p.set.id;
                  const selected = selectedId === p.set.id;
                  const fill = p.date
                    ? selected
                      ? "#0c0a09"
                      : hover
                        ? "#1c1917"
                        : "#44403c"
                    : "#d6d3d1";
                  const stroke = p.date ? "#1c1917" : "#a8a29e";
                  return (
                    <g
                      key={p.set.id}
                      onMouseEnter={() => setHoverId(p.set.id)}
                      onMouseLeave={() => setHoverId(null)}
                      onClick={() => onSelect?.(p.set)}
                      style={{ cursor: onSelect ? "pointer" : "default" }}
                    >
                      <circle
                        cx={cx}
                        cy={rowY}
                        r={hover || selected ? 6 : 4.5}
                        fill={fill}
                        stroke={stroke}
                        strokeWidth={1}
                      >
                        <title>{tooltipText(p)}</title>
                      </circle>
                      {(hover || selected) && p.set.source_version && (
                        <text
                          x={cx}
                          y={rowY - 10}
                          textAnchor="middle"
                          fill="#1c1917"
                          className="font-medium"
                        >
                          v{p.set.source_version}
                        </text>
                      )}
                    </g>
                  );
                })}
            </g>
          );
        })}

        {/* Undated marker explanation */}
        {positioned.some((p) => !p.date) && (
          <g>
            <line
              x1={undatedX - 6}
              x2={undatedX - 6}
              y1={20}
              y2={H - 30}
              stroke="#e7e5e4"
              strokeDasharray="2 2"
            />
            <text
              x={undatedX + 8}
              y={H - 26}
              fill="#a8a29e"
              fontSize="10"
              transform={`rotate(-90 ${undatedX + 8} ${H - 26})`}
            >
              undated
            </text>
          </g>
        )}
      </svg>

      <p className="mt-2 text-[11px] text-stone-500">
        Dates from EBI FTP (`goa_uniprot_all.gaf.N.gz`). Greyed markers
        lack a publication date; run the refresh job to backfill.
      </p>
    </div>
  );
}
