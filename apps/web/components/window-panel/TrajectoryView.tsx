"use client";

// When the best result improved, on the whole cell or on one band of identity.

import { useTranslations } from "next-intl";
import { BAND_KEY, HOMOLOGY_BANDS, MIN_PROTEINS, armLabel, bandOf, cellF, championSteps, type SummaryResult } from "@/lib/windowPanel";
import { Figure, INK, Legend, SERIES, extent, fmt, pressable, signed, ticks, useTooltip } from "./chart";
import type { PanelView } from "./WindowPanel";

type Value = (r: SummaryResult) => number | null;

function Trajectory({
  view,
  points,
  value,
  x0,
  y0,
  w,
  h,
  compact,
  tip,
}: {
  view: PanelView;
  points: SummaryResult[];
  value: Value;
  x0: number;
  y0: number;
  w: number;
  h: number;
  compact: boolean;
  tip: ReturnType<typeof useTooltip>;
}) {
  const t = useTranslations("windowPanel");
  const m = compact ? { l: 34, r: 8, t: 8, b: 14 } : { l: 44, r: 16, t: 14, b: 30 };
  const refValue = value(view.reference as SummaryResult);
  const values = points.map((p) => value(p) ?? 0);
  const [lo, hi] = extent(refValue != null ? [...values, refValue] : values);
  const X = (i: number) => x0 + m.l + 8 + (i / Math.max(points.length - 1, 1)) * (w - m.l - m.r - 16);
  const Y = (v: number) => y0 + m.t + (1 - (v - lo) / (hi - lo)) * (h - m.t - m.b);
  const steps = championSteps(points.map((p) => ({ item: p, value: value(p) ?? 0 })));
  const path: [number, number][] = [];
  steps.forEach((s, k) => {
    if (k > 0) path.push([X(s.at), Y(steps[k - 1].now)]);
    path.push([X(s.at), Y(s.now)]);
  });
  if (path.length) path.push([X(points.length - 1), path[path.length - 1][1]]);
  return (
    <g>
      {ticks(lo, hi, hi - lo > 0.3 ? 0.1 : 0.05).map((v) => (
        <g key={v}>
          <line x1={x0 + m.l} x2={x0 + w - m.r} y1={Y(v)} y2={Y(v)} stroke={INK.grid} />
          <text x={x0 + m.l - 5} y={Y(v) + 4} textAnchor="end" fontSize={compact ? 10 : 11} fill="#78716c">{v.toFixed(2)}</text>
        </g>
      ))}
      {refValue != null ? (
        <line x1={x0 + m.l} x2={x0 + w - m.r} y1={Y(refValue)} y2={Y(refValue)} stroke={INK.reference} strokeWidth={1.5} strokeDasharray="5 4" />
      ) : null}
      <polyline points={path.map((q) => q.join(",")).join(" ")} fill="none" stroke="#0c0a09" strokeWidth={2} />
      {points.map((p, i) => {
        const colour = view.focusColour(p);
        const v = value(p) ?? 0;
        return (
          <g key={p.id}>
            <circle cx={X(i)} cy={Y(v)} r={colour ? (compact ? 4.5 : 6) : compact ? 3 : 4.5} fill={colour ?? SERIES[0]} stroke={INK.surface} strokeWidth={compact ? 1.5 : 2} />
            <circle
              cx={X(i)}
              cy={Y(v)}
              r={compact ? 7 : 11}
              fill="transparent"
              {...pressable(() => view.toggleFocus(p))}
              {...tip.bind(
                <>
                  <b>{armLabel(p)}</b>
                  <br />
                  {fmt(v)} ({signed(v - (refValue ?? 0))} {t("trajectory.overReference")})
                </>,
              )}
            />
          </g>
        );
      })}
    </g>
  );
}

export default function TrajectoryView({ view }: { view: PanelView }) {
  const t = useTranslations("windowPanel");
  const { state, commit, shown, reference: ref } = view;
  const tip = useTooltip();
  const measure = state.measure === "five" || (HOMOLOGY_BANDS as readonly string[]).includes(state.measure) ? state.measure : "all";
  const arms = shown.filter((r) => r !== ref);
  const legend = (
    <Legend
      items={[
        { label: t("trajectory.each"), swatch: { background: SERIES[0] } },
        { label: t("trajectory.best"), swatch: { background: "#0c0a09" } },
        { label: `${ref ? armLabel(ref) : ""} (${t("referenceShort")})`, swatch: { border: `2px dashed ${INK.reference}` } },
      ]}
    />
  );
  const options = [["all", t("trajectory.all")], ...HOMOLOGY_BANDS.map((b) => [b, t(`band.${BAND_KEY[b]}`)]), ["five", t("trajectory.five")]] as const;
  const placed = HOMOLOGY_BANDS.reduce((sum, b) => sum + (bandOf(ref, state.cell, "homology", b)?.n ?? 0), 0);

  let body: React.ReactNode;
  if (measure === "five") {
    const pw = 178;
    body = (
      <Figure title={t("trajectory.fiveTitle", { cell: state.cell.replace(":", " ") })} legend={legend} caption={t("trajectory.fiveCaption")}>
        <svg width={pw * HOMOLOGY_BANDS.length} viewBox={`0 0 ${pw * HOMOLOGY_BANDS.length} 244`} role="img" aria-label={t("trajectory.fiveTitle", { cell: state.cell })} style={{ maxWidth: "none" }}>
          {HOMOLOGY_BANDS.map((b, j) => {
            const value: Value = (r) => {
              const band = bandOf(r, state.cell, "homology", b);
              return band && band.n >= MIN_PROTEINS ? band.f : null;
            };
            const points = arms.filter((r) => value(r) != null);
            return (
              <g key={b}>
                <text x={j * pw + 34} y={14} fontSize={12} fontWeight={600} fill="#0c0a09" {...pressable(() => commit({ measure: b }, { push: true }))}>
                  {t(`band.${BAND_KEY[b]}`)} · n {bandOf(ref, state.cell, "homology", b)?.n ?? 0}
                </text>
                {points.length && value(ref as SummaryResult) != null ? (
                  <Trajectory view={view} points={points} value={value} x0={j * pw} y0={24} w={pw - 6} h={210} compact tip={tip} />
                ) : (
                  <text x={j * pw + 34} y={60} fontSize={11} fill="#78716c">{t("trajectory.underFloor")}</text>
                )}
              </g>
            );
          })}
        </svg>
      </Figure>
    );
  } else {
    const value: Value =
      measure === "all"
        ? (r) => cellF(r, state.cell)
        : (r) => {
            const band = bandOf(r, state.cell, "homology", measure);
            return band && band.n >= MIN_PROTEINS ? band.f : null;
          };
    const points = arms.filter((r) => value(r) != null);
    const steps = championSteps(points.map((p) => ({ item: p, value: value(p) ?? 0 })));
    const refValue = ref ? value(ref) : null;
    body = (
      <>
        <Figure title={measure === "all" ? t("trajectory.cellTitle", { cell: state.cell.replace(":", " ") }) : t("trajectory.bandTitle", { cell: state.cell.replace(":", " "), band: t(`band.${BAND_KEY[measure]}`) })} legend={legend}>
          {points.length && refValue != null ? (
            <svg width={900} viewBox="0 0 900 260" role="img" aria-label={t("trajectory.question")} style={{ maxWidth: "none" }}>
              <Trajectory view={view} points={points} value={value} x0={0} y0={0} w={900} h={260} compact={false} tip={tip} />
              <text x={44} y={252} fontSize={11} fill="#78716c">{t("trajectory.arrival")}</text>
            </svg>
          ) : (
            <p className="text-xs text-slate-500">{t("trajectory.none")}</p>
          )}
        </Figure>
        {steps.length ? (
          <div className="overflow-x-auto rounded-lg border bg-white">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
                  <th className="px-3 py-2">{t("trajectory.newBest")}</th>
                  <th className="px-3 py-2 text-right">{t("trajectory.before")}</th>
                  <th className="px-3 py-2 text-right">{t("trajectory.now")}</th>
                  <th className="px-3 py-2 text-right">{t("trajectory.overReference")}</th>
                  <th className="px-3 py-2">{t("trajectory.paired")}</th>
                </tr>
              </thead>
              <tbody>
                {steps.map((s) => (
                  <tr key={s.item.id} className="cursor-pointer border-t hover:bg-blue-50" onClick={() => view.toggleFocus(s.item)}>
                    <td className="px-3 py-1.5">{armLabel(s.item)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{fmt(s.before)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{fmt(s.now)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{signed(s.now - (refValue ?? 0))}</td>
                    <td className="px-3 py-1.5 text-xs text-slate-500">{t("trajectory.pending")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </>
    );
  }

  return (
    <>
      <p className="text-sm font-medium text-slate-800">{t("trajectory.question")}</p>
      <p className="text-xs text-slate-500">{t("trajectory.hint")}</p>
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("trajectory.measure")}</span>
        <span className="inline-flex flex-wrap overflow-hidden rounded-md border bg-white">
          {options.map(([key, label]) => (
            <button key={key} type="button" aria-pressed={measure === key} onClick={() => commit({ measure: key }, { push: true })} className={`px-2.5 py-0.5 ${measure === key ? "bg-blue-50 font-semibold text-blue-800" : ""}`}>
              {label}
            </button>
          ))}
        </span>
      </div>
      <p className="text-xs text-slate-500">
        {measure === "all"
          ? t("trajectory.nowAll", { proteins: placed, cell: state.cell.replace(":", " ") })
          : measure === "five"
            ? t("trajectory.nowFive")
            : t("trajectory.nowBand", { proteins: bandOf(ref, state.cell, "homology", measure)?.n ?? 0, band: t(`band.${BAND_KEY[measure]}`) })}
      </p>
      {body}
      {tip.node}
    </>
  );
}
