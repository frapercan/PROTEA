"use client";

// Which arm wins each cell, and how far from the reference.

import { useTranslations } from "next-intl";
import { CELLS, armLabel, armShort, cellEdge, cellF, type SummaryResult } from "@/lib/windowPanel";
import { Figure, INK, Legend, deltaFill, fmt, pressable, signed, useTooltip } from "./chart";
import type { PanelView } from "./WindowPanel";

const ROWS = ["15", "30", "all"] as const;

export default function SummaryView({ view }: { view: PanelView }) {
  const t = useTranslations("windowPanel");
  const { state, commit, shown, reference: ref } = view;
  const tip = useTooltip();
  const arms = shown
    .filter((r) => r !== ref)
    .sort((a, b) => (cellF(b, state.cell) ?? -1) - (cellF(a, state.cell) ?? -1));
  const rows: SummaryResult[] = [
    ...(ref ? [ref] : []),
    ...(state.rows === "all" ? arms : arms.slice(0, Number(state.rows))),
  ];
  const best = arms[0];
  const label = 200;
  const width = 78;
  const height = 24;
  const top = 30;
  const svgHeight = top + height * rows.length + 4;
  const columnBest = CELLS.map((c) => Math.max(...rows.map((r) => cellF(r, c) ?? -1)));
  const column = CELLS.indexOf(state.cell);

  return (
    <>
      <p className="text-sm font-medium text-slate-800">{t("summary.question")}</p>
      <p className="text-xs text-slate-500">{t("summary.hint")}</p>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-600">
        <span className="inline-flex items-center gap-2">
          <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("summary.rows")}</span>
          <span className="inline-flex overflow-hidden rounded-md border bg-white">
            {ROWS.map((n) => (
              <button
                key={n}
                type="button"
                aria-pressed={state.rows === n}
                onClick={() => commit({ rows: n })}
                className={`px-2 py-0.5 ${state.rows === n ? "bg-blue-50 font-semibold text-blue-800" : ""}`}
              >
                {n === "all" ? t("summary.rowsAll") : n}
              </button>
            ))}
          </span>
        </span>
        <span>
          {t("summary.shown", { shown: rows.length - (ref ? 1 : 0), total: arms.length })}
          {best ? (
            <>
              {" · "}
              {t("summary.best", { cell: state.cell.replace(":", " ") })}{" "}
              <strong>{armLabel(best)}</strong> {fmt(cellF(best, state.cell))} (
              {signed((cellF(best, state.cell) ?? 0) - (cellF(ref, state.cell) ?? 0))})
            </>
          ) : null}
        </span>
      </div>
      <Figure
        legend={
          <Legend
            items={[
              { label: t("summary.above"), swatch: { background: INK.above } },
              { label: t("summary.same"), swatch: { background: INK.neutral } },
              { label: t("summary.below"), swatch: { background: INK.below } },
              { label: t("summary.edge"), swatch: { background: INK.edge } },
              { label: t("summary.bestOfColumn"), swatch: { border: "1.5px solid #0c0a09" } },
            ]}
          />
        }
        caption={t("summary.caption")}
      >
        <svg
          width={label + width * CELLS.length}
          viewBox={`0 0 ${label + width * CELLS.length} ${svgHeight}`}
          role="img"
          aria-label={t("summary.question")}
          style={{ maxWidth: "none" }}
        >
          <rect x={label + width * column} y={0} width={width} height={svgHeight} rx={6} fill={INK.selected} />
          {CELLS.map((c, j) => (
            <text
              key={c}
              x={label + width * j + width / 2}
              y={19}
              textAnchor="middle"
              fontSize={12}
              fontWeight={c === state.cell ? 700 : 400}
              fill={c === state.cell ? "#0c0a09" : "#57534e"}
              {...pressable(() => commit({ cell: c, band: "" }, { push: true }))}
            >
              {c.replace(":", " ")}
            </text>
          ))}
          {rows.map((r, i) => {
            const y = top + height * i;
            const colour = view.focusColour(r);
            return (
              <g key={r.id}>
                {colour ? <circle cx={6} cy={y + height / 2} r={4} fill={colour} /> : null}
                <text
                  x={16}
                  y={y + height / 2 + 4}
                  fontSize={12.5}
                  fontWeight={r === ref ? 600 : 400}
                  fill="#0c0a09"
                  {...pressable(() => view.toggleFocus(r))}
                  {...tip.bind(
                    <>
                      <b>{armLabel(r)}</b>
                      <br />
                      {t("summary.rowTip", { id: r.id.slice(0, 8) })}
                    </>,
                  )}
                >
                  {r === ref ? `${armShort(r)} (${t("referenceShort")})` : armShort(r)}
                </text>
                {CELLS.map((c, j) => {
                  const v = cellF(r, c);
                  const delta = r === ref || v == null ? null : v - (cellF(ref, c) ?? 0);
                  const { fill, dark } = r === ref ? { fill: INK.neutral, dark: false } : deltaFill(delta);
                  const x = label + width * j + 2;
                  const edge = cellEdge(r, c);
                  return (
                    <g key={c}>
                      <rect
                        x={x}
                        y={y + 1}
                        width={width - 4}
                        height={height - 2}
                        rx={4}
                        fill={fill}
                        stroke={v != null && v === columnBest[j] ? "#0c0a09" : "none"}
                        strokeWidth={1.5}
                        {...pressable(() => view.open(r, c))}
                        {...tip.bind(
                          <>
                            <b>{armLabel(r)}</b>
                            <br />
                            {c.replace(":", " ")}: f_micro_w {fmt(v)}
                            {delta != null ? <><br />{t("summary.cellTip", { delta: signed(delta) })}</> : null}
                            {edge ? <><br />{t("summary.edgeTip", { edge })}</> : null}
                            <br />
                            {t("summary.openTip")}
                          </>,
                        )}
                      />
                      {edge ? (
                        <path d={`M${x + width - 16} ${y + 1} h8 a4 4 0 0 1 4 4 v8 z`} fill={INK.edge} pointerEvents="none" />
                      ) : null}
                      <text
                        x={x + (width - 4) / 2}
                        y={y + height / 2 + 4}
                        textAnchor="middle"
                        fontSize={11.5}
                        fontFamily="var(--font-mono)"
                        fill={dark ? "#ffffff" : "#0c0a09"}
                        pointerEvents="none"
                      >
                        {fmt(v)}
                      </text>
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </Figure>
      {tip.node}
    </>
  );
}
