"use client";

// What the panel's drawings share: the palette, a colour mix, number formats
// and one tooltip that follows the pointer.

import { useCallback, useState, type ReactNode } from "react";

/** The validated categorical palette, in its fixed order (never cycled). */
export const SERIES = [
  "#2a78d6",
  "#eb6834",
  "#1baf7a",
  "#eda100",
  "#e87ba4",
  "#008300",
  "#6250d6",
  "#e34948",
] as const;
export const FOCUS = SERIES.slice(0, 3);
export const INK = {
  above: "#2a78d6",
  below: "#eb6834",
  neutral: "#e9e8e4",
  reference: "#52514e",
  referenceSoft: "#e4e3df",
  grid: "#e7e9ee",
  edge: "#b26b00",
  surface: "#ffffff",
  selected: "#eff6ff",
} as const;

/** ``a`` moved ``t`` of the way to ``b``, both ``#rrggbb``. */
export function mix(a: string, b: string, t: number): string {
  const rgb = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
  const [x, y] = [rgb(a), rgb(b)];
  return `#${x.map((v, i) => Math.round(v + (y[i] - v) * t).toString(16).padStart(2, "0")).join("")}`;
}

/** The colour of a difference from the reference, saturated at ±0.10. */
export function deltaFill(delta: number | null): { fill: string; dark: boolean } {
  if (delta == null) return { fill: INK.neutral, dark: false };
  const t = Math.min(Math.abs(delta) / 0.1, 1);
  return { fill: mix(INK.neutral, delta >= 0 ? INK.above : INK.below, t * 0.85), dark: t > 0.6 };
}

export function fmt(v: number | null | undefined, digits = 3): string {
  return v == null || Number.isNaN(v) ? "—" : v.toFixed(digits);
}

export function signed(v: number | null | undefined, digits = 3): string {
  if (v == null || Number.isNaN(v)) return "—";
  return `${v >= 0 ? "+" : "−"}${Math.abs(v).toFixed(digits)}`;
}

/** Gridline values between ``lo`` and ``hi`` at ``step``. */
export function ticks(lo: number, hi: number, step: number): number[] {
  const out: number[] = [];
  for (let v = lo; v <= hi + 1e-9; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

/** A scale's ends: the values' range, padded and rounded outwards to ``unit``. */
export function extent(values: number[], unit = 0.05, pad = 0.02): [number, number] {
  if (!values.length) return [0, 1];
  const lo = Math.max(0, Math.floor((Math.min(...values) - pad) / unit) * unit);
  const hi = Math.ceil((Math.max(...values) + pad) / unit) * unit;
  return [lo, hi > lo ? hi : lo + unit];
}

type Tip = { x: number; y: number; body: ReactNode } | null;

/** One tooltip for a whole view: ``bind(body)`` goes on any mark that has one. */
export function useTooltip() {
  const [tip, setTip] = useState<Tip>(null);
  const bind = useCallback(
    (body: ReactNode) => ({
      onMouseMove: (e: React.MouseEvent) => setTip({ x: e.clientX, y: e.clientY, body }),
      onMouseLeave: () => setTip(null),
    }),
    [],
  );
  const hide = useCallback(() => setTip(null), []);
  const node = tip ? (
    <div
      role="tooltip"
      className="pointer-events-none fixed z-50 max-w-xs rounded-md border bg-white px-3 py-2 text-xs leading-relaxed text-slate-800 shadow-md"
      style={{ left: tip.x + 14, top: tip.y + 14 }}
    >
      {tip.body}
    </div>
  ) : null;
  return { bind, hide, node };
}

/** A mark that acts like a button: clickable, focusable, and Enter or Space press it. */
export function pressable(onPress: () => void) {
  return {
    role: "button",
    tabIndex: 0,
    style: { cursor: "pointer" },
    onClick: onPress,
    onKeyDown: (e: React.KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onPress();
      }
    },
  };
}

export function Legend({ items }: { items: { label: ReactNode; swatch: React.CSSProperties }[] }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600">
      {items.map((item, i) => (
        <span key={i} className="inline-flex items-center gap-1.5">
          <i className="inline-block h-3 w-3 shrink-0 rounded-sm" style={item.swatch} />
          {item.label}
        </span>
      ))}
    </div>
  );
}

export function Figure({
  title,
  legend,
  caption,
  children,
}: {
  title?: ReactNode;
  legend?: ReactNode;
  caption?: ReactNode;
  children: ReactNode;
}) {
  return (
    <figure className="grid min-w-0 gap-2.5 rounded-lg border bg-white px-4 py-3.5 shadow-sm">
      {title ? <h3 className="text-sm font-semibold text-slate-800">{title}</h3> : null}
      {legend}
      <div className="min-w-0 overflow-x-auto">{children}</div>
      {caption ? <figcaption className="text-xs text-slate-500">{caption}</figcaption> : null}
    </figure>
  );
}
