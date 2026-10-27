/**
 * Deterministic formatting, identical on the server and in the browser.
 *
 * A bare `toLocale*` call, with no locale argument, takes
 * the ENVIRONMENT's locale and time zone. Node on the deployment host
 * resolves to es-ES / Europe/Madrid, a visitor's browser resolves to
 * whatever they have, so the server rendered "754.862" and an
 * English-locale browser rendered "754,862". React then refuses to
 * reconcile the tree: that is the React #418 an external audit hit on
 * /instrument/datasets on 2026-10-07, and the same cause behind the
 * thousands separator disagreeing from page to page.
 *
 * Pinned rather than locale-aware, deliberately. Two reasons: several
 * call sites are plain modules with no access to a locale (lib/*.ts has
 * no hooks), and a figure that reads the same for every visitor is worth
 * more on an instrument page than one that follows their locale. Prose
 * numbers on the landing page do NOT come through here: those go through
 * next-intl's ICU `{n, number}`, which is locale-aware and already
 * agrees across hydration because both sides know the route's locale.
 */

const COUNT = new Intl.NumberFormat("en-GB");
const DECIMAL = new Intl.NumberFormat("en-GB", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 3,
});
const DATE_TIME = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "short",
  timeStyle: "short",
  timeZone: "UTC",
});
const DATE_ONLY = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeZone: "UTC",
});

/** A whole number with thousands separators: 754862 -> "754,862". */
export function formatCount(n: number | null | undefined): string {
  return n == null || Number.isNaN(n) ? "—" : COUNT.format(n);
}

/** A number that may carry decimals, capped at three. */
export function formatDecimal(n: number | null | undefined): string {
  return n == null || Number.isNaN(n) ? "—" : DECIMAL.format(n);
}

/**
 * A timestamp, in UTC and labelled as such.
 *
 * UTC on purpose: on an operations page a time means more when every
 * reader is looking at the same instant, and the label removes the
 * ambiguity that a bare local time leaves.
 */
export function formatDateTime(value: string | number | Date | null | undefined): string {
  if (value == null) return "—";
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? "—" : `${DATE_TIME.format(d)} UTC`;
}

/** A date with no time, for when the hour carries no information. */
export function formatDate(value: string | number | Date | null | undefined): string {
  if (value == null) return "—";
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? "—" : DATE_ONLY.format(d);
}

/** A clock time, UTC, for an event stream where only the hour matters. */
const TIME_ONLY = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC",
});

export function formatTime(value: string | number | Date | null | undefined): string {
  if (value == null) return "—";
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? "—" : TIME_ONLY.format(d);
}
