import {
  ASPECT_COLS,
  ASPECT_LABEL,
  KNOWLEDGE_LABEL,
  KNOWLEDGE_ROWS,
  NINE_CELL,
} from "@/lib/book";

/**
 * The hero, and the one risk the design takes: the nine-cell board typeset as a
 * table, opening with the two cells the method does NOT win. A product hero would
 * never lead with a loss; an honest frontier must, because it is the most
 * characteristic thing in this subject's world.
 *
 * Seven cells are set in ink. The two BP cells we do not win are set in frontier
 * rose inside a hairline outline. The only motion is the three rows arriving in
 * order (NK, then LK, then PK); the global prefers-reduced-motion rule removes it.
 *
 * A pure server component: no client JS, the reveal is CSS.
 */
export function NineCellGrid({ frameCaption, italicLine }: { frameCaption: string; italicLine: string }) {
  return (
    <figure className="m-0">
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-right font-mono tabular-nums">
          <caption className="sr-only">{frameCaption}</caption>
          <thead>
            <tr className="text-[var(--muted)]">
              <th scope="col" className="w-24 pb-3 text-left font-normal" />
              {ASPECT_COLS.map((a) => (
                <th
                  key={a}
                  scope="col"
                  className="pb-3 pl-6 text-right text-sm font-semibold tracking-wide"
                >
                  <abbr title={ASPECT_LABEL[a]} className="no-underline">
                    {a}
                  </abbr>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {KNOWLEDGE_ROWS.map((k, i) => (
              <tr
                key={k}
                className="grid-row-reveal border-t border-[var(--border)]"
                style={{ animationDelay: `${i * 180}ms` }}
              >
                <th
                  scope="row"
                  className="py-4 text-left align-baseline text-sm font-semibold text-[var(--muted)]"
                >
                  <abbr title={KNOWLEDGE_LABEL[k]} className="no-underline">
                    {k}
                  </abbr>
                </th>
                {ASPECT_COLS.map((a) => {
                  const cell = NINE_CELL[k][a];
                  return (
                    <td key={a} className="py-4 pl-6 align-baseline">
                      {/*
                        The value is withdrawn while the campaign recomputes, so each
                        cell shows the finding that survives it: whether this regime is
                        one the method carries, or the frontier where it does not. A
                        word is used rather than a blank, because a blank reads as
                        missing data and this is a deliberate absence.
                      */}
                      {cell.won ? (
                        <span className="text-[1.6rem] leading-none text-[var(--foreground)]">
                          {cell.value === null ? "carried" : cell.value.toFixed(3)}
                        </span>
                      ) : (
                        <span className="inline-flex items-baseline gap-1 text-[var(--danger)]">
                          <span aria-hidden className="text-lg opacity-60">
                            [
                          </span>
                          <span className="text-[1.6rem] leading-none">
                            {cell.value === null ? "frontier" : cell.value.toFixed(3)}
                          </span>
                          <span aria-hidden className="text-lg opacity-60">
                            ]
                          </span>
                        </span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <figcaption className="mt-5 space-y-1.5 text-left">
        <p className="font-serif text-[15px] italic leading-snug text-[var(--muted)]">{italicLine}</p>
        <p className="protea-eyebrow text-[11px] uppercase text-[var(--subtle)]">{frameCaption}</p>
      </figcaption>
    </figure>
  );
}
