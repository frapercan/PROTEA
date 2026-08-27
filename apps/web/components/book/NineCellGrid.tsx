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
export function NineCellGrid({
  frameCaption,
  italicLine,
}: {
  frameCaption: string;
  italicLine: string;
}) {
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
        {/*
          Where the words come from, said to the reader and not only to
          whoever opens book.ts. Seven cells read "carried" and two read
          "frontier", and without this line those are verdicts a reader has
          no way to place: they cannot tell that the figures were withdrawn
          rather than never taken, that the position is an external board's
          and not this system's own measurement, or where to go to see
          anything live.
        */}
        <p className="max-w-2xl text-[11px] leading-relaxed text-[var(--subtle)]">
          Positions are from the sealed board, which is an external evaluation
          of a submitted container and does not move. The figures behind them
          are withdrawn while the campaign recomputes, so each cell names which
          side of the frontier it is on and not by how much.
          {/*
            This caption used to carry a second sentence naming the campaign's
            own window and pointing at the grid that was filling it in. Both
            came from the retired ladder surface, which counted jobs whose
            evaluation results had been deleted, so the window it named was not
            the window any surviving result was scored over. It is dropped
            rather than corrected here: the replacement claim belongs to the
            experiment graph, which reads its frame from the evaluation set.
          */}
        </p>
      </figcaption>
    </figure>
  );
}
