import type { EvidenceTable as EvidenceTableData } from "@/lib/book";

/**
 * An evidence table in the register of a scholarly edition: horizontal hairlines
 * only, no zebra, no vertical rules, decimal-aligned tabular figures. The caption
 * carries the frame stamp; a table without one is a bug, not a style choice.
 */
export function EvidenceTable({ data }: { data: EvidenceTableData }) {
  return (
    <figure className="m-0">
      <div className="overflow-x-auto protea-scroll-shadow">
        <table className="w-full border-collapse text-left">
          <thead>
            <tr className="border-b border-[var(--border-strong)] text-[var(--muted)]">
              <th scope="col" className="py-2 pr-4 text-[13px] font-semibold" />
              <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                {data.valueHeader}
              </th>
              <th scope="col" className="py-2 pl-4 text-right text-[13px] font-semibold" />
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row) => (
              <tr key={row.label} className="border-b border-[var(--border)]">
                <th
                  scope="row"
                  className={`py-2.5 pr-4 text-left text-[14px] font-normal ${
                    row.frontier ? "font-mono text-[var(--danger)]" : "font-mono text-[var(--foreground)]"
                  }`}
                >
                  {row.label}
                </th>
                <td
                  className={`py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] ${
                    row.frontier ? "text-[var(--danger)]" : "text-[var(--foreground)]"
                  }`}
                >
                  {row.value}
                </td>
                <td
                  className={`py-2.5 pl-4 text-right text-[13px] ${
                    row.frontier ? "text-[var(--danger)]" : "text-[var(--muted)]"
                  }`}
                >
                  {row.note ?? ""}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <figcaption className="mt-2.5 protea-eyebrow text-[11px] uppercase text-[var(--subtle)]">
        {data.caption}
      </figcaption>
    </figure>
  );
}
