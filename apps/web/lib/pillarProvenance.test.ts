import { describe, expect, it } from "vitest";

import { PILLARS } from "@/lib/book";

/**
 * Every chapter's evidence has to say where it stands.
 *
 * The campaign was wiped on 2026-09-14 and the figures written before it are
 * not evidence about anything now. Chapters two and three handled that by
 * withdrawing their values; chapter four handled it by labelling its own as
 * lab evaluations measured off the sealed board. Chapter one did neither: its
 * caption said "sealed booster" while, measured on this server on 2026-10-27,
 * `reranker_model` holds 0 rows, the cited `gain_report.json` is absent from
 * the filesystem, and `evaluation_frame` holds 0 rows.
 *
 * Nothing enforced the discipline, which is why it was applied to three
 * chapters out of four. This does.
 */
describe("pillar evidence states its provenance", () => {
  const withdrawn = (v: string) => v.toLowerCase().includes("withdrawn");

  it.each(PILLARS.map((p) => [p.n, p] as const))(
    "chapter %i either withdraws its values or labels them",
    (_n, pillar) => {
      const values = pillar.evidence.rows.map((r) => r.value);
      const allWithdrawn = values.every(withdrawn);
      const captionSaysSo = withdrawn(pillar.evidence.caption);
      const labelled = Boolean(pillar.evidenceLabel?.trim());
      expect(
        allWithdrawn || captionSaysSo || labelled,
        `chapter ${pillar.n} shows live values with no evidenceLabel and no ` +
          "withdrawal, so a reader cannot tell whether they come from the " +
          "sealed board, from the lab, or from before the wipe",
      ).toBe(true);
    },
  );

  it("no caption claims the sealed board", () => {
    // There is no sealed board in this record: evaluation_frame holds 0 rows
    // and no evaluation_result carries a frame or a frame_digest. A caption
    // that cites one is citing something absent.
    for (const p of PILLARS) {
      expect(
        p.evidence.caption.toLowerCase(),
        `chapter ${p.n}`,
      ).not.toContain("sealed");
    }
  });

  it("a labelled chapter says what the figures ARE, not just that they differ", () => {
    // The point of the label is to name the source. A note that only says
    // "these are different" leaves the reader where they started.
    for (const p of PILLARS) {
      if (!p.evidenceLabel) continue;
      const text = p.evidenceLabel.toLowerCase();
      expect(
        /lab |measured|server|wipe|record|board/.test(text),
        `chapter ${p.n}'s label names no source`,
      ).toBe(true);
    }
  });

  it("every receipt that cannot be read is marked pending", () => {
    // All four chapters rest on receipts under storage/, and none of those
    // paths exists on this server. The flag is what keeps the chapter honest
    // while the measurement is redone.
    for (const p of PILLARS) {
      expect(p.receipt.pending, `chapter ${p.n} receipt`).toBe(true);
      if (p.receiptSecondary) {
        expect(
          p.receiptSecondary.pending,
          `chapter ${p.n} secondary receipt`,
        ).toBe(true);
      }
    }
  });
});
