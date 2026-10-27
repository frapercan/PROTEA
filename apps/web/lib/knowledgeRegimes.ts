/**
 * The three knowledge regimes, defined once.
 *
 * NK, LK and PK are not labels: they are the partition that decides which
 * proteins each of the nine cells scores and whether cafaeval is given
 * ``-known``. Five places in this app defined them independently, and they did
 * not agree -- PK expanded to "Prior knowledge" in `lib/book.ts` and
 * `lib/strata.ts` and to "Partial Knowledge" in `components/BenchmarkHeatmap.tsx`,
 * `components/home/HomeShowcase.tsx` and the evaluation page. A reader meeting
 * both on one screen has to decide whether they are the same thing, and on the
 * page where two windows sit side by side "NK" already names populations that
 * differ by a factor of 33.
 *
 * The naming here is CAFA's own, and it is the evaluation page's, which is the
 * one surface that states the consequence rather than only the name: the
 * ``-known`` flag. "Prior knowledge" is retired.
 */

export const KNOWLEDGE_REGIMES = ["NK", "LK", "PK"] as const;

export type KnowledgeRegime = (typeof KNOWLEDGE_REGIMES)[number];

/** Title case, for headers and chips. */
export const KNOWLEDGE_LABEL: Record<KnowledgeRegime, string> = {
  NK: "No Knowledge",
  LK: "Limited Knowledge",
  PK: "Partial Knowledge",
};

/** Lower case, for running prose and axis values inside a sentence. */
export const KNOWLEDGE_LABEL_LOWER: Record<KnowledgeRegime, string> = {
  NK: "no knowledge",
  LK: "limited knowledge",
  PK: "partial knowledge",
};

/**
 * What the regime means, in terms of what the protein had at t0.
 *
 * Each sentence says the partition rule and then its scoring consequence,
 * because the second is what a reader needs to interpret the number and it is
 * the half that was missing wherever only a label existed.
 */
export const KNOWLEDGE_DEFINITION: Record<KnowledgeRegime, string> = {
  NK:
    "No Knowledge: the protein had no experimental annotation in any aspect at " +
    "t0, so everything it gained is new. Scored without -known.",
  LK:
    "Limited Knowledge: the protein was annotated in other aspects at t0 but " +
    "not in the one being evaluated, so this aspect is new for it. Scored " +
    "without -known.",
  PK:
    "Partial Knowledge: the protein already had annotations in the aspect " +
    "being evaluated at t0, so only the terms added by t1 count. Scored with " +
    "-known, which excludes what was already there.",
};

/** Whether cafaeval receives ``-known`` for this regime. */
export const KNOWLEDGE_USES_KNOWN: Record<KnowledgeRegime, boolean> = {
  NK: false,
  LK: false,
  PK: true,
};

/** Narrow an arbitrary string to a regime, or undefined. */
export function asKnowledgeRegime(value: string): KnowledgeRegime | undefined {
  return (KNOWLEDGE_REGIMES as readonly string[]).includes(value)
    ? (value as KnowledgeRegime)
    : undefined;
}
