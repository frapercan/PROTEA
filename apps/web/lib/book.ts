/**
 * The book, as data.
 *
 * The interface has a front door that reads as a book rather than a control
 * panel: `/` is THE ARGUMENT, `/pillar/1..4` are its four chapters. This module
 * is the single place the argument's prose and its sealed numbers live, so a
 * claim (and every figure inside it) exists in exactly one place and the three
 * surfaces (thesis, interface, reference) can point at it instead of copying it.
 *
 * Language policy: the scholarly prose here is English only, matching the
 * doctoral manuscript (the thesis is written in English). The surrounding UI
 * chrome (section labels, the receipt apparatus, navigation) IS localized
 * through next-intl in all five locales; see the `book` namespace in
 * messages/*.json. The argument itself is not machine-translated, because a
 * mistranslated claim would violate the one-metric-one-frame discipline these
 * pages exist to enforce.
 *
 * Every number below traces to a receipt file under `storage/` or to the sealed
 * board. Where a receipt named by the plan was not present in the storage
 * snapshot, its `pending` flag is set and the copy says so, rather than a value
 * being invented.
 */

// The one metric, the one frame. Stamped on every evidence table.
export const METRIC = "f_micro_w";
export const FRAME = "v227 to v230";
export const VALIDATION_FRAME = "v225 to v227";

// The sealed headline. Immutable.
export const HEADLINE = {
  value: "0.4063",
  metric: METRIC,
  frame: FRAME,
  validation: VALIDATION_FRAME,
  wonCells: 7,
  totalCells: 9,
} as const;

// The one sentence the whole product is an argument for.
export const THESIS_SENTENCE =
  "Protein function is predictable from a taxonomy of orthogonal evidence, combined by a calibrated fusion, measured on a leakage-free temporal frame. The nine-cell benchmark is not nine numbers, it is a map of which evidence wins in which regime, and it has an honest, evidence-bound frontier that this work characterises rather than hides.";

/**
 * Chapter zero: the whole argument, end to end, for a reader who is barely
 * initiated. Every term new to a beginner (GO, kNN, reranker, temporal holdout)
 * is explained in place, the real sealed numbers are cited, and each movement
 * links on to the chapter that develops it. The test for every line: could
 * someone who just started understand it?
 */
export interface ChapterZeroMovement {
  lead: string;
  body: string;
  /** A path relative to the locale root, e.g. "pillar/1". */
  link?: { to: string; label: string };
}

export const CHAPTER_ZERO: ChapterZeroMovement[] = [
  {
    lead: "What this is.",
    body:
      "A protein is a chain of amino acids that does a job in the cell. Its function is that job, written in a shared vocabulary called the Gene Ontology, or GO: terms like binds DNA or located in the mitochondrion. Most proteins ever sequenced carry no such labels. PROTEA reads a protein's sequence and proposes its GO terms, with the evidence for each one in plain view.",
    link: { to: "pillar/1", label: "The kinds of evidence it reads" },
  },
  {
    lead: "How it works, in one breath.",
    body:
      "PROTEA does not guess a function from the sequence directly. It retrieves. It turns each protein into a compact code, finds the most similar proteins that are already labelled, and lets those neighbours vote for GO terms; this is k-nearest-neighbours, kNN. A second stage, a reranker, weighs that vote against other clues, how strong each match is and how a candidate term relates to what we already know, and calibrates a final score.",
    link: { to: "pillar/2", label: "The two learning layers, and why calibration is the lever" },
  },
  {
    lead: "What it achieves.",
    body:
      "On a fair test, PROTEA reaches " + HEADLINE.value + " on the field's headline score (a weighted, information-aware measure called " + METRIC + ") and ranks first in seven of the nine evaluation cells. The nine cells are three knowledge regimes, from proteins we know nothing about to proteins we already know something about, crossed with the three branches of GO.",
    link: { to: "pillar/3", label: "The nine-cell board, read as a map" },
  },
  {
    lead: "Why the test is fair.",
    body:
      "The hard part of this field is not the model, it is not cheating. PROTEA is scored on a temporal holdout: we freeze what was known on one date and ask only about function that was discovered afterwards, so the answer cannot leak into the question. Every number here is reproducible from that frozen frame, and the sealed board never moves; regenerated numbers are candidates until reviewed against it.",
  },
  {
    lead: "Where it stops, honestly.",
    body:
      "Two of the nine cells are not won: predicting the Biological Process branch for the least-studied proteins. The evidence available does not reach there yet. We name it the biological-process wall and we show it, rather than hide it, and we point to the first crack in it.",
    link: { to: "pillar/4", label: "The frontier, and the first crack in the wall" },
  },
];

export type Aspect = "MF" | "BP" | "CC";
export type Knowledge = "NK" | "LK" | "PK";

export interface GridCell {
  value: number;
  /** false only for the two cells the method does not win: LK-BP and PK-BP. */
  won: boolean;
}

/**
 * The nine-cell board, `f_micro_w`, sealed champion, frame v227 to v230.
 * Rows are knowledge regimes (No / Limited / Prior knowledge), columns are GO
 * aspects. The two BP cells the method does not win are the biological-process
 * wall, and they are the honest centre of the whole argument.
 */
export const NINE_CELL: Record<Knowledge, Record<Aspect, GridCell>> = {
  NK: { MF: { value: 0.648, won: true }, BP: { value: 0.331, won: true }, CC: { value: 0.481, won: true } },
  LK: { MF: { value: 0.559, won: true }, BP: { value: 0.354, won: false }, CC: { value: 0.467, won: true } },
  PK: { MF: { value: 0.230, won: true }, BP: { value: 0.141, won: false }, CC: { value: 0.273, won: true } },
};

export const KNOWLEDGE_ROWS: Knowledge[] = ["NK", "LK", "PK"];
export const ASPECT_COLS: Aspect[] = ["MF", "BP", "CC"];

export const KNOWLEDGE_LABEL: Record<Knowledge, string> = {
  NK: "No knowledge",
  LK: "Limited knowledge",
  PK: "Prior knowledge",
};
export const ASPECT_LABEL: Record<Aspect, string> = {
  MF: "Molecular function",
  BP: "Biological process",
  CC: "Cellular component",
};

/** How a claim is regenerated: a dispatchable operation, or an offline script. */
export type Operation =
  | { kind: "job"; operation: string; payload: Record<string, unknown>; note?: string }
  | { kind: "script"; script: string; note?: string };

/** A receipt: the artifact that carries the number, and the script that made it. */
export interface Receipt {
  /** Path under the repository `storage/` tree. */
  artifact: string;
  /** The script that produced the artifact, if distinct. */
  script?: string;
  /** Content fingerprint when one is recorded. */
  sha?: string;
  /** True when the plan names this receipt but it was absent from the storage snapshot. */
  pending?: boolean;
}

export interface EvidenceRow {
  label: string;
  /** Rendered in mono, decimal-aligned. */
  value: string;
  /** Optional right-hand note, muted. */
  note?: string;
  /** Marks the frontier: rendered in frontier rose. */
  frontier?: boolean;
}

export interface EvidenceTable {
  /** The caption carries the frame stamp. A table without one is a bug. */
  caption: string;
  /** Column header for the value column. */
  valueHeader: string;
  rows: EvidenceRow[];
}

export interface Pillar {
  n: 1 | 2 | 3 | 4;
  eyebrow: string;
  title: string;
  /** One line for the chapter card on the argument page. */
  teaser: string;
  /** The claim, in prose, one paragraph. */
  claim: string;
  /** The evidence, board-faithful or clearly labelled otherwise. */
  evidence: EvidenceTable;
  /** For frontier evidence measured off the sealed board, a labelling note. */
  evidenceLabel?: string;
  receipt: Receipt;
  /** A secondary receipt, when the pillar rests on two. */
  receiptSecondary?: Receipt;
  operation: Operation;
  /** Caveats, ours, stated plainly. What would falsify the claim. */
  caveats: string[];
}

export const PILLARS: Pillar[] = [
  {
    n: 1,
    eyebrow: "Chapter one",
    title: "The taxonomy of evidence",
    teaser: "Orthogonal evidence classes, not one model. Sixteen of sixty-four features carry ninety-five percent of the gain.",
    claim:
      "PROTEA does not use one model to predict function. It gathers several independent kinds of evidence and lets a final judge weigh them. One kind is a compact numerical fingerprint of the protein, learned by a protein language model (a neural network trained on millions of sequences). Another is a set of per-candidate clues the judge reads directly: how similar a match is, how common a term is, how a candidate term relates to what we already know. A third is grafted in from an outside catalogue, the InterPro2GO mapping, which links known protein domains to functions; it adds 0.0179 to the sealed score and is what tips seven of the nine cells into first place. Why so many kinds? Because none of them wins everywhere. And the judge is frugal: of the sixty-four clues it is offered, sixteen carry ninety-five percent of the weight and twenty-eight carry none at all.",
    evidence: {
      caption: `LightGBM gain share, sealed booster (${METRIC}, ${FRAME})`,
      valueHeader: "Gain share",
      rows: [
        { label: "go_term_frequency", value: "0.255", note: "base-rate prior" },
        { label: "anc2vec_query_known_maxcos", value: "0.194" },
        { label: "lineage_is_ancestor_of_known", value: "0.127" },
        { label: "k_position", value: "0.117" },
        { label: "anc2vec_neighbor_cos", value: "0.093" },
        { label: "vote_count", value: "0.034" },
        { label: "anc2vec_neighbor_maxcos", value: "0.034" },
        { label: "identity_nw", value: "0.022" },
      ],
    },
    receipt: {
      artifact: "storage/feature_necessity/gain_report.json",
      script: "storage/feature_necessity/measure_gain.py",
    },
    operation: {
      kind: "script",
      script: "storage/feature_necessity/measure_gain.py",
      note: "Gain is read off the sealed booster offline; the dataset it was trained on is regenerated with the export_research_dataset operation.",
    },
    caveats: [
      "Gain measures how much the trees lean on a column, not causation on held-out proteins.",
      "The single largest driver, go_term_frequency at twenty-five percent, is a base-rate prior. We surface it rather than hide it.",
      "Of the twenty-eight zero-gain features, the emb_pca_* columns are populated but worthless, while the interpro_* columns read zero only because their producer tables are empty. That is a broken producer, not a proof of uselessness, and the graft above is the evidence InterPro can matter.",
    ],
  },
  {
    n: 2,
    eyebrow: "Chapter two",
    title: "The calibrated fusion",
    teaser: "Two learning layers and a signal store. The lever is calibration, not depth.",
    claim:
      "The pipeline learns in two places, with a shared signal store between them. First, a learned encoder turns each protein into a sparse code and uses it to retrieve similar, already-labelled proteins: the candidates. Then a set of scorers put every clue onto one common scale, and a small model, trained separately for each branch of the ontology, fuses them into a single probability. The surprising lesson is that the biggest lever is not a deeper or fancier model, it is calibration: simply standardising a representation (rescaling each of its dimensions to a common range) moves the score more than changing which internal layer of the language model you read it from. Run from end to end, this pipeline reproduces the sealed 0.4063; the exact remaining gap waits on the reproducibility receipt named below.",
    evidence: {
      caption: `Sealed headline (${METRIC}, ${FRAME})`,
      valueHeader: "Value",
      rows: [
        { label: "Sealed board", value: "0.4063", note: "first in 7 of 9 cells" },
      ],
    },
    receipt: {
      artifact: "storage/clean_227230/comparison.json",
      pending: true,
    },
    receiptSecondary: {
      artifact: "storage/layer_ablation/crown_result.json",
      script: "storage/layer_ablation/WRITEUP.md",
    },
    operation: {
      kind: "job",
      operation: "run_cafa_evaluation",
      payload: { prediction_set_id: "<sealed>", metric: "f_micro_w", frame: "v227-v230" },
      note: "The board number is regenerated by exporting the dataset (export_research_dataset) and scoring it (run_cafa_evaluation). Dispatch both from the instrument.",
    },
    caveats: [
      "The largest single lever is standardisation, a normalisation effect, not a deeper model. We name it so no one reads depth into the result.",
      "A single universal reranker reaches essentially the same number as the per-category combiner, so the per-category split earns little and we say so. The exact figures wait on the receipt below.",
      "The reproducibility receipt named by the plan, comparison.json, was not present in the storage snapshot this page was built from, and the values it would carry are not restated here. Until that file is re-materialised, this pillar rests only on the sealed board and the layer-ablation receipt.",
    ],
  },
  {
    n: 3,
    eyebrow: "Chapter three",
    title: "The map of regimes",
    teaser: "The nine cells read as a map of which evidence wins where, not as nine scores.",
    claim:
      "The nine cells are not nine separate scores, they are a map of which evidence wins where. The rows are how much we already know about a protein (nothing, a little, or a fair amount); the columns are the three branches of the Gene Ontology (molecular function, biological process, cellular component). Read across the map and a pattern appears: a learned classifier carries the proteins we know nothing about; what we already know about a protein, its associations and its lineage in the ontology, carries the partly-known ones; and plain sequence similarity proposes candidates but cannot tell the good from the bad on its own. First in seven of the nine cells is the shape of that map, not one number, and the two cells we do not hold are exactly the two the map predicts are hardest.",
    evidence: {
      caption: `Nine-cell board, first in 7 of 9 (${METRIC}, ${FRAME})`,
      valueHeader: METRIC,
      rows: [
        { label: "NK molecular function", value: "0.648" },
        { label: "NK biological process", value: "0.331" },
        { label: "NK cellular component", value: "0.481" },
        { label: "LK molecular function", value: "0.559" },
        { label: "LK biological process", value: "0.354", frontier: true, note: "not won" },
        { label: "LK cellular component", value: "0.467" },
        { label: "PK molecular function", value: "0.230" },
        { label: "PK biological process", value: "0.141", frontier: true, note: "not won" },
        { label: "PK cellular component", value: "0.273" },
      ],
    },
    receipt: {
      artifact: "storage/lofo_9cell/result.json",
      pending: true,
    },
    operation: {
      kind: "job",
      operation: "run_cafa_evaluation",
      payload: { prediction_set_id: "<sealed>", stratify: ["knowledge", "aspect"] },
      note: "Each cell is regenerated by scoring the sealed predictions with the knowledge x aspect stratification. Dispatch from the instrument.",
    },
    caveats: [
      "The leave-one-family-out ablation that reads the map, lofo_9cell/result.json, was not present in the storage snapshot this page was built from. The regime reading rests on the sealed board until that receipt is re-materialised.",
      "That homology proposes but does not discriminate is a statement about this reference and this frame. A different reference proteome could shift it.",
    ],
  },
  {
    n: 4,
    eyebrow: "Chapter four",
    title: "The frontier",
    teaser: "The biological-process wall is bounded by evidence, and a text-aligned representation is the first crack.",
    claim:
      "The two cells we do not win are Biological Process for the proteins we know little or nothing about: the biological-process wall. It is bounded by the evidence available, not by the design of the method. A protein's 3D structure, for example, is conserved for molecular function but not for biological process, so adding a structural check recovers the molecular-function cells and leaves the process cells exactly where they were. The first genuine crack in the wall comes from a different kind of representation, one trained on written descriptions of function: a text-aligned protein language model lifts biological process for novel proteins by about 0.062, with no data leakage, over the same setup without text. We state the wall before we state the crack.",
    evidence: {
      caption: "Novel-protein BP, text-aligned vs base representation (aspect-aware lab eval)",
      valueHeader: "nk-BP",
      rows: [
        { label: "ESM-1b, no text (base)", value: "0.145" },
        { label: "ProtST, text-aligned", value: "0.206", note: "+0.062 over base" },
        { label: "Structural gate, BP", value: "no lift", frontier: true, note: "structure conserves MF, not BP" },
      ],
    },
    evidenceLabel: `These are aspect-aware lab evaluations on the text-scorer substrate, not the sealed ${METRIC} board. The lift is a direction, not a new headline.`,
    receipt: {
      artifact: "storage/text_scorer/knn_confirm_text_result.json",
      script: "storage/text_scorer/stratify_identity_result.json",
    },
    receiptSecondary: {
      artifact: "storage/struct_gate/m2ab/eval_9cell.json",
      script: "storage/struct_gate/carriers.json",
    },
    operation: {
      kind: "script",
      script: "storage/text_scorer/knn_confirm_text.py",
      note: "The frontier probes run offline (text_scorer and struct_gate). They are not yet a dispatchable operation, so this claim lives at the edge of the argument until one exists.",
    },
    caveats: [
      "These frontier numbers are lab evaluations on a text-scorer substrate, not the sealed metric. Read them as a direction, not a headline.",
      "The biological-process lift is specific to ProtST. A second text model, ProTrek, did not reproduce it, so the claim is function-description-aligned representation, not text models in general.",
    ],
  },
];

export function pillarByNumber(n: number): Pillar | undefined {
  return PILLARS.find((p) => p.n === n);
}
