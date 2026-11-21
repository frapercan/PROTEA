// Where a figure on the window page was read from, and how to check it.
//
// A cell's f_micro_w and cov_w are read from its category's
// `evaluation_best_f_micro_w.tsv`, the table cafaeval writes, and its size from
// the category's `per_protein_grid.parquet`. Since 2026-11-21 a result records
// the sha256 of every file it wrote, so each cell can name its file and that
// digest, and the file can be fetched by itself and checked: the sha256 of the
// download equals the digest shown here, or the stored bytes are not the ones
// the figure was read from. A result made before then records the file but not
// the digest, and says so rather than showing nothing.

/** What a result records about the files it wrote. */
export type ArtifactsRecord = {
  keys?: string[];
  /** key -> sha256 of the bytes uploaded under it; absent before 2026-11-21. */
  sha256?: Record<string, string>;
};

/** The file a cell's f_micro_w and cov_w are read from, under its category. */
export const HEADLINE_FILE = "evaluation_best_f_micro_w.tsv";
/** The file a cell's size is read from, under its category. */
export const GRID_FILE = "per_protein_grid.parquet";

export type FileSource = {
  /** The path under the result, as the evaluator wrote it. */
  relpath: string;
  /** The digest the result recorded, or null when the result predates digests. */
  sha256: string | null;
  /** Where the file is served by itself. */
  href: string;
};

/** One file of one result, or null when the result recorded no such file. */
export function fileSource(
  apiBase: string,
  evaluationSetId: string,
  resultId: string,
  record: ArtifactsRecord | undefined,
  relpath: string,
): FileSource | null {
  const key = `eval_artifacts/${resultId}/${relpath}`;
  if (!record?.keys?.includes(key)) return null;
  return {
    relpath,
    sha256: record.sha256?.[key] ?? null,
    href: `${apiBase}/annotations/evaluation-sets/${evaluationSetId}/results/${resultId}/artifacts/${relpath}`,
  };
}

/** The file and its digest, as a tooltip names them. */
export function describeSource(source: FileSource): string {
  return source.sha256
    ? `${source.relpath}, sha256 ${source.sha256}`
    : `${source.relpath}, no digest recorded (result made before 2026-11-21)`;
}
