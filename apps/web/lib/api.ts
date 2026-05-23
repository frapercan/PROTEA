export type Job = {
  id: string;
  operation: string;
  operation_description?: string | null;
  operation_summary?: string | null;
  queue_name: string;
  status: string;
  parent_job_id?: string | null;
  created_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  progress_current?: number | null;
  progress_total?: number | null;
  error_code?: string | null;
  error_message?: string | null;
};

export type JobEvent = {
  id: number;
  ts: string;
  level: "info" | "warning" | "error";
  event: string;
  message: string | null;
  fields: Record<string, any>;
};

export function baseUrl(): string {
  const u = process.env.NEXT_PUBLIC_API_URL;
  if (!u) throw new Error("NEXT_PUBLIC_API_URL is not set");
  return u.replace(/\/+$/, "");
}

// Opt-in cache convention: pass `cacheable: true` to use Next.js fetch
// revalidation (60s) instead of the default `cache: "no-store"`. Reserved
// for stable reference endpoints (configs/snapshots/embeddings lists).
async function http<T>(path: string, init?: RequestInit & { cacheable?: boolean }): Promise<T> {
  const { cacheable, ...rest } = init ?? {};
  const fetchInit: RequestInit = cacheable
    ? { next: { revalidate: 60 }, ...rest }
    : { cache: "no-store", ...rest };
  let res: Response;
  try {
    res = await fetch(`${baseUrl()}${path}`, fetchInit);
  } catch (e: any) {
    throw new Error(e?.message ?? "Network error");
  }
  if (!res.ok) {
    const body = await res.text();
    const msg = body.trimStart().startsWith("<")
      ? `HTTP ${res.status} ${res.statusText}`
      : body;
    throw new Error(msg);
  }
  return await res.json();
}

export function listJobs(params?: { limit?: number; status?: string; operation?: string; parent_job_id?: string }) {
  const q = new URLSearchParams();
  q.set("limit", String(params?.limit ?? 50));
  if (params?.status) q.set("status", params.status);
  if (params?.operation) q.set("operation", params.operation);
  if (params?.parent_job_id) q.set("parent_job_id", params.parent_job_id);
  return http<Job[]>(`/jobs?${q.toString()}`);
}

export function getJob(id: string) {
  return http<any>(`/jobs/${id}`);
}

export function getJobEvents(id: string, limit = 200) {
  return http<JobEvent[]>(`/jobs/${id}/events?limit=${limit}`);
}

export function createJob(body: {
  operation: string;
  queue_name: string;
  payload?: Record<string, any>;
  meta?: Record<string, any>;
}) {
  return http<{ id: string; status: string }>(`/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteJob(id: string) {
  return http<{ deleted: string }>(`/jobs/${id}`, { method: "DELETE" });
}

export function cancelJob(id: string) {
  return http<{ id: string; status: string }>(`/jobs/${id}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
}

export type EmbeddingConfig = {
  id: string;
  model_name: string;
  model_backend: string;
  layer_indices: number[];
  layer_agg: string;
  pooling: string;
  normalize_residues: boolean;
  normalize: boolean;
  max_length: number;
  use_chunking: boolean;
  chunk_size: number;
  chunk_overlap: number;
  description?: string | null;
  created_at: string;
  embedding_count?: number;
};

export type PredictionSet = {
  id: string;
  embedding_config_id: string;
  embedding_config_name?: string;
  annotation_set_id: string;
  annotation_set_label?: string;
  ontology_snapshot_id: string;
  ontology_snapshot_version?: string;
  limit_per_entry: number;
  distance_threshold?: number | null;
  created_at: string;
  prediction_count?: number;
};

export type AnnotationSet = {
  id: string;
  source: string;
  source_version?: string | null;
  ontology_snapshot_id: string;
  job_id?: string | null;
  created_at: string;
  meta?: Record<string, any>;
  annotation_count?: number;
};

export type OntologySnapshot = {
  id: string;
  obo_url: string;
  obo_version: string;
  ia_url?: string | null;
  loaded_at: string;
  go_term_count?: number;
};

export type ProteinItem = {
  accession: string;
  entry_name?: string | null;
  gene_name?: string | null;
  organism?: string | null;
  taxonomy_id?: string | null;
  length?: number | null;
  reviewed?: boolean | null;
  is_canonical: boolean;
  isoform_index?: number | null;
};

export type ProteinDetail = ProteinItem & {
  canonical_accession: string;
  isoforms: string[];
  sequence_id?: number | null;
  embedding_count: number;
  go_annotation_count: number;
  metadata?: {
    function_cc?: string | null;
    ec_number?: string | null;
    catalytic_activity?: string | null;
    pathway?: string | null;
    keywords?: string | null;
    cofactor?: string | null;
    activity_regulation?: string | null;
    [key: string]: string | null | undefined;
  } | null;
};

export type ProteinStats = {
  total: number;
  canonical: number;
  isoforms: number;
  reviewed: number;
  unreviewed: number;
  with_metadata: number;
  with_embeddings: number;
  with_go_annotations: number;
};

export function getProteinStats() {
  return http<ProteinStats>(`/proteins/stats/`);
}

export function listProteins(params?: {
  search?: string;
  reviewed?: boolean;
  canonical_only?: boolean;
  limit?: number;
  offset?: number;
}) {
  const q = new URLSearchParams();
  if (params?.search) q.set("search", params.search);
  if (params?.reviewed !== undefined) q.set("reviewed", String(params.reviewed));
  if (params?.canonical_only !== undefined) q.set("canonical_only", String(params.canonical_only));
  if (params?.limit !== undefined) q.set("limit", String(params.limit));
  if (params?.offset !== undefined) q.set("offset", String(params.offset));
  return http<{ total: number; offset: number; limit: number; items: ProteinItem[] }>(
    `/proteins?${q.toString()}`
  );
}

export function getProtein(accession: string) {
  return http<ProteinDetail>(`/proteins/${encodeURIComponent(accession)}`);
}

export function listEmbeddingConfigs() {
  return http<EmbeddingConfig[]>(`/embeddings/configs/`, { cacheable: true });
}

export function createEmbeddingConfig(body: Omit<EmbeddingConfig, "id" | "created_at" | "embedding_count" | "use_chunking" | "chunk_size" | "chunk_overlap" | "normalize_residues"> & {
  normalize_residues?: boolean;
  use_chunking?: boolean;
  chunk_size?: number;
  chunk_overlap?: number;
}) {
  return http<EmbeddingConfig>(`/embeddings/configs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteEmbeddingConfig(id: string) {
  return http<{ deleted: string }>(`/embeddings/configs/${id}`, { method: "DELETE" });
}

export function launchPredictGoTerms(body: {
  embedding_config_id: string;
  annotation_set_id: string;
  ontology_snapshot_id: string;
  limit_per_entry?: number;
  distance_threshold?: number | null;
  batch_size?: number;
  query_accessions?: string[] | null;
  query_set_id?: string | null;
  // Search backend
  search_backend?: string;
  metric?: string;
  faiss_index_type?: string;
  faiss_nlist?: number;
  faiss_nprobe?: number;
  faiss_hnsw_m?: number;
  faiss_hnsw_ef_search?: number;
  // Feature engineering
  compute_alignments?: boolean;
  compute_taxonomy?: boolean;
  compute_reranker_features?: boolean;
  // Per-aspect KNN indices
  aspect_separated_knn?: boolean;
}) {
  return http<{ id: string; status: string }>(`/embeddings/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function listPredictionSets() {
  return http<PredictionSet[]>(`/embeddings/prediction-sets/`);
}

export function getPredictionSet(id: string) {
  return http<PredictionSet & { query_set_id?: string | null }>(`/embeddings/prediction-sets/${id}`);
}

export function getPredictionSetProteins(setId: string, params?: { search?: string; limit?: number; offset?: number }) {
  const q = new URLSearchParams();
  if (params?.search) q.set("search", params.search);
  if (params?.limit !== undefined) q.set("limit", String(params.limit));
  if (params?.offset !== undefined) q.set("offset", String(params.offset));
  return http<{
    total: number; offset: number; limit: number;
    items: { accession: string; go_count: number; min_distance: number | null; annotation_count: number; match_count: number; in_db: boolean }[];
  }>(`/embeddings/prediction-sets/${setId}/proteins?${q.toString()}`);
}

export type Prediction = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  distance: number;
  ref_protein_accession: string;
  qualifier: string | null;
  evidence_code: string | null;
  // Alignment — NW
  identity_nw: number | null;
  similarity_nw: number | null;
  alignment_score_nw: number | null;
  gaps_pct_nw: number | null;
  alignment_length_nw: number | null;
  // Alignment — SW
  identity_sw: number | null;
  similarity_sw: number | null;
  alignment_score_sw: number | null;
  gaps_pct_sw: number | null;
  alignment_length_sw: number | null;
  // Lengths
  length_query: number | null;
  length_ref: number | null;
  // Taxonomy
  query_taxonomy_id: number | null;
  ref_taxonomy_id: number | null;
  taxonomic_lca: number | null;
  taxonomic_distance: number | null;
  taxonomic_common_ancestors: number | null;
  taxonomic_relation: string | null;
  // Re-ranker features
  vote_count: number | null;
  k_position: number | null;
  go_term_frequency: number | null;
  ref_annotation_density: number | null;
  neighbor_distance_std: number | null;
};

export function getProteinPredictions(setId: string, accession: string) {
  return http<Prediction[]>(`/embeddings/prediction-sets/${setId}/proteins/${encodeURIComponent(accession)}`);
}

export function getGoTermDistribution(setId: string) {
  return http<{
    by_aspect: Record<string, { go_id: string; name: string | null; count: number }[]>;
    aspect_totals: Record<string, number>;
    top_terms: { go_id: string; name: string | null; aspect: string | null; count: number }[];
  }>(`/embeddings/prediction-sets/${setId}/go-terms`);
}

export type ProteinAnnotation = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  qualifier: string | null;
  evidence_code: string | null;
  assigned_by: string | null;
  db_reference: string | null;
  annotation_set_id: string;
  annotation_set_source: string;
  annotation_set_version: string | null;
};

export function getProteinAnnotations(accession: string, annotationSetId?: string) {
  const q = new URLSearchParams();
  if (annotationSetId) q.set("annotation_set_id", annotationSetId);
  return http<ProteinAnnotation[]>(`/proteins/${encodeURIComponent(accession)}/annotations?${q.toString()}`);
}

export function deletePredictionSet(id: string) {
  return http<{ deleted: string; predictions_deleted: number }>(`/embeddings/prediction-sets/${id}`, { method: "DELETE" });
}

export type GoSubgraph = {
  nodes: { id: number; go_id: string; name: string | null; aspect: string | null; is_query: boolean }[];
  edges: { source: number; target: number; relation_type: string }[];
};

export function getGoSubgraph(snapshotId: string, goIds: string[], depth = 3) {
  const q = new URLSearchParams({ go_ids: goIds.join(","), depth: String(depth) });
  return http<GoSubgraph>(`/annotations/snapshots/${snapshotId}/subgraph?${q.toString()}`);
}

export function listAnnotationSets() {
  return http<AnnotationSet[]>(`/annotations/sets/`);
}

export function deleteAnnotationSet(id: string) {
  return http<{ deleted: string; annotations_deleted: number }>(`/annotations/sets/${id}`, { method: "DELETE" });
}

export function listOntologySnapshots() {
  return http<OntologySnapshot[]>(`/annotations/snapshots/`, { cacheable: true });
}

export function setSnapshotIaUrl(snapshotId: string, iaUrl: string | null) {
  return http<{ id: string; obo_version: string; ia_url: string | null }>(
    `/annotations/snapshots/${snapshotId}/ia-url`,
    { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ia_url: iaUrl }) }
  );
}

export type QuerySet = {
  id: string;
  name: string;
  description?: string | null;
  entry_count: number;
  created_at: string;
  entries?: { accession: string; sequence_id: number }[];
};

export function listQuerySets() {
  return http<QuerySet[]>(`/query-sets/`);
}

export function getQuerySet(id: string) {
  return http<QuerySet>(`/query-sets/${id}`);
}

export function deleteQuerySet(id: string) {
  return http<{ deleted: string }>(`/query-sets/${id}`, { method: "DELETE" });
}

export type VacuumSequencesPreview = {
  total_sequences: number;
  orphan_sequences: number;
  referenced_sequences: number;
};

export type VacuumEmbeddingsPreview = {
  total_embeddings: number;
  unindexed_embeddings: number;
  indexed_embeddings: number;
};

export function previewVacuumSequences() {
  return http<VacuumSequencesPreview>(`/maintenance/vacuum-sequences/preview/`);
}

export function runVacuumSequences() {
  return http<{ deleted_sequences: number }>(`/maintenance/vacuum-sequences`, { method: "POST" });
}

export function previewVacuumEmbeddings() {
  return http<VacuumEmbeddingsPreview>(`/maintenance/vacuum-embeddings/preview/`);
}

export function runVacuumEmbeddings() {
  return http<{ deleted_embeddings: number }>(`/maintenance/vacuum-embeddings`, { method: "POST" });
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

export type ScoringConfig = {
  id: string;
  name: string;
  formula: string;
  /** Signal weights (embedding_similarity, identity_nw, …) — all in [0, 1]. */
  weights: Record<string, number>;
  /**
   * Optional per-GO-evidence-code quality overrides in [0, 1].
   * When null the engine falls back to the system defaults
   * (EXP/IDA → 1.0, ISS/IBA → 0.7, IEA → 0.3, ND → 0.1, …).
   * Partial dicts are valid: absent codes still resolve via the system table.
   */
  evidence_weights: Record<string, number> | null;
  description?: string | null;
  created_at: string;
};

export function listScoringConfigs() {
  return http<ScoringConfig[]>(`/scoring/configs/`, { cacheable: true });
}

export function createScoringConfig(body: {
  name: string;
  formula: string;
  weights: Record<string, number>;
  /**
   * Optional per-GO-evidence-code quality overrides.
   * Omit or pass null to use system defaults.
   * Keys must be valid GO evidence codes (EXP, IDA, IEA, …).
   * Values must be floats in [0, 1].
   */
  evidence_weights?: Record<string, number> | null;
  description?: string;
}) {
  return http<ScoringConfig>(`/scoring/configs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function deleteScoringConfig(id: string) {
  const res = await fetch(`${baseUrl()}/scoring/configs/${id}`, {
    cache: "no-store",
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await res.text());
}

export function createPresetScoringConfigs() {
  return http<{ created: string[] }>(`/scoring/configs/presets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
}

export function getScoredTsvUrl(
  setId: string,
  scoringConfigId: string,
  params?: { minScore?: number },
): string {
  const q = new URLSearchParams();
  q.set("scoring_config_id", scoringConfigId);
  if (params?.minScore !== undefined) q.set("min_score", String(params.minScore));
  return `${baseUrl()}/scoring/prediction-sets/${setId}/score.tsv?${q.toString()}`;
}

// ---------------------------------------------------------------------------
// Re-ranker
// ---------------------------------------------------------------------------

export type RerankerFeatureSelection = {
  families_enabled: string[] | null; // null = all available families
  families_available: string[];
  drop_features: string[];
  feature_count: number | null;
};

export type RerankerModel = {
  id: string;
  name: string;
  prediction_set_id: string | null;
  evaluation_set_id: string | null;
  category: string;
  aspect: string | null;
  metrics: Record<string, any>;
  feature_importance: Record<string, number>;
  // Provenance / reproducibility (surfaced from the reranker-model API).
  feature_schema_sha: string | null;
  feature_selection: RerankerFeatureSelection | null;
  producer_version: string | null;
  producer_git_sha: string | null;
  external_source: string | null;
  dataset_id: string | null;
  dataset_name: string | null;
  dataset_schema_sha: string | null;
  dataset_manifest_sha: string | null;
  spec_yaml: string | null;
  created_at: string;
};

export function listRerankers() {
  return http<RerankerModel[]>(`/scoring/rerankers/`);
}

export function getReranker(id: string) {
  return http<RerankerModel>(`/scoring/rerankers/${id}`);
}

export function trainReranker(body: {
  name: string;
  prediction_set_id: string;
  evaluation_set_id: string;
  category?: string;
  aspect?: string | null;
  neg_pos_ratio?: number | null;
  extra_pairs?: { prediction_set_id: string; evaluation_set_id: string }[];
}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5 * 60_000); // 5 min
  return http<RerankerModel>(`/scoring/rerankers/train`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: controller.signal,
  }).finally(() => clearTimeout(timer));
}

export async function deleteReranker(id: string) {
  const res = await fetch(`${baseUrl()}/scoring/rerankers/${id}`, {
    cache: "no-store",
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await res.text());
}

export function getRerankedTsvUrl(
  setId: string,
  rerankerId: string,
  params?: { minScore?: number },
): string {
  const q = new URLSearchParams();
  q.set("reranker_id", rerankerId);
  if (params?.minScore !== undefined) q.set("min_score", String(params.minScore));
  return `${baseUrl()}/scoring/prediction-sets/${setId}/rerank.tsv?${q.toString()}`;
}

export function getRerankerMetrics(
  setId: string,
  rerankerId: string,
  evaluationSetId: string,
  category: string = "nk",
) {
  const q = new URLSearchParams();
  q.set("reranker_id", rerankerId);
  q.set("evaluation_set_id", evaluationSetId);
  q.set("category", category);
  return http<Record<string, any>>(`/scoring/prediction-sets/${setId}/reranker-metrics?${q.toString()}`);
}

export function getTrainingDataTsvUrl(
  setId: string,
  evaluationSetId: string,
  category: string = "nk",
): string {
  const q = new URLSearchParams();
  q.set("evaluation_set_id", evaluationSetId);
  q.set("category", category);
  return `${baseUrl()}/scoring/prediction-sets/${setId}/training-data.tsv?${q.toString()}`;
}

// ---------------------------------------------------------------------------
// Re-ranker import (lab → PROTEA bridge)
// ---------------------------------------------------------------------------

export type RerankerImportResponse = {
  id: string;
  name: string;
  artifact_uri: string;
  storage_backend?: string;
};

export type RerankerImportOverrides = {
  name?: string | null;
  dataset_id?: string | null;
  external_source?: string | null;
  prediction_set_id?: string | null;
  evaluation_set_id?: string | null;
  force?: boolean;
};

/**
 * Upload a lab-trained booster (`model.txt` + `spec.yaml` + `run.json`)
 * to ``POST /reranker-models/import``. PROTEA stores the booster in the
 * configured artifact store (local FS or MinIO) and inserts a
 * ``RerankerModel`` row linked back to the source dataset.
 */
export async function importRerankerMultipart(
  files: { modelFile: File; specYaml: File; runJson: File },
  overrides: RerankerImportOverrides = {},
): Promise<RerankerImportResponse> {
  const form = new FormData();
  form.append("model_file", files.modelFile);
  form.append("spec_yaml", files.specYaml);
  form.append("run_json", files.runJson);
  if (overrides.name) form.append("name", overrides.name);
  if (overrides.dataset_id) form.append("dataset_id", overrides.dataset_id);
  if (overrides.external_source) form.append("external_source", overrides.external_source);
  if (overrides.prediction_set_id) form.append("prediction_set_id", overrides.prediction_set_id);
  if (overrides.evaluation_set_id) form.append("evaluation_set_id", overrides.evaluation_set_id);
  if (overrides.force) form.append("force", "true");
  const res = await fetch(`${baseUrl()}/reranker-models/import`, {
    method: "POST",
    body: form,
    cache: "no-store",
  });
  if (!res.ok) throw new Error(await res.text());
  return (await res.json()) as RerankerImportResponse;
}

export type RerankerImportByReferenceBody = {
  artifact_uri: string;
  spec_yaml: string;
  run: Record<string, unknown>;
  name?: string | null;
  dataset_id?: string | null;
  external_source?: string | null;
  prediction_set_id?: string | null;
  evaluation_set_id?: string | null;
  force?: boolean;
};

/**
 * Register a booster whose blob is already in the artifact store
 * (``s3://…`` or ``file://…``) via ``POST /reranker-models/import-by-reference``.
 * Useful when the lab pushes ``model.txt`` to MinIO directly and only
 * needs PROTEA to record the URI + provenance.
 */
export function importRerankerByReference(body: RerankerImportByReferenceBody) {
  return http<RerankerImportResponse>(`/reranker-models/import-by-reference`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// ---------------------------------------------------------------------------
// Annotate (one-click pipeline)
// ---------------------------------------------------------------------------

export type AnnotateResult = {
  query_set_id: string;
  embedding_config_id: string;
  annotation_set_id: string;
  ontology_snapshot_id: string;
  embedding_job_id: string;
  predict_payload: Record<string, any>;
  reranker_id: string | null;
  sequence_count: number;
};

export async function annotateProteins(
  input: {
    file?: File;
    fastaText?: string;
    name?: string;
    /** Enable the full reranker feature bundle (lineage/anc2vec/emb_pca/annotation_meta). Default: true. */
    computeRerankerFeatures?: boolean;
  },
): Promise<AnnotateResult> {
  const form = new FormData();
  if (input.file) form.append("file", input.file);
  if (input.fastaText) form.append("fasta_text", input.fastaText);
  form.append("name", input.name ?? "Quick annotation");
  form.append(
    "compute_reranker_features",
    String(input.computeRerankerFeatures ?? true),
  );
  const res = await fetch(`${baseUrl()}/annotate`, {
    cache: "no-store",
    method: "POST",
    body: form,
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

// ---------------------------------------------------------------------------
// Query sets (upload)
// ---------------------------------------------------------------------------

export async function createQuerySet(file: File, name: string, description?: string): Promise<QuerySet> {
  const form = new FormData();
  form.append("file", file);
  form.append("name", name);
  if (description) form.append("description", description);
  const res = await fetch(`${baseUrl()}/query-sets`, { cache: "no-store", method: "POST", body: form });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

// ---------------------------------------------------------------------------
// Showcase
// ---------------------------------------------------------------------------

export type ShowcasePipelineStage = {
  name: string;
  count: number;
  href: string;
};

export type ShowcaseBestCell = {
  category: string;
  aspect: string;
  fmax: number;
  precision: number | null;
  recall: number | null;
};

export type ShowcaseEmbedding = {
  id: string;
  model_name: string;
  model_backend: string;
  family: string;
  display_name: string;
  param_count: number | null;
};

export type ShowcaseBest = {
  evaluation_result_id: string;
  evaluation_set_id: string;
  stage: "baseline" | "alignment_weighted" | "reranker";
  avg_fmax: number;
  embedding: ShowcaseEmbedding;
  per_cell: ShowcaseBestCell[];
};

export type ShowcaseData = {
  protein_stats: { total: number; canonical: number };
  best: ShowcaseBest | null;
  counts: {
    proteins: number;
    sequences: number;
    embeddings: number;
    prediction_sets: number;
    predictions: number;
    reranker_models: number;
    evaluations: number;
  };
  pipeline_stages: ShowcasePipelineStage[];
};

export function getShowcase() {
  return http<ShowcaseData>("/showcase/");
}

// ─── Benchmark matrix ──────────────────────────────────────────────

export type BenchmarkEmbedding = {
  id: string;
  model_name: string;
  model_backend: string;
  description: string | null;
  pooling: string;
  layer_agg: string;
  family: string;
  display_name: string;
  param_count: number | null;
};

export type BenchmarkEmbeddingsResponse = {
  embeddings: BenchmarkEmbedding[];
  total: number;
};

export type BenchmarkStageKind = "scoring" | "reranker";

export type BenchmarkStage = {
  name: string;
  label: string;
  kind: BenchmarkStageKind;
  is_baseline: boolean;
};

export type BenchmarkRow = {
  embedding_config_id: string;
  evaluation_set_id: string;
  stage: string;
  k: number;
  category: string;
  aspect: string;
  fmax: number;
  precision: number | null;
  recall: number | null;
  coverage: number | null;
  n_proteins: number | null;
  evaluation_result_id: string;
  /**
   * Multiseed dispersion + bootstrap bands. All three fields are optional
   * and only emitted by the lab when the cell was produced from >=2
   * seeds (see project_v27_binary_multiseed memory: 0.7291 +/- 0.0028 is
   * the canonical publishable claim). Backends that haven't backfilled
   * them yet keep returning the scalar shape; the UI falls back to
   * rendering just `fmax` and skips the band/whisker silently.
   */
  fmax_std?: number | null;
  fmax_ci_low?: number | null;
  fmax_ci_high?: number | null;
};

export type BenchmarkBestCell = {
  category: string;
  aspect: string;
  fmax: number;
  precision: number | null;
  recall: number | null;
  coverage: number | null;
  embedding_config_id: string;
  k: number;
  stage: string;
  evaluation_result_id: string;
  evaluation_set_id: string;
};

export type BenchmarkEvalSet = {
  id: string;
  label: string;
  old_source: string | null;
  old_source_version: string | null;
  new_source: string | null;
  new_source_version: string | null;
  old_obo_version: string | null;
  new_obo_version: string | null;
  stats: {
    delta_proteins?: number;
    nk_proteins?: number;
    lk_proteins?: number;
    pk_proteins?: number;
    nk_annotations?: number;
    lk_annotations?: number;
    pk_annotations?: number;
    known_terms_count?: number;
  };
};

export type BenchmarkMatrixResponse = {
  rows: BenchmarkRow[];
  total: number;
  evaluation_sets: BenchmarkEvalSet[];
  embedding_config_ids: string[];
  stages: BenchmarkStage[];
  categories: string[];
  aspects: string[];
  ks: number[];
  best_per_cell: BenchmarkBestCell[];
  /** Same shape as best_per_cell but ignores stage and K filters. Stable
   *  across filter changes; the absolute champion per cell within the
   *  active evaluation_set scope. */
  best_per_cell_global: BenchmarkBestCell[];
  filters: {
    evaluation_set_id: string | null;
    stage: string | null;
    k: number | null;
  };
};

export function getBenchmarkEmbeddings() {
  return http<BenchmarkEmbeddingsResponse>("/benchmark/embeddings/", { cacheable: true });
}

export function getBenchmarkMatrix(params?: {
  evaluation_set_id?: string;
  stage?: string;
  k?: number;
}) {
  const qs = new URLSearchParams();
  if (params?.evaluation_set_id) qs.set("evaluation_set_id", params.evaluation_set_id);
  if (params?.stage) qs.set("stage", params.stage);
  if (params?.k !== undefined) qs.set("k", String(params.k));
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return http<BenchmarkMatrixResponse>(`/benchmark/matrix/${suffix}`);
}

export type StackRepo = {
  name: string;
  slug: string;
  role: string;
  role_label: string;
  status: "active" | "beta" | "skeleton" | "archived";
  summary: string;
  github_url: string;
  docs_url: string | null;
  package_url: string | null;
  local_docs_path: string | null;
};

export type StackResponse = {
  repos: StackRepo[];
  thesis_pdf_url: string | null;
};

export type StackPullRequest = {
  repo: string;
  number: number;
  title: string;
  url: string;
  state: string;
  draft: boolean;
  author: string | null;
  created_at: string;
  updated_at: string;
  labels: string[];
};

export type StackPullsResponse = {
  fetched_at: number;
  cached: boolean;
  repos_queried: number;
  pulls: StackPullRequest[];
  rate_limit_remaining: number | null;
  errors: Record<string, string>;
};

export function getStack() {
  return http<StackResponse>("/stack");
}

export function getStackPulls() {
  return http<StackPullsResponse>("/stack/pulls");
}

// ─── Datasets (frozen reranker dumps) ──────────────────────────────
//
// Mirrors ``protea/api/routers/datasets.py``. The Dataset row is the
// durable handle the offline lab uses to pull the exact dump by name
// or id; the UI surfaces the same registry so operators can see what
// has been exported, copy provenance shas, and dispatch new exports
// without having to compose a raw POST /v1/datasets curl call.

export type Dataset = {
  id: string;
  name: string;
  operation: string;
  job_id: string | null;
  storage_backend: string;
  key_prefix: string;
  train_uri: string | null;
  eval_uri: string | null;
  manifest_uri: string;
  schema_sha: string | null;
  manifest_sha: string | null;
  n_train_rows: number;
  n_eval_rows: number;
  k: number;
  annotation_source: string;
  embedding_config_id: string | null;
  ontology_snapshot_id: string | null;
  train_snapshot_pairs: string[];
  eval_snapshot_pair: string | null;
  producer_version: string | null;
  producer_git_sha: string | null;
  meta: Record<string, any> | null;
  created_at: string | null;
};

export function listDatasets(params?: {
  name_like?: string;
  embedding_config_id?: string;
  limit?: number;
}) {
  const q = new URLSearchParams();
  if (params?.name_like) q.set("name_like", params.name_like);
  if (params?.embedding_config_id) q.set("embedding_config_id", params.embedding_config_id);
  q.set("limit", String(params?.limit ?? 200));
  return http<Dataset[]>(`/datasets?${q.toString()}`);
}

export function getDataset(idOrName: string) {
  return http<Dataset>(`/datasets/${encodeURIComponent(idOrName)}`);
}

export type CreateDatasetPayload = {
  output_name: string;
  embedding_config_id: string;
  ontology_snapshot_id: string;
  train_versions: number[];
  test_versions: number[];
  annotation_source?: string;
  k?: number;
  search_backend?: string;
  compute_alignments?: boolean;
  compute_taxonomy?: boolean;
  expand_votes_to_ancestors?: boolean;
  use_embedding_pca?: boolean;
};

export function createDataset(body: CreateDatasetPayload) {
  return http<{ job_id: string; queue: string; status: string }>(`/datasets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
